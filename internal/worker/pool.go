// Package worker contains the heart of JobQ: a pool of concurrent workers
// that pull jobs off a shared queue and run them reliably.
//
// This is the single most important concept in the whole project. If you
// understand this file, you understand most of what JobQ is. Later phases
// (Redis, backpressure, metrics) are variations on the ideas here.
package worker

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/VocalVirus/jobq/internal/job"
	"github.com/VocalVirus/jobq/internal/queue"
)

// Metrics receives observations from the pool as jobs run. It's the seam for
// instrumentation: the pool reports numbers through this interface without
// importing Prometheus (or anything else) — exactly like OnStatus reports state
// changes without importing the store. A metrics.Recorder satisfies it, but so
// could a test spy or a no-op. Optional: a nil Metrics means "don't measure".
//
// WorkerBusy/WorkerIdle bracket each handler call so a gauge can track how many
// are running; JobFinished fires once per job at its terminal state; JobRetried
// fires on each failed-but-retrying attempt. handlerDur is how long that single
// handler invocation took (the retry backoff wait is deliberately excluded).
type Metrics interface {
	WorkerBusy()
	WorkerIdle()
	JobFinished(result string, handlerDur time.Duration) // result: "succeeded" | "dead"
	JobRetried(handlerDur time.Duration)
}

// Config holds the tunable settings for a Pool. Using a struct (instead of a
// long list of function arguments) keeps things readable as options grow.
type Config struct {
	NumWorkers  int           // how many jobs can run at the same time ("parallelism")
	MaxAttempts int           // total tries per job before it goes to the dead-letter list
	BaseBackoff time.Duration // wait before the first retry (grows exponentially after)
	MaxBackoff  time.Duration // cap on the retry wait
	Handler     job.Handler   // what to actually do with each job

	// Queue is the durable source of jobs. Workers pull from it, and Ack a job
	// only once it reaches a terminal state (succeeded or dead-lettered). Until
	// this phase the queue was an in-memory channel; now it's Redis-backed, so a
	// job that a worker is holding when the process dies is NOT lost — it stays
	// unacked and Redis redelivers it. That's the whole point of "at-least-once".
	Queue queue.Queue

	// OnStatus, if set, is called whenever a job changes state (queued -> running
	// -> succeeded/failed/dead). It's how the pool reports progress to the outside
	// world (e.g. a status store) without the pool needing to know what that store
	// is. Optional: leave it nil and the pool simply won't report.
	OnStatus func(id int, status job.Status, attempts int)

	// Metrics, if set, receives observations (durations, outcomes, active count)
	// as jobs run. Optional (nil = no measurement), same decoupling as OnStatus:
	// the pool never imports the metrics package.
	Metrics Metrics
}

// Pool manages a group of worker goroutines that share one durable job queue.
type Pool struct {
	cfg Config
	wg  sync.WaitGroup

	// deadLetter holds jobs that failed too many times. In a later phase this
	// becomes a Postgres table + Redis stream; for now it's an in-memory slice.
	// The mutex guards it because multiple worker goroutines may append at once.
	mu         sync.Mutex
	deadLetter []job.Job
}

// NewPool builds a Pool from a Config.
func NewPool(cfg Config) *Pool {
	return &Pool{cfg: cfg}
}

// Start launches the worker goroutines. They immediately begin waiting for
// jobs to arrive on the channel.
//
// The ctx lets us cancel long waits (like a retry backoff) promptly when the
// program is shutting down, so we don't sit sleeping while the user waits.
func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.cfg.NumWorkers; i++ {
		// wg.Add(1) records "one more goroutine is running." Shutdown uses this
		// to wait until every worker has finished.
		p.wg.Add(1)
		// The `go` keyword starts a goroutine: p.worker(i) runs concurrently and
		// the loop keeps going. This is how we get NumWorkers workers at once.
		go p.worker(ctx, i)
	}
	log.Printf("pool started with %d workers", p.cfg.NumWorkers)
}

// worker is the loop each goroutine runs: pull a job off the durable queue,
// process it, acknowledge it, repeat — until the context is cancelled.
func (p *Pool) worker(ctx context.Context, id int) {
	// defer wg.Done() runs when this function returns, telling the WaitGroup
	// "this worker has finished." Pairs with the wg.Add(1) in Start.
	defer p.wg.Done()

	for {
		// Once we're shutting down, stop pulling. Any jobs still in Redis stay
		// there durably and get picked up on the next start — nothing is dropped.
		if ctx.Err() != nil {
			return
		}

		// Dequeue blocks up to its internal timeout. ErrNoJob just means "nothing
		// waiting right now" — loop and ask again. A real error during shutdown is
		// the context being cancelled, which we catch and exit on.
		msg, err := p.cfg.Queue.Dequeue(ctx)
		if err != nil {
			if errors.Is(err, queue.ErrNoJob) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			log.Printf("worker %d: dequeue error: %v", id, err)
			continue
		}

		// Ack only if the job reached a terminal state. If process was abandoned
		// mid-retry by a shutdown, we deliberately leave it unacked so Redis
		// redelivers it later — at-least-once, no silent loss.
		if p.process(ctx, id, msg.Job) {
			p.ack(id, msg)
		}
	}
}

// ack confirms a finished job so Redis stops tracking it as pending. It uses a
// short detached context (not the worker's) so an ack still lands even when the
// worker loop is shutting down — otherwise a job we just completed could be
// needlessly redelivered and run twice.
func (p *Pool) ack(id int, msg queue.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.cfg.Queue.Ack(ctx, msg); err != nil {
		log.Printf("worker %d: ack failed: %v", id, err)
	}
}

// process runs a single job, retrying with exponential backoff on failure.
// It returns true when the job reached a terminal state (succeeded or
// dead-lettered) and should be acked, or false when it was abandoned by a
// shutdown mid-retry and should be left for redelivery.
//
// NOTE (honest limitation): while a job is waiting to retry, this function
// sleeps — which ties up the worker and blocks it from doing other jobs during
// the backoff. A later phase can re-queue the job with a delay instead, so the
// worker stays free. Worth remembering as a real trade-off.
func (p *Pool) process(ctx context.Context, workerID int, j job.Job) bool {
	for {
		j.Attempts++
		p.setStatus(j.ID, job.StatusRunning, j.Attempts)

		// Time only the handler call (not the backoff wait) and bracket it with
		// the busy/idle hooks so a gauge can count in-flight handlers accurately.
		err, dur := p.runHandler(j)

		if err == nil {
			log.Printf("worker %d: job %d SUCCEEDED on attempt %d", workerID, j.ID, j.Attempts)
			p.setStatus(j.ID, job.StatusSucceeded, j.Attempts)
			p.jobFinished(job.StatusSucceeded, dur)
			return true
		}

		// It failed. Have we exhausted our attempts?
		if j.Attempts >= p.cfg.MaxAttempts {
			log.Printf("worker %d: job %d FAILED permanently after %d attempts (%v) -> dead-letter",
				workerID, j.ID, j.Attempts, err)
			p.setStatus(j.ID, job.StatusDead, j.Attempts)
			p.toDeadLetter(j)
			p.jobFinished(job.StatusDead, dur)
			return true
		}

		// A transient failure: mark it failed (a retry is pending).
		p.setStatus(j.ID, job.StatusFailed, j.Attempts)
		p.jobRetried(dur)

		// Otherwise wait (exponential backoff) and try again — but make the wait
		// interruptible. `select` blocks until one of its cases can proceed:
		//   - time.After(wait) fires when the backoff elapses -> retry.
		//   - ctx.Done() fires if we're shutting down -> abandon the retry now
		//     instead of sleeping. (When Redis arrives, an abandoned job stays in
		//     the queue and another instance picks it up; in memory it's just lost,
		//     which is an honest limitation of this phase.)
		wait := backoff(j.Attempts, p.cfg.BaseBackoff, p.cfg.MaxBackoff)
		log.Printf("worker %d: job %d failed attempt %d (%v) -> retry in %s",
			workerID, j.ID, j.Attempts, err, wait.Round(time.Millisecond))

		select {
		case <-time.After(wait):
			// backoff elapsed; loop around and retry
		case <-ctx.Done():
			log.Printf("worker %d: job %d retry abandoned due to shutdown (stays queued in Redis)", workerID, j.ID)
			return false
		}
	}
}

// setStatus reports a job's state change to the OnStatus hook, if one is set.
// Kept nil-safe so callers that don't care about status can ignore it entirely.
func (p *Pool) setStatus(id int, s job.Status, attempts int) {
	if p.cfg.OnStatus != nil {
		p.cfg.OnStatus(id, s, attempts)
	}
}

// runHandler executes one job's handler, timing just that call and reporting the
// worker as busy for its duration. Returns the handler's error and how long it
// ran. The busy/idle hooks are balanced via defer so the active gauge stays
// correct even if the handler panics.
func (p *Pool) runHandler(j job.Job) (error, time.Duration) {
	if p.cfg.Metrics != nil {
		p.cfg.Metrics.WorkerBusy()
		defer p.cfg.Metrics.WorkerIdle()
	}
	start := time.Now()
	err := p.cfg.Handler(j)
	return err, time.Since(start)
}

// jobFinished / jobRetried forward terminal and retry observations to the
// Metrics hook, if one is set. Nil-safe, mirroring setStatus, so an unmetered
// pool (e.g. in tests) just skips them.
func (p *Pool) jobFinished(result job.Status, dur time.Duration) {
	if p.cfg.Metrics != nil {
		p.cfg.Metrics.JobFinished(string(result), dur)
	}
}

func (p *Pool) jobRetried(dur time.Duration) {
	if p.cfg.Metrics != nil {
		p.cfg.Metrics.JobRetried(dur)
	}
}

// toDeadLetter records a job that failed too many times. Guarded by a mutex
// because several workers might dead-letter jobs concurrently.
func (p *Pool) toDeadLetter(j job.Job) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deadLetter = append(p.deadLetter, j)
}

// DeadLetter returns a copy of the dead-lettered jobs. Safe to call after
// Shutdown to inspect what permanently failed.
func (p *Pool) DeadLetter() []job.Job {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]job.Job, len(p.deadLetter))
	copy(out, p.deadLetter)
	return out
}

// Shutdown waits for all workers to stop and finish their in-flight jobs.
//
// Unlike the old in-memory channel, there's nothing to "drain" here: jobs the
// workers haven't pulled yet live durably in Redis and survive the restart.
// Workers notice the cancelled context (passed to Start), finish the job they're
// holding, and exit — so this just blocks until every worker has done so.
//
//	wg.Wait() blocks until every worker has called wg.Done().
func (p *Pool) Shutdown() {
	log.Println("shutting down: workers finishing in-flight jobs, rest stay queued in Redis...")
	p.wg.Wait()
	log.Println("all workers finished")
}
