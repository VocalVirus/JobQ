// Package worker contains the heart of JobQ: a pool of concurrent workers
// that pull jobs off a shared queue and run them reliably.
//
// This is the single most important concept in the whole project. If you
// understand this file, you understand most of what JobQ is. Later phases
// (Redis, backpressure, metrics) are variations on the ideas here.
package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/VocalVirus/jobq/internal/job"
)

// Config holds the tunable settings for a Pool. Using a struct (instead of a
// long list of function arguments) keeps things readable as options grow.
type Config struct {
	NumWorkers  int           // how many jobs can run at the same time ("parallelism")
	QueueSize   int           // how many jobs can wait in the buffer before Submit blocks
	MaxAttempts int           // total tries per job before it goes to the dead-letter list
	BaseBackoff time.Duration // wait before the first retry (grows exponentially after)
	MaxBackoff  time.Duration // cap on the retry wait
	Handler     job.Handler   // what to actually do with each job
}

// Pool manages a group of worker goroutines that share one job queue.
type Pool struct {
	cfg  Config
	jobs chan job.Job // the queue: a channel every worker reads from
	wg   sync.WaitGroup

	// deadLetter holds jobs that failed too many times. In a later phase this
	// becomes a Postgres table + Redis stream; for now it's an in-memory slice.
	// The mutex guards it because multiple worker goroutines may append at once.
	mu         sync.Mutex
	deadLetter []job.Job
}

// NewPool builds a Pool from a Config.
func NewPool(cfg Config) *Pool {
	return &Pool{
		cfg: cfg,
		// A "buffered channel": it can hold up to QueueSize jobs waiting to be
		// picked up. Think of it as the conveyor belt between producers and workers.
		jobs: make(chan job.Job, cfg.QueueSize),
	}
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

// worker is the loop each goroutine runs: pull a job, process it, repeat.
func (p *Pool) worker(ctx context.Context, id int) {
	// defer wg.Done() runs when this function returns, telling the WaitGroup
	// "this worker has finished." Pairs with the wg.Add(1) in Start.
	defer p.wg.Done()

	// `for j := range p.jobs` reads jobs off the channel one at a time. It
	// blocks when the channel is empty, and exits automatically once the
	// channel is closed AND drained. That's how workers know to stop.
	for j := range p.jobs {
		p.process(ctx, id, j)
	}
}

// process runs a single job, retrying with exponential backoff on failure.
//
// NOTE (honest limitation): while a job is waiting to retry, this function
// sleeps — which ties up the worker and blocks it from doing other jobs during
// the backoff. That's fine for learning the retry concept. When we add Redis
// (a later phase) we'll re-queue the job with a delay instead, so the worker
// stays free. Worth remembering as a real trade-off.
func (p *Pool) process(ctx context.Context, workerID int, j job.Job) {
	for {
		j.Attempts++

		err := p.cfg.Handler(j)
		if err == nil {
			log.Printf("worker %d: job %d SUCCEEDED on attempt %d", workerID, j.ID, j.Attempts)
			return
		}

		// It failed. Have we exhausted our attempts?
		if j.Attempts >= p.cfg.MaxAttempts {
			log.Printf("worker %d: job %d FAILED permanently after %d attempts (%v) -> dead-letter",
				workerID, j.ID, j.Attempts, err)
			p.toDeadLetter(j)
			return
		}

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
			log.Printf("worker %d: job %d retry abandoned due to shutdown", workerID, j.ID)
			return
		}
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

// Submit puts a job onto the queue to be processed.
//
// If the queue buffer is full, this call blocks until a worker frees up space.
// That blocking IS backpressure — the system refuses to accept infinite work.
func (p *Pool) Submit(j job.Job) {
	p.jobs <- j
}

// Shutdown stops accepting new jobs and waits for all in-flight jobs to finish.
//
// This is a "graceful shutdown": we don't drop jobs that are already queued.
//  1. close(p.jobs) signals "no more jobs are coming."
//  2. Each worker finishes draining the channel, then its range loop exits.
//  3. wg.Wait() blocks here until every worker has called wg.Done().
func (p *Pool) Shutdown() {
	log.Println("shutting down: no longer accepting new jobs, draining queue...")
	close(p.jobs)
	p.wg.Wait()
	log.Println("all workers finished")
}
