// Package worker contains the heart of JobQ: a pool of concurrent workers
// that pull jobs off a shared queue and run them.
//
// This is the single most important concept in the whole project. If you
// understand this file, you understand ~60% of what JobQ is. Everything in
// later phases (Redis, retries, backpressure) is a variation on the ideas here.
package worker

import (
	"log"
	"sync"

	"github.com/VocalVirus/jobq/internal/job"
)

// Pool manages a group of worker goroutines that share one job queue.
type Pool struct {
	numWorkers int          // how many workers to run concurrently ("parallelism")
	jobs       chan job.Job // the queue: a channel every worker reads from
	handler    job.Handler  // the function each worker calls to process a job
	wg         sync.WaitGroup
}

// NewPool builds a Pool.
//
//   - numWorkers: how many jobs can be processed at the same time.
//   - queueSize:  how many jobs can wait in the buffer before Submit blocks.
//     This buffer is what will give us "backpressure" in a later phase.
//   - handler:    what to actually do with each job.
func NewPool(numWorkers, queueSize int, handler job.Handler) *Pool {
	return &Pool{
		numWorkers: numWorkers,
		// A "buffered channel": it can hold up to queueSize jobs waiting to be
		// picked up. Think of it as the conveyor belt between producers and workers.
		jobs:    make(chan job.Job, queueSize),
		handler: handler,
	}
}

// Start launches the worker goroutines. They immediately begin waiting for
// jobs to arrive on the channel.
func (p *Pool) Start() {
	for i := 1; i <= p.numWorkers; i++ {
		// wg.Add(1) records "one more goroutine is running." We use this later
		// in Shutdown to wait until every worker has finished.
		p.wg.Add(1)
		// The `go` keyword starts a goroutine: p.worker(i) runs concurrently,
		// and this loop keeps going without waiting for it. This is how we get
		// numWorkers workers all running at once.
		go p.worker(i)
	}
	log.Printf("pool started with %d workers", p.numWorkers)
}

// worker is the loop each goroutine runs.
func (p *Pool) worker(id int) {
	// defer wg.Done() runs when this function returns, telling the WaitGroup
	// "this worker has finished." Pairs with the wg.Add(1) in Start.
	defer p.wg.Done()

	// `for j := range p.jobs` reads jobs off the channel one at a time. It
	// blocks (waits) when the channel is empty, and — crucially — it exits
	// automatically once the channel is closed AND drained. That's how workers
	// know to stop.
	for j := range p.jobs {
		log.Printf("worker %d picked up job %d", id, j.ID)

		// Run the actual work. If it fails, just log it for now. Retries and a
		// dead-letter queue come in a later phase.
		if err := p.handler(j); err != nil {
			log.Printf("worker %d: job %d FAILED: %v", id, j.ID, err)
			continue
		}

		log.Printf("worker %d: job %d done", id, j.ID)
	}
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
//   1. close(p.jobs) signals "no more jobs are coming."
//   2. Each worker finishes draining the channel, then its range loop exits.
//   3. wg.Wait() blocks here until every worker has called wg.Done().
func (p *Pool) Shutdown() {
	log.Println("shutting down: no longer accepting new jobs, draining queue...")
	close(p.jobs)
	p.wg.Wait()
	log.Println("all workers finished")
}
