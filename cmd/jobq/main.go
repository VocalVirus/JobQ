// Command jobq is the entry point for the JobQ service.
//
// Phase 1: an in-memory demo. We create a pool of workers, submit some fake
// jobs, and watch the workers process them concurrently. No Redis, Postgres,
// or Docker yet — that comes in later phases. The goal here is to SEE the
// worker pool working.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/VocalVirus/jobq/internal/job"
	"github.com/VocalVirus/jobq/internal/worker"
)

func main() {
	// The handler is our fake "work." It pretends to do something that takes
	// time (200ms) so we can watch multiple workers run in parallel. Later
	// this becomes real work (send email, resize image, etc.).
	handler := func(j job.Job) error {
		time.Sleep(200 * time.Millisecond)
		return nil // nil means "succeeded"
	}

	// 3 workers, a queue that can buffer up to 100 waiting jobs.
	pool := worker.NewPool(3, 100, handler)
	pool.Start()

	// Submit 10 jobs. Notice: we submit all 10 almost instantly, but only 3
	// can be processed at a time (3 workers). The rest wait in the queue.
	for i := 1; i <= 10; i++ {
		pool.Submit(job.Job{
			ID:      i,
			Payload: fmt.Sprintf("do-work-%d", i),
		})
	}

	// Gracefully shut down: this waits for all 10 jobs to finish before exiting.
	pool.Shutdown()

	log.Println("JobQ: all jobs processed, exiting")
}
