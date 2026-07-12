// Command jobq is the entry point for the JobQ service.
//
// Phase 2: an in-memory demo with retries. We create a pool of workers, submit
// some jobs whose "work" randomly fails, and watch the pool retry them with
// exponential backoff — sending jobs that fail too many times to a dead-letter
// list. Still no Redis/Postgres/Docker; the goal is to SEE reliability working.
package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/VocalVirus/jobq/internal/job"
	"github.com/VocalVirus/jobq/internal/worker"
)

func main() {
	// A flaky handler: it "does work" (a short sleep) and then fails ~40% of the
	// time at random. This simulates the real world — network blips, busy
	// servers — and lets us watch the retry logic kick in.
	handler := func(j job.Job) error {
		time.Sleep(150 * time.Millisecond)
		if rand.Float64() < 0.4 {
			return errors.New("simulated transient failure")
		}
		return nil
	}

	pool := worker.NewPool(worker.Config{
		NumWorkers:  3,
		QueueSize:   100,
		MaxAttempts: 4,                      // try up to 4 times before dead-lettering
		BaseBackoff: 100 * time.Millisecond, // first retry waits ~100ms, then ~200, ~400...
		MaxBackoff:  2 * time.Second,
		Handler:     handler,
	})
	pool.Start()

	// Submit 10 jobs. With ~40% failure and up to 4 attempts, most will
	// eventually succeed after a retry or two; a few may exhaust their attempts
	// and land in the dead-letter list.
	for i := 1; i <= 10; i++ {
		pool.Submit(job.Job{
			ID:      i,
			Payload: fmt.Sprintf("do-work-%d", i),
		})
	}

	// Gracefully shut down: waits for all jobs (and their retries) to finish.
	pool.Shutdown()

	// Report what permanently failed.
	dead := pool.DeadLetter()
	log.Printf("JobQ: done. %d job(s) dead-lettered.", len(dead))
	for _, j := range dead {
		log.Printf("  dead-letter: job %d (%s) after %d attempts", j.ID, j.Payload, j.Attempts)
	}
}
