// Command jobq is the entry point for the JobQ service.
//
// Phase 2.5: a long-running server. Instead of processing 10 hardcoded jobs and
// exiting, JobQ now runs continuously — a producer streams jobs in, the worker
// pool processes them with retries, and the whole thing shuts down *gracefully*
// when you press Ctrl+C (draining in-flight work first). Still no Redis/Postgres;
// the goal is to see real server lifecycle + graceful shutdown (resume bullet #3).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/VocalVirus/jobq/internal/job"
	"github.com/VocalVirus/jobq/internal/worker"
)

func main() {
	// signal.NotifyContext gives us a context that is automatically cancelled
	// when the user presses Ctrl+C (os.Interrupt) or the OS sends SIGTERM (how
	// Docker/Kubernetes ask a program to stop). This ctx is our "please shut
	// down now" signal, threaded through the whole program.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A flaky handler: pretends to do ~150ms of work, then fails ~30% of the
	// time so we can watch retries happen live.
	handler := func(j job.Job) error {
		time.Sleep(150 * time.Millisecond)
		if rand.Float64() < 0.3 {
			return errors.New("simulated transient failure")
		}
		return nil
	}

	pool := worker.NewPool(worker.Config{
		NumWorkers:  3,
		QueueSize:   100,
		MaxAttempts: 4,
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
		Handler:     handler,
	})
	pool.Start(ctx)

	// Producer: a goroutine that simulates a live stream of incoming jobs,
	// submitting a new one every 120ms until we're told to shut down. In a later
	// phase this is replaced by real jobs arriving over an HTTP API + Redis.
	var producerWG sync.WaitGroup
	producerWG.Add(1)
	go func() {
		defer producerWG.Done()
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()

		id := 0
		for {
			select {
			case <-ctx.Done():
				// Shutdown requested: stop producing new jobs.
				return
			case <-ticker.C:
				id++
				pool.Submit(job.Job{ID: id, Payload: fmt.Sprintf("do-work-%d", id)})
			}
		}
	}()

	log.Println("JobQ running — press Ctrl+C to shut down gracefully")

	// Block here until a shutdown signal cancels ctx.
	<-ctx.Done()
	log.Println("shutdown signal received")

	// Order matters for a clean shutdown:
	//  1. Wait for the producer to stop, so nothing tries to Submit after we
	//     close the jobs channel (sending on a closed channel would panic).
	//  2. Shut down the pool, which closes the channel and drains in-flight jobs.
	producerWG.Wait()
	pool.Shutdown()

	dead := pool.DeadLetter()
	log.Printf("JobQ: stopped. %d job(s) dead-lettered.", len(dead))
	for _, j := range dead {
		log.Printf("  dead-letter: job %d (%s) after %d attempts", j.ID, j.Payload, j.Attempts)
	}
}
