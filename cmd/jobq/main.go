// Command jobq is the entry point for the JobQ service.
//
// Phase 5: jobs flow through a durable Redis queue AND their state is persisted
// in Postgres, so both the queue and the status survive a restart. The HTTP API
// (POST /jobs) records the job in Postgres and enqueues it; the worker pool
// dequeues, processes with retries, and acks each once done. Because a job stays
// unacked until it finishes, anything a worker was holding when the process dies
// is redelivered on restart — no silent loss. Jobs can also be scheduled for the
// future (POST /jobs with delay_seconds): they wait in a Redis sorted set and a
// promoter loop moves them onto the stream once due. Start Redis and Postgres
// first with `docker compose up -d`. Shuts down gracefully on Ctrl+C:
// stops accepting HTTP requests, then lets in-flight jobs finish (the rest stay
// safely queued in Redis).
package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VocalVirus/jobq/internal/api"
	"github.com/VocalVirus/jobq/internal/job"
	"github.com/VocalVirus/jobq/internal/queue"
	"github.com/VocalVirus/jobq/internal/store"
	"github.com/VocalVirus/jobq/internal/worker"
)

func main() {
	// ctx is cancelled when the user presses Ctrl+C or the OS sends SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Flaky handler: ~150ms of "work", failing ~30% of the time so retries show.
	handler := func(j job.Job) error {
		time.Sleep(150 * time.Millisecond)
		if rand.Float64() < 0.3 {
			return errors.New("simulated transient failure")
		}
		return nil
	}

	// The store tracks each job's status durably in Postgres, so a status lookup
	// still works after a restart. DATABASE_URL overrides the default for Docker.
	dsn := "postgres://jobq:jobq@localhost:5432/jobq?sslmode=disable"
	if u := os.Getenv("DATABASE_URL"); u != "" {
		dsn = u
	}
	st, err := store.NewPostgres(ctx, dsn)
	if err != nil {
		log.Fatalf("connect to Postgres: %v (is `docker compose up -d` running?)", err)
	}
	defer st.Close()

	// Connect to the durable queue. REDIS_ADDR overrides the default for Docker.
	redisAddr := "localhost:6379"
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		redisAddr = a
	}
	q, err := queue.NewRedisQueue(redisAddr, "jobq:jobs", "workers", "jobq-1")
	if err != nil {
		log.Fatalf("connect to Redis at %s: %v (is `docker compose up -d` running?)", redisAddr, err)
	}
	defer q.Close()

	pool := worker.NewPool(worker.Config{
		NumWorkers:  3,
		MaxAttempts: 4,
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
		Handler:     handler,
		Queue:       q,            // workers pull from (and ack to) Redis
		OnStatus:    st.SetStatus, // wire pool status updates into the store
	})
	pool.Start(ctx)

	// Scheduler loop: once a second, promote any now-due delayed jobs from the
	// sorted set into the live stream, where the worker pool picks them up. This
	// is what turns "run in 30s" (EnqueueAt) into an actual delayed execution.
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Detached context so an in-progress promote isn't cancelled the
				// instant we start shutting down; the loop still exits on ctx.Done.
				if n, err := q.PromoteDue(context.Background(), 100); err != nil {
					log.Printf("scheduler: %v", err)
				} else if n > 0 {
					log.Printf("scheduler: promoted %d due job(s)", n)
				}
			}
		}
	}()

	// HTTP server. Port is :8080 by default, overridable via the PORT env var.
	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}
	// The API enqueues jobs durably. A zero delay goes straight onto the live
	// stream; a positive delay is scheduled via the sorted set (EnqueueAt) and
	// promoted later by the loop above. We use the app context so an enqueue is
	// cancelled if we're shutting down.
	submit := func(j job.Job, delay time.Duration) error {
		if delay > 0 {
			return q.EnqueueAt(ctx, j, time.Now().Add(delay))
		}
		return q.Enqueue(ctx, j)
	}
	srv := &http.Server{
		Addr:    addr,
		Handler: api.NewServer(st, submit),
	}

	// Run the server in a goroutine so main can wait for the shutdown signal.
	go func() {
		log.Printf("JobQ listening on %s — try: curl -X POST localhost%s/jobs -d '{\"payload\":\"hello\"}'", addr, addr)
		// ListenAndServe blocks until the server is closed; ErrServerClosed is the
		// expected, clean way it ends after Shutdown, so we don't treat it as fatal.
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()

	// Block until a shutdown signal arrives.
	<-ctx.Done()
	log.Println("shutdown signal received")

	// Graceful shutdown, in order:
	//  1. Stop accepting new HTTP requests and let in-flight ones finish (up to 10s).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}
	//  2. Let the worker pool finish in-flight jobs (unpulled jobs stay in Redis).
	pool.Shutdown()

	dead := pool.DeadLetter()
	log.Printf("JobQ: stopped. %d job(s) dead-lettered.", len(dead))
}
