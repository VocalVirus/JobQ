// Command jobq is the entry point for the JobQ service.
//
// Phase 3: JobQ is now an HTTP service. Jobs arrive from outside via a REST API
// (POST /jobs), the worker pool processes them with retries, and callers can
// poll GET /jobs/{id} for status. Shuts down gracefully on Ctrl+C: it stops
// accepting HTTP requests, then drains in-flight jobs. Still in-memory (no
// Redis/Postgres yet), so state is lost on restart — that's the next phase.
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

	// The store tracks each job's status; the pool reports transitions to it.
	st := store.NewMemory()

	pool := worker.NewPool(worker.Config{
		NumWorkers:  3,
		QueueSize:   100,
		MaxAttempts: 4,
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
		Handler:     handler,
		OnStatus:    st.SetStatus, // wire pool status updates into the store
	})
	pool.Start(ctx)

	// HTTP server. Port is :8080 by default, overridable via the PORT env var.
	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}
	srv := &http.Server{
		Addr:    addr,
		Handler: api.NewServer(st, pool.Submit),
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
	//  2. Drain the worker pool (finish jobs already queued).
	pool.Shutdown()

	dead := pool.DeadLetter()
	log.Printf("JobQ: stopped. %d job(s) dead-lettered.", len(dead))
}
