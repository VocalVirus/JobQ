package store

// This file is the durable twin of Memory: the same Store behavior, but backed
// by PostgreSQL so a job's status survives a process restart or crash. Redis
// already made the *queue* durable (jobs waiting to run); Postgres makes the
// *state* durable (what happened to each job), so GET /jobs/{id} keeps working
// after a restart instead of forgetting every job it ever saw.

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/VocalVirus/jobq/internal/job"
)

// schema is created on connect. Using IF NOT EXISTS keeps startup idempotent —
// safe to run every boot without a separate migration step for a local project.
// BIGSERIAL gives Postgres ownership of ID assignment (via RETURNING id below),
// replacing Memory's hand-rolled counter.
const schema = `
CREATE TABLE IF NOT EXISTS jobs (
    id       BIGSERIAL PRIMARY KEY,
    payload  TEXT NOT NULL,
    status   TEXT NOT NULL,
    attempts INT  NOT NULL DEFAULT 0
)`

// Postgres is a durable, concurrency-safe job store backed by PostgreSQL.
type Postgres struct {
	// pool is a connection pool, not a single connection: our worker goroutines
	// call SetStatus concurrently, and a pool hands each one its own connection
	// instead of serializing them through one. It's safe for concurrent use.
	pool *pgxpool.Pool
}

// NewPostgres connects to Postgres using a DSN like
// "postgres://user:pass@host:5432/dbname", verifies the connection, and ensures
// the jobs table exists. It returns an error (rather than panicking) so main can
// fail fast with a clear message if the database isn't up yet.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}
	// New() is lazy — it doesn't actually dial until first use. Ping now so a bad
	// DSN or a down database surfaces here at startup, not on the first request.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: create schema: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Add inserts a new queued job and returns it with the ID Postgres assigned.
// RETURNING id gives us the generated primary key in the same round-trip as the
// INSERT — no separate SELECT needed. We pass status as a plain string because
// job.Status is just a named string type; string(...) makes the intent explicit.
func (p *Postgres) Add(ctx context.Context, payload string) (job.Job, error) {
	var id int
	err := p.pool.QueryRow(ctx,
		`INSERT INTO jobs (payload, status) VALUES ($1, $2) RETURNING id`,
		payload, string(job.StatusQueued),
	).Scan(&id)
	if err != nil {
		return job.Job{}, fmt.Errorf("store: add job: %w", err)
	}
	return job.Job{ID: id, Payload: payload}, nil
}

// Get loads one job by ID. pgx.ErrNoRows is the normal "no such job" signal, so
// we translate it to (false, nil) rather than treating it as a real error —
// only genuine query failures come back as a non-nil error.
func (p *Postgres) Get(ctx context.Context, id int) (Record, bool, error) {
	var status string
	var r Record
	err := p.pool.QueryRow(ctx,
		`SELECT id, payload, status, attempts FROM jobs WHERE id = $1`, id,
	).Scan(&r.ID, &r.Payload, &status, &r.Attempts)
	if err == pgx.ErrNoRows {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("store: get job %d: %w", id, err)
	}
	r.Status = job.Status(status)
	return r, true, nil
}

// SetStatus updates a job's status and attempt count. It can't take a context or
// return an error (the worker pool's OnStatus hook signature is fixed), so it
// manages its own short timeout and logs failures instead of propagating them.
// A dropped status write is a lost *observation*, not a lost job — the job still
// lives durably in Redis until it's acked — but it's a real gap worth knowing.
func (p *Postgres) SetStatus(id int, status job.Status, attempts int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.pool.Exec(ctx,
		`UPDATE jobs SET status = $1, attempts = $2 WHERE id = $3`,
		string(status), attempts, id,
	)
	if err != nil {
		log.Printf("store: SetStatus(id=%d, status=%s) failed: %v", id, status, err)
	}
}

// Close releases the connection pool. Call it on shutdown.
func (p *Postgres) Close() { p.pool.Close() }
