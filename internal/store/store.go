// Package store keeps track of every job's current state so callers can look it
// up by ID. It ships with two interchangeable implementations behind the Store
// interface: an in-memory map (Memory, great for tests and demos) and a
// PostgreSQL-backed one (Postgres) whose state survives restarts. Because the
// rest of the app depends only on the Store interface, swapping one for the
// other is a one-line change in main.
package store

import (
	"context"
	"sync"

	"github.com/VocalVirus/jobq/internal/job"
)

// Record is what we remember about one job.
type Record struct {
	ID       int        `json:"id"`
	Payload  string     `json:"payload"`
	Status   job.Status `json:"status"`
	Attempts int        `json:"attempts"`
}

// Store is the behavior the rest of JobQ needs from a job-state store. Depending
// on this interface (rather than a concrete type) is what lets us swap the
// in-memory map for PostgreSQL without touching the API or worker layers.
//
// Note that SetStatus takes no context and returns no error: it has to match the
// worker pool's OnStatus hook signature exactly (func(id, status, attempts)).
// The Postgres implementation therefore handles its own timeout and logs write
// failures internally — an honest limitation of fitting that fixed hook shape.
type Store interface {
	// Add records a new job (assigning an ID, marking it queued) and returns it.
	Add(ctx context.Context, payload string) (job.Job, error)
	// Get looks up a job by ID. The bool is false if no such job exists.
	Get(ctx context.Context, id int) (Record, bool, error)
	// SetStatus updates a job's status and attempt count as it's processed.
	SetStatus(id int, status job.Status, attempts int)
}

// Memory is a concurrency-safe, in-memory job store.
type Memory struct {
	// RWMutex allows many concurrent readers (status lookups) OR one writer
	// (a status update) at a time — a good fit since reads outnumber writes.
	mu      sync.RWMutex
	records map[int]Record
	nextID  int
}

// NewMemory creates an empty store.
func NewMemory() *Memory {
	return &Memory{records: make(map[int]Record)}
}

// Add registers a new job with the given payload, assigns it an ID, marks it
// "queued", and returns the job.Job to hand to the worker pool. The ctx is
// unused here (an in-memory map can't block or fail) but is part of the Store
// interface so the Postgres implementation can honor cancellation/timeouts.
func (m *Memory) Add(_ context.Context, payload string) (job.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextID++
	id := m.nextID
	m.records[id] = Record{
		ID:      id,
		Payload: payload,
		Status:  job.StatusQueued,
	}
	return job.Job{ID: id, Payload: payload}, nil
}

// SetStatus updates a job's status and attempt count. Called by the worker pool
// as the job moves through its lifecycle.
func (m *Memory) SetStatus(id int, status job.Status, attempts int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	r, ok := m.records[id]
	if !ok {
		return // unknown job; nothing to update
	}
	r.Status = status
	r.Attempts = attempts
	m.records[id] = r
}

// Get returns the record for a job ID. The bool is false if no such job exists.
// The error is always nil for the in-memory store; it exists to satisfy Store.
func (m *Memory) Get(_ context.Context, id int) (Record, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.records[id]
	return r, ok, nil
}
