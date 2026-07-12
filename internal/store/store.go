// Package store keeps track of every job's current state so callers can look it
// up by ID. For now it's an in-memory map guarded by a mutex; in a later phase
// this same responsibility moves to PostgreSQL (so state survives restarts).
// Keeping it behind this small type means the rest of the app won't change when
// we swap the implementation.
package store

import (
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
// "queued", and returns the job.Job to hand to the worker pool.
func (m *Memory) Add(payload string) job.Job {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextID++
	id := m.nextID
	m.records[id] = Record{
		ID:      id,
		Payload: payload,
		Status:  job.StatusQueued,
	}
	return job.Job{ID: id, Payload: payload}
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
func (m *Memory) Get(id int) (Record, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.records[id]
	return r, ok
}
