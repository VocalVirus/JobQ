// Package queue is the durable job queue: where jobs live between being
// submitted and being processed. Until now that queue was an in-memory Go
// channel (lost on restart). This package backs it with Redis instead, so
// jobs survive crashes — the foundation of "at-least-once, no silent loss".
//
// The Queue interface hides the implementation. The worker pool will depend on
// this interface, not on Redis directly, so we could swap in another broker
// later without touching the pool.
package queue

import (
	"context"
	"errors"

	"github.com/VocalVirus/jobq/internal/job"
)

// ErrNoJob is returned by Dequeue when it waited but no job arrived in time.
// It's a normal "nothing to do right now" signal, not a failure.
var ErrNoJob = errors.New("queue: no job available")

// Message is one dequeued job plus an opaque handle used to acknowledge it.
// The caller processes Message.Job, then calls Ack(msg) to confirm success.
// The handle is unexported: callers pass the Message back to Ack without
// needing to know it's really a Redis stream entry ID.
type Message struct {
	Job job.Job
	id  string // broker-specific ack handle (Redis stream entry ID)
}

// Queue is a durable, at-least-once job queue.
type Queue interface {
	// Enqueue adds a job to the queue.
	Enqueue(ctx context.Context, j job.Job) error
	// Dequeue blocks (up to an internal timeout) for the next job. It returns
	// ErrNoJob if none arrived — the caller should simply try again.
	Dequeue(ctx context.Context) (Message, error)
	// Ack confirms a message was processed so the queue can drop it. Until a
	// message is acked it stays pending and can be redelivered — that's what
	// makes delivery "at-least-once" even if a worker crashes mid-job.
	Ack(ctx context.Context, m Message) error
	// Close releases the underlying connection.
	Close() error
}
