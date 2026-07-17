// Package job defines the core unit of work that flows through the system.
//
// Everything in JobQ ultimately revolves around this one type. Redis, the
// worker pool, retries — they all exist to move Jobs around and run them
// reliably. Keeping this package tiny and dependency-free on purpose.
package job

// Job is a single unit of work to be processed by a worker.
//
// For now it's deliberately minimal. In later phases we'll add fields like
// a Status (queued/running/succeeded/...) and timestamps.
type Job struct {
	ID       int    // unique identifier for this job
	Payload  string // the actual data/instructions for the work (kept as a string for now)
	Attempts int    // how many times we've tried to process this job (starts at 0)
}

// Status describes where a job is in its lifecycle. Tracking this is what lets
// a caller submit a job and later ask "is it done yet?".
type Status string

const (
	StatusScheduled Status = "scheduled" // accepted, but held until a future run-at time
	StatusQueued    Status = "queued"    // accepted, waiting for a worker
	StatusRunning   Status = "running"   // a worker is processing it right now
	StatusSucceeded Status = "succeeded" // finished successfully
	StatusFailed    Status = "failed"    // an attempt failed; a retry is pending
	StatusDead      Status = "dead"      // failed too many times; moved to dead-letter
)

// Handler is a function that processes one Job.
//
// This is the "what do I actually DO with a job" plug-in point. The worker
// pool doesn't care what the work is — it just calls a Handler. Returning a
// non-nil error means the job failed, which is what drives our retry logic.
//
// Defining it as a named function type lets us pass behavior around like a
// value — a very common Go pattern.
type Handler func(j Job) error
