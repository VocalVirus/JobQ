// Package job defines the core unit of work that flows through the system.
//
// Everything in JobQ ultimately revolves around this one type. Redis, the
// worker pool, retries — they all exist to move Jobs around and run them
// reliably. Keeping this package tiny and dependency-free on purpose.
package job

// Job is a single unit of work to be processed by a worker.
//
// For now it's deliberately minimal. In later phases we'll add fields like
// Attempts (for retries) and a Status (queued/running/succeeded/...).
type Job struct {
	ID      int    // unique identifier for this job
	Payload string // the actual data/instructions for the work (kept as a string for now)
}

// Handler is a function that processes one Job.
//
// This is the "what do I actually DO with a job" plug-in point. The worker
// pool doesn't care what the work is — it just calls a Handler. Returning a
// non-nil error means the job failed (which later drives our retry logic).
//
// Defining it as a named function type lets us pass behavior around like a
// value — a very common Go pattern.
type Handler func(j Job) error
