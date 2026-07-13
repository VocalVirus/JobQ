package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VocalVirus/jobq/internal/job"
)

// TestRedisQueueRoundTrip is an integration test: it needs a real Redis at
// localhost:6379 (start one with `docker compose up -d`). If Redis isn't
// reachable, the test skips rather than fails, so `go test ./...` stays green
// without Docker.
func TestRedisQueueRoundTrip(t *testing.T) {
	// Unique stream per run so repeated tests don't interfere.
	stream := fmt.Sprintf("jobq:test:%d", time.Now().UnixNano())

	q, err := NewRedisQueue("localhost:6379", stream, "workers", "tester")
	if err != nil {
		t.Skipf("redis not available, skipping integration test: %v", err)
	}
	defer q.Close()

	ctx := context.Background()
	want := job.Job{ID: 42, Payload: "hello-redis"}

	if err := q.Enqueue(ctx, want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	msg, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if msg.Job.ID != want.ID || msg.Job.Payload != want.Payload {
		t.Fatalf("Dequeue returned %+v, want %+v", msg.Job, want)
	}

	if err := q.Ack(ctx, msg); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}
