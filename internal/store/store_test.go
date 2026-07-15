package store

import (
	"context"
	"testing"

	"github.com/VocalVirus/jobq/internal/job"
)

// TestAddThenGet checks that a newly added job is retrievable and starts queued.
func TestAddThenGet(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()

	j, err := m.Add(ctx, "hello")
	if err != nil {
		t.Fatalf("Add returned error: %v", err)
	}
	if j.ID != 1 {
		t.Fatalf("first job ID = %d, want 1", j.ID)
	}

	rec, ok, _ := m.Get(ctx, j.ID)
	if !ok {
		t.Fatalf("Get(%d) returned ok=false, want the record", j.ID)
	}
	if rec.Payload != "hello" {
		t.Errorf("Payload = %q, want %q", rec.Payload, "hello")
	}
	if rec.Status != job.StatusQueued {
		t.Errorf("Status = %q, want %q", rec.Status, job.StatusQueued)
	}
}

// TestSetStatus checks that a status/attempts update is reflected on read.
func TestSetStatus(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	j, _ := m.Add(ctx, "work")

	m.SetStatus(j.ID, job.StatusSucceeded, 2)

	rec, ok, _ := m.Get(ctx, j.ID)
	if !ok {
		t.Fatalf("Get(%d) returned ok=false after SetStatus", j.ID)
	}
	if rec.Status != job.StatusSucceeded {
		t.Errorf("Status = %q, want %q", rec.Status, job.StatusSucceeded)
	}
	if rec.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", rec.Attempts)
	}
}

// TestGetUnknown checks that looking up a non-existent job reports ok=false.
func TestGetUnknown(t *testing.T) {
	m := NewMemory()
	if _, ok, _ := m.Get(context.Background(), 999); ok {
		t.Errorf("Get(999) returned ok=true, want false for unknown job")
	}
}
