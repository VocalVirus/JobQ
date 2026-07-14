package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/VocalVirus/jobq/internal/job"
	"github.com/VocalVirus/jobq/internal/store"
)

// newTestServer builds a server backed by a real store and a submit function
// that just records what was submitted, so tests don't need a running pool.
func newTestServer() (http.Handler, *[]job.Job) {
	st := store.NewMemory()
	var submitted []job.Job
	srv := NewServer(st, func(j job.Job) error { submitted = append(submitted, j); return nil })
	return srv, &submitted
}

func TestCreateJob(t *testing.T) {
	srv, submitted := newTestServer()

	req := httptest.NewRequest("POST", "/jobs", strings.NewReader(`{"payload":"hello"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /jobs status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if len(*submitted) != 1 || (*submitted)[0].Payload != "hello" {
		t.Fatalf("expected 1 submitted job with payload %q, got %+v", "hello", *submitted)
	}
}

func TestCreateJobEmptyPayload(t *testing.T) {
	srv, _ := newTestServer()

	req := httptest.NewRequest("POST", "/jobs", strings.NewReader(`{"payload":""}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("empty payload status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGetUnknownJob(t *testing.T) {
	srv, _ := newTestServer()

	req := httptest.NewRequest("GET", "/jobs/999", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET unknown job status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
