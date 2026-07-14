// Package api is JobQ's "front door": a small HTTP server that lets outside
// callers submit jobs and check their status. It deliberately knows nothing
// about how jobs are run — it just writes to the store and hands work to a
// submit function. That keeps the web layer decoupled from the worker pool.
package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/VocalVirus/jobq/internal/job"
	"github.com/VocalVirus/jobq/internal/store"
)

// Server holds the dependencies the HTTP handlers need.
type Server struct {
	store  *store.Memory       // where job records live (for status lookups)
	submit func(job.Job) error // how to hand a job to the durable queue; may fail
}

// NewServer wires up the routes and returns an http.Handler ready to serve.
//
// It uses Go 1.22+ routing patterns ("METHOD /path/{wildcard}"), so the method
// and path are matched for us — no manual `if r.Method == ...` checks.
func NewServer(st *store.Memory, submit func(job.Job) error) http.Handler {
	s := &Server{store: st, submit: submit}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.handleCreate)
	mux.HandleFunc("GET /jobs/{id}", s.handleGet)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return mux
}

// createRequest is the JSON body expected by POST /jobs.
type createRequest struct {
	Payload string `json:"payload"`
}

// handleCreate accepts a new job, stores it, and queues it for processing.
func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Payload == "" {
		writeError(w, http.StatusBadRequest, "payload must not be empty")
		return
	}

	// Record the job (assigns an ID, marks it queued), then enqueue it durably.
	// If the queue is unreachable (e.g. Redis down) we must NOT report success —
	// tell the caller to retry with a 503 rather than silently dropping the job.
	j := s.store.Add(req.Payload)
	if err := s.submit(j); err != nil {
		writeError(w, http.StatusServiceUnavailable, "could not enqueue job, try again")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"id":     j.ID,
		"status": job.StatusQueued,
	})
}

// handleGet returns the current status of a job by ID.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be an integer")
		return
	}

	rec, ok := s.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

// handleHealth is a liveness check. Docker/Kubernetes will hit this later.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// writeJSON serializes v as JSON with the given status code.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError sends a JSON error body like {"error":"..."}.
func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
