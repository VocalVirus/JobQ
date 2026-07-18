# JobQ

A durable, concurrent **job queue and scheduler** written in Go. Submit work over HTTP, and JobQ
runs it across a pool of workers with automatic retries, exponential backoff, a dead-letter queue,
delayed/scheduled execution, and full Prometheus metrics — all backed by Redis and PostgreSQL so
nothing is lost across restarts.

Built as a study of the concurrency and reliability patterns behind real task-processing systems
(Sidekiq, Celery, river, etc.), with an emphasis on clean seams: each capability plugs into a small
interface rather than rewriting the core.

---

## Features

- **Concurrent worker pool** — a configurable number of goroutines process jobs in parallel over a
  shared queue.
- **At-least-once delivery** — jobs live in a Redis Stream with a consumer group; a job stays
  *pending* until a worker acknowledges completion, so a crash mid-job means redelivery, never
  silent loss.
- **Automatic retries with exponential backoff + jitter** — transient failures are retried with
  growing, jittered delays (to avoid thundering-herd retries), capped at a maximum.
- **Dead-letter queue** — jobs that exhaust their retry budget are set aside as `dead` for
  inspection instead of being dropped or retried forever.
- **Delayed & scheduled jobs** — submit with `delay_seconds` and the job waits in a Redis sorted
  set until due, then runs through the exact same worker path. Promotion is atomic (a Lua script),
  so multiple JobQ instances can share the schedule without double-running a job.
- **Durable job state** — every job's status (`scheduled → queued → running → succeeded/failed/dead`)
  is persisted in PostgreSQL, so `GET /jobs/{id}` still answers after a restart.
- **Graceful shutdown** — on `SIGINT`/`SIGTERM`, JobQ stops accepting new HTTP requests, lets
  in-flight jobs finish, and leaves everything unstarted safely queued in Redis.
- **Prometheus metrics** — job throughput, success/failure rates, per-job latency histograms,
  active-worker gauge, and live queue depth, exposed at `/metrics`.
- **One-command local stack** — `docker compose up` runs Redis, PostgreSQL, JobQ, and Prometheus
  together.

---

## Architecture

```mermaid
flowchart LR
    client([HTTP client]) -->|POST /jobs| api[API server]
    api -->|record status| pg[(PostgreSQL<br/>job state)]
    api -->|enqueue| redis[(Redis)]

    subgraph redis_detail [Redis]
        stream[["Stream<br/>(ready jobs)"]]
        zset[["Sorted set<br/>(delayed jobs)"]]
    end

    api -->|"delay_seconds > 0"| zset
    api -->|"immediate"| stream
    sched[Scheduler loop<br/>every 1s] -->|promote due| zset
    sched --> stream

    stream -->|XREADGROUP| pool[Worker pool]
    pool -->|run + retry/backoff| pool
    pool -->|ack on success/dead| stream
    pool -->|update status| pg
    pool -->|observe| metrics[/metrics/]
    prom[Prometheus] -->|scrape| metrics
```

**Data flow:** an HTTP `POST /jobs` records the job in Postgres and enqueues it in Redis. Immediate
jobs go straight onto a Redis **Stream**; delayed jobs wait in a **sorted set** and are promoted
onto the stream by a 1-second scheduler loop once due. Workers read from the stream via a consumer
group, process with retries, persist each status transition to Postgres, and **ack** only once a
job reaches a terminal state. Prometheus scrapes `/metrics` for observability.

Two independent durability axes: **Redis** makes the *queue* durable (jobs waiting to run);
**Postgres** makes the *state* durable (what happened to each job).

---

## Quickstart

### Requirements
- [Docker](https://www.docker.com/) + Docker Compose (for the one-command stack), **or**
- [Go 1.26+](https://go.dev/dl/) with a Redis and PostgreSQL reachable locally.

### Run the full stack

```bash
git clone https://github.com/VocalVirus/JobQ.git
cd JobQ
docker compose up -d --build
```

This starts **Redis**, **PostgreSQL**, **JobQ** (`:8080`), and **Prometheus** (`:9090`).

### Submit and track a job

```bash
# Submit a job
curl -X POST localhost:8080/jobs -d '{"payload":"hello"}'
# -> {"id":1,"status":"queued"}

# Check its status
curl localhost:8080/jobs/1
# -> {"id":1,"payload":"hello","status":"succeeded","attempts":1}

# Schedule a job for 30 seconds in the future
curl -X POST localhost:8080/jobs -d '{"payload":"later","delay_seconds":30}'
# -> {"id":2,"status":"scheduled"}
```

### See the metrics

```bash
curl -s localhost:8080/metrics | grep jobq_
```

Or open the Prometheus UI at **http://localhost:9090** and graph
`rate(jobq_jobs_processed_total[1m])` or `jobq_queue_depth`.

### Tear down

```bash
docker compose down
```

---

## HTTP API

| Method & path      | Body                                        | Description                                   |
|--------------------|---------------------------------------------|-----------------------------------------------|
| `POST /jobs`       | `{"payload":"...", "delay_seconds":0}`      | Enqueue a job. `delay_seconds` is optional; `>0` schedules it for later. Returns `202 Accepted` with `{"id","status"}`. |
| `GET /jobs/{id}`   | —                                           | Look up a job's current record: `{"id","payload","status","attempts"}`. `404` if unknown. |
| `GET /healthz`     | —                                           | Liveness check, returns `{"status":"ok"}`.    |
| `GET /metrics`     | —                                           | Prometheus metrics (text exposition format).  |

The API validates input (empty payloads are rejected) and returns `503` if the queue or store is
unreachable, so a caller can safely retry rather than assume a dropped job.

### Job lifecycle

```
scheduled ──(promoted when due)──► queued ──► running ──► succeeded
                                               │  ▲            
                                               │  └── failed ──(retry w/ backoff)
                                               └────────────────► dead  (retries exhausted)
```

| Status      | Meaning                                                       |
|-------------|---------------------------------------------------------------|
| `scheduled` | Accepted, held in the delayed set until its run-at time.      |
| `queued`    | On the stream, waiting for a worker.                          |
| `running`   | A worker is processing it right now.                          |
| `succeeded` | Finished successfully.                                        |
| `failed`    | An attempt failed; a retry is pending.                        |
| `dead`      | Failed too many times; moved to the dead-letter queue.        |

---

## Metrics

Exposed at `GET /metrics` and scraped by the bundled Prometheus.

| Metric | Type | Description |
|--------|------|-------------|
| `jobq_jobs_processed_total{result}` | counter | Jobs reaching a terminal state, by `result` (`succeeded`/`dead`). Use `rate()` for success/failure rates. |
| `jobq_job_retries_total` | counter | Failed attempts that were retried. |
| `jobq_job_duration_seconds{result}` | histogram | Handler execution latency per attempt, by outcome (`succeeded`/`failed`). |
| `jobq_workers_active` | gauge | Handlers executing right now. |
| `jobq_queue_depth{queue}` | gauge | Live depth by partition: `ready` (stream length), `pending` (delivered-but-unacked), `delayed` (scheduled). Read from Redis at scrape time. |

---

## Configuration

JobQ reads a few environment variables (with sensible localhost defaults):

| Variable        | Default                                                        | Purpose                          |
|-----------------|---------------------------------------------------------------|----------------------------------|
| `PORT`          | `8080`                                                        | HTTP listen port.                |
| `REDIS_ADDR`    | `localhost:6379`                                             | Redis address.                   |
| `DATABASE_URL`  | `postgres://jobq:jobq@localhost:5432/jobq?sslmode=disable`   | PostgreSQL DSN.                  |

Worker-pool tuning (worker count, max attempts, backoff bounds) lives in `cmd/jobq/main.go`:

```go
worker.Config{
    NumWorkers:  3,
    MaxAttempts: 4,
    BaseBackoff: 100 * time.Millisecond,
    MaxBackoff:  2 * time.Second,
    Handler:     handler, // your work goes here
}
```

### Plugging in real work

The unit of work is a `job.Handler` — `func(job.Job) error`. The bundled `main.go` ships a
deliberately flaky demo handler (sleeps ~150ms, fails ~30% of the time) so retries and metrics are
visible out of the box. Replace it with your own function; returning a non-nil error is what drives
the retry/backoff/dead-letter machinery.

---

## Project layout

```
cmd/jobq/            Entry point: wiring, HTTP server, scheduler loop, graceful shutdown
internal/
  api/               HTTP handlers (POST /jobs, GET /jobs/{id}, /healthz)
  job/               Core Job type, Status values, and the Handler seam
  queue/             Durable queue: Queue interface + Redis Streams implementation + scheduler
  store/             Job-state store: Store interface + in-memory and PostgreSQL implementations
  worker/            The worker pool, retry loop, and exponential backoff
  metrics/           Prometheus instrumentation and the /metrics handler
Dockerfile           Multi-stage build of the JobQ binary
docker-compose.yml   Redis + PostgreSQL + JobQ + Prometheus
prometheus.yml       Scrape configuration
```

---

## Development

```bash
# Build everything
go build ./...

# Run the test suite (integration tests that need Redis/Postgres skip automatically
# when those services aren't reachable, so this stays green without Docker)
go test ./...

# Vet
go vet ./...
```

To run JobQ against your own Redis/Postgres without Docker, start those services, then:

```bash
go run ./cmd/jobq
```

---

## Design notes

A few decisions worth calling out — the through-line is **plug into a small seam, don't rewrite the
core**:

- **Interfaces at every boundary.** `queue.Queue`, `store.Store`, the `job.Handler` func, and the
  pool's `OnStatus` / `Metrics` hooks each let a capability be added or swapped without touching the
  worker loop. Postgres replaced the in-memory store in one constructor call; metrics were added
  without the worker package ever importing Prometheus.
- **Ack-on-completion, not on-receipt.** A worker holds a job pending in Redis until it's truly
  done, which is what makes at-least-once real across crashes.
- **The scheduler reuses the pipe.** A promoted delayed job is indistinguishable from an immediate
  one — retries, acks, and durability all come for free because there's one execution path, not two.
- **Pull-based queue depth.** Depth is read from Redis inside the Prometheus collector at scrape
  time, so there's no polling loop and the value is never stale-by-design.

---

## Roadmap

- **Bounded-queue backpressure** — cap in-flight work (an admission semaphore and/or Redis stream
  `MAXLEN`) so the system sheds or blocks under overload instead of accepting unboundedly. The
  original in-memory design blocked on a bounded channel; the durable Redis stream is effectively
  unbounded, so this is the next reliability milestone.
- **Dead-letter inspection & requeue API** — HTTP endpoints to list and replay dead-lettered jobs.
- **Configurable handlers / job types** — dispatch on a job `type` field instead of a single global
  handler.
- **Grafana dashboard** — a ready-made board over the Prometheus metrics.

---

## License

No license file is currently included; all rights reserved by the author until one is added. If you
intend this to be open source, add a `LICENSE` (MIT is a common choice).
