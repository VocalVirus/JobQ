// Package metrics is JobQ's observability layer: it owns every Prometheus
// metric and the /metrics endpoint. It's the ONLY package that imports the
// Prometheus client, on purpose — the worker pool and queue stay ignorant of it
// and report through small seams (a hook interface, a DepthSource). That's the
// same "plug in, don't rewrite" pattern as OnStatus and the Store interface.
//
// Two kinds of metric, gathered two different ways:
//
//   - Event metrics (jobs processed, retries, durations, active workers) are
//     PUSHED to the Recorder by the worker pool as jobs run — counters and a
//     histogram that only ever move forward or track an in-flight count.
//   - Queue depth is PULLED at scrape time: a custom collector asks Redis "how
//     deep is the queue right now?" only when Prometheus scrapes, so there's no
//     background polling loop and the number is never stale-by-design.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Recorder holds the event metrics and implements the worker pool's Metrics
// hook (WorkerBusy/WorkerIdle/JobFinished/JobRetried). The pool satisfies that
// interface structurally by holding a *Recorder, so this package never imports
// the worker package — the dependency points one way only.
type Recorder struct {
	// jobsProcessed counts jobs that reached a terminal state, split by result
	// ("succeeded" or "dead"). rate() over this gives success/failure rates.
	jobsProcessed *prometheus.CounterVec
	// retries counts individual failed attempts that will be retried. It's
	// separate from the terminal counter so a job that fails twice then succeeds
	// shows up as 2 retries + 1 success, not lost.
	retries prometheus.Counter
	// duration is a histogram of how long each Handler invocation took, labelled
	// by that attempt's outcome ("succeeded" | "failed"). This is the "job
	// latency" signal — quantiles come from histogram_quantile() at query time.
	duration *prometheus.HistogramVec
	// active tracks how many handlers are executing right now (a gauge goes up
	// and down). A useful saturation signal: if it pins at NumWorkers, the pool
	// is the bottleneck.
	active prometheus.Gauge
}

// New registers the event metrics on the default Prometheus registry and returns
// a Recorder to hand to the worker pool. Call it once at startup.
func New() *Recorder {
	return &Recorder{
		jobsProcessed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "jobq_jobs_processed_total",
			Help: "Total jobs that reached a terminal state, by result (succeeded|dead).",
		}, []string{"result"}),
		retries: promauto.NewCounter(prometheus.CounterOpts{
			Name: "jobq_job_retries_total",
			Help: "Total failed attempts that were retried (excludes the final terminal attempt).",
		}),
		duration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name: "jobq_job_duration_seconds",
			Help: "Duration of each job handler invocation, by attempt outcome (succeeded|failed).",
			// Buckets span sub-millisecond to a few seconds — the handler in this
			// project runs ~150ms, so these straddle it with room on both sides.
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"result"}),
		active: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "jobq_workers_active",
			Help: "Number of job handlers currently executing.",
		}),
	}
}

// --- worker.Metrics hook implementation ---------------------------------------
// These four methods are what the pool calls. They're deliberately tiny: all the
// mapping from "what happened" to "which metric" lives here, not in the pool.

// WorkerBusy marks that a worker has started running a handler.
func (r *Recorder) WorkerBusy() { r.active.Inc() }

// WorkerIdle marks that a worker has finished running a handler. Paired with
// WorkerBusy (the pool defers it), so the gauge always balances out.
func (r *Recorder) WorkerIdle() { r.active.Dec() }

// JobFinished records a job reaching a terminal state. result is "succeeded" or
// "dead"; d is how long that final handler call took. A dead job's last attempt
// errored, so its duration is filed under the "failed" histogram label even
// though the terminal counter says "dead".
func (r *Recorder) JobFinished(result string, d time.Duration) {
	r.jobsProcessed.WithLabelValues(result).Inc()
	outcome := "succeeded"
	if result != "succeeded" {
		outcome = "failed"
	}
	r.duration.WithLabelValues(outcome).Observe(d.Seconds())
}

// JobRetried records a failed attempt that will be retried. d is that attempt's
// handler duration, filed under the "failed" label.
func (r *Recorder) JobRetried(d time.Duration) {
	r.retries.Inc()
	r.duration.WithLabelValues("failed").Observe(d.Seconds())
}

// --- Queue depth (pull model) -------------------------------------------------

// DepthSource is anything that can report current queue depth. *queue.RedisQueue
// satisfies it structurally (via its Depth method), so this package doesn't
// import queue either — same one-way-dependency discipline as the worker hook.
type DepthSource interface {
	// Depth returns the current sizes of the three job holding areas:
	// ready (waiting in the stream), pending (delivered but unacked), and
	// delayed (scheduled for the future).
	Depth(ctx context.Context) (ready, pending, delayed int64, err error)
}

// depthCollector is a custom prometheus.Collector that reads queue depth from a
// DepthSource at scrape time. Implementing Collector (rather than setting a
// Gauge on a ticker) means Redis is queried only when Prometheus actually
// scrapes, and the value is exactly as fresh as the scrape — no polling loop,
// no staleness window.
type depthCollector struct {
	src  DepthSource
	desc *prometheus.Desc
}

// Describe sends the single metric descriptor. Required by prometheus.Collector.
func (c *depthCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect queries the source and emits one gauge sample per queue partition. On
// error it emits nothing for this scrape (better a gap than a wrong number); a
// short timeout keeps a slow Redis from stalling the whole /metrics response.
func (c *depthCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ready, pending, delayed, err := c.src.Depth(ctx)
	if err != nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(ready), "ready")
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(pending), "pending")
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(delayed), "delayed")
}

// RegisterQueueDepth wires a DepthSource into the default registry as the
// jobq_queue_depth{queue="ready|pending|delayed"} gauge. Call it once after the
// queue is built.
func (r *Recorder) RegisterQueueDepth(src DepthSource) {
	prometheus.MustRegister(&depthCollector{
		src: src,
		desc: prometheus.NewDesc(
			"jobq_queue_depth",
			"Current number of jobs by queue partition (ready|pending|delayed), read at scrape time.",
			[]string{"queue"}, nil,
		),
	})
}

// Handler returns the HTTP handler that serves the metrics in Prometheus's text
// exposition format. Mount it at /metrics.
func Handler() http.Handler { return promhttp.Handler() }
