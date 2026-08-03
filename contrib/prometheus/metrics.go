// Package prometheus provides a Prometheus implementation of chronos-go's
// Metrics hook plus a Collector for live queue-depth gauges. It lives in a
// separate module so the chronos-go core stays free of the prometheus dependency.
package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/kenshin579/chronos-go"
)

// Recoverer outcomes reported on chronos_tasks_recovered_total.
const (
	// outcomeRequeued: the task was reclaimed from a crashed worker and made
	// available for another attempt.
	outcomeRequeued = "requeued"
	// outcomeDeadLetter: the task was reclaimed with its retry budget already
	// exhausted and archived without ever reaching a handler. Spelled exactly
	// like chronos.OutcomeDeadLetter so one concept has one spelling across both
	// metric families.
	outcomeDeadLetter = string(chronos.OutcomeDeadLetter)
)

// Option configures Metrics. Options are additive: NewMetrics with no options
// keeps the defaults.
type Option func(*options)

type options struct {
	durationBuckets []float64
}

// WithDurationBuckets sets the histogram buckets for
// chronos_task_duration_seconds. An empty or nil slice keeps DefBuckets.
//
// The default, prometheus.DefBuckets, tops out at 10s — a good fit for
// web-request latency and a poor one for batch work measured in minutes. Once
// every observation exceeds the largest finite bucket they all fall into +Inf,
// and histogram_quantile reports the highest finite bound rather than an error,
// so a p95 panel flatlines at 10s instead of visibly breaking. Pick a range that
// brackets the work:
//
//	NewMetrics(WithDurationBuckets([]float64{1, 5, 15, 30, 60, 120, 300, 600, 1800}))
//
// Changing this on a running deployment re-buckets the histogram: recorded
// counts are not migrated, so quantiles over a window spanning the change mix
// both layouts until the old series age out.
func WithDurationBuckets(buckets []float64) Option {
	return func(o *options) { o.durationBuckets = buckets }
}

var (
	_ chronos.Metrics         = (*Metrics)(nil)
	_ chronos.RecoveryMetrics = (*Metrics)(nil)
	_ prometheus.Collector    = (*Metrics)(nil)
)

// Metrics implements chronos.Metrics using Prometheus counters and a histogram.
// It also implements the optional chronos.RecoveryMetrics, so recoverer activity
// is exported too.
type Metrics struct {
	processed *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	recovered *prometheus.CounterVec
}

// NewMetrics creates the task metrics. It does not register them — the caller
// decides what to do if registration fails, since a metric collision is not a
// reason to kill an application.
//
//	m := prometheus.NewMetrics()
//	if err := reg.Register(m); err != nil {
//		log.Warn("chronos metrics not registered", "error", err)
//	}
func NewMetrics(opts ...Option) *Metrics {
	cfg := options{durationBuckets: prometheus.DefBuckets}
	for _, opt := range opts {
		opt(&cfg)
	}
	if len(cfg.durationBuckets) == 0 {
		cfg.durationBuckets = prometheus.DefBuckets
	}

	return &Metrics{
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronos_tasks_processed_total",
			Help: "Total tasks processed, by queue, kind and outcome.",
		}, []string{"queue", "kind", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chronos_task_duration_seconds",
			Help:    "Task handler duration in seconds, by queue and kind.",
			Buckets: cfg.durationBuckets,
		}, []string{"queue", "kind"}),
		// No kind label: the recoverer reports a count for requeued tasks, not
		// the messages, so the kind is not knowable for that half.
		recovered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronos_tasks_recovered_total",
			Help: "Total tasks reclaimed from crashed workers, by queue and outcome.",
		}, []string{"queue", "outcome"}),
	}
}

// Describe implements prometheus.Collector.
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	m.processed.Describe(ch)
	m.duration.Describe(ch)
	m.recovered.Describe(ch)
}

// Collect implements prometheus.Collector.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	m.processed.Collect(ch)
	m.duration.Collect(ch)
	m.recovered.Collect(ch)
}

// ObserveTask implements chronos.Metrics.
func (m *Metrics) ObserveTask(queue, kind string, outcome chronos.TaskOutcome, dur time.Duration) {
	m.processed.WithLabelValues(queue, kind, string(outcome)).Inc()
	m.duration.WithLabelValues(queue, kind).Observe(dur.Seconds())
}

// ObserveRecovered implements chronos.RecoveryMetrics. The counts are per-sweep
// deltas, so they are added rather than set. An idle sweep adds zero, which
// still creates both child series: an operator can then tell a recoverer that is
// running and finding nothing from one that is not running at all.
func (m *Metrics) ObserveRecovered(queue string, recovered, deadLettered int) {
	m.recovered.WithLabelValues(queue, outcomeRequeued).Add(float64(recovered))
	m.recovered.WithLabelValues(queue, outcomeDeadLetter).Add(float64(deadLettered))
}
