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

var (
	_ chronos.Metrics         = (*Metrics)(nil)
	_ chronos.RecoveryMetrics = (*Metrics)(nil)
)

// Metrics implements chronos.Metrics using Prometheus counters and a histogram.
// It also implements the optional chronos.RecoveryMetrics, so recoverer activity
// is exported too.
type Metrics struct {
	processed *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	recovered *prometheus.CounterVec
}

// NewMetrics creates the task metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronos_tasks_processed_total",
			Help: "Total tasks processed, by queue, kind and outcome.",
		}, []string{"queue", "kind", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chronos_task_duration_seconds",
			Help:    "Task handler duration in seconds, by queue and kind.",
			Buckets: prometheus.DefBuckets,
		}, []string{"queue", "kind"}),
		// No kind label: the recoverer reports a count for requeued tasks, not
		// the messages, so the kind is not knowable for that half.
		recovered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chronos_tasks_recovered_total",
			Help: "Total tasks reclaimed from crashed workers, by queue and outcome.",
		}, []string{"queue", "outcome"}),
	}
	reg.MustRegister(m.processed, m.duration, m.recovered)
	return m
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
