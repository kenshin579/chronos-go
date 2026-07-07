// Package prometheus provides a Prometheus implementation of chronos-go's
// Metrics hook plus a Collector for live queue-depth gauges. It lives in a
// separate module so the chronos-go core stays free of the prometheus dependency.
package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/kenshin579/chronos-go"
)

// Metrics implements chronos.Metrics using Prometheus counters and a histogram.
type Metrics struct {
	processed *prometheus.CounterVec
	duration  *prometheus.HistogramVec
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
	}
	reg.MustRegister(m.processed, m.duration)
	return m
}

// ObserveTask implements chronos.Metrics.
func (m *Metrics) ObserveTask(queue, kind string, outcome chronos.TaskOutcome, dur time.Duration) {
	m.processed.WithLabelValues(queue, kind, string(outcome)).Inc()
	m.duration.WithLabelValues(queue, kind).Observe(dur.Seconds())
}
