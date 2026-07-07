package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/kenshin579/chronos-go"
)

// QueueCollector reports per-queue task counts (pending/active/scheduled/retry/
// archived) as gauges, read from a chronos.Inspector at scrape time.
type QueueCollector struct {
	insp    *chronos.Inspector
	timeout time.Duration
	desc    *prometheus.Desc
}

// NewQueueCollector returns a collector over the given inspector. Register it
// with a prometheus registry.
func NewQueueCollector(insp *chronos.Inspector) *QueueCollector {
	return &QueueCollector{
		insp:    insp,
		timeout: 5 * time.Second,
		desc: prometheus.NewDesc(
			"chronos_queue_tasks",
			"Number of tasks in a queue by state.",
			[]string{"queue", "state"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *QueueCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

// Collect implements prometheus.Collector: it reads live queue stats per scrape.
func (c *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	queues, err := c.insp.Queues(ctx)
	if err != nil {
		return // skip this scrape; a transient Redis error should not crash /metrics
	}
	for _, q := range queues {
		g := func(state string, v int64) {
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(v), q.Queue, state)
		}
		g("pending", q.Pending)
		g("active", q.Active)
		g("scheduled", q.Scheduled)
		g("retry", q.Retry)
		g("archived", q.Archived)
	}
}
