package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/kenshin579/chronos-go"
)

var _ prometheus.Collector = (*QueueCollector)(nil)

// QueueCollector reports per-queue task counts (pending/active/scheduled/retry/
// archived/completed), whether each queue is paused, and whether the scrape
// itself succeeded, read from a chronos.Inspector at scrape time.
type QueueCollector struct {
	insp    *chronos.Inspector
	timeout time.Duration

	tasks  *prometheus.Desc
	paused *prometheus.Desc
	up     *prometheus.Desc
}

// NewQueueCollector returns a collector over the given inspector. Register it
// with a prometheus registry.
func NewQueueCollector(insp *chronos.Inspector) *QueueCollector {
	return &QueueCollector{
		insp:    insp,
		timeout: 5 * time.Second,
		tasks: prometheus.NewDesc(
			"chronos_queue_tasks",
			"Number of tasks in a queue by state.",
			[]string{"queue", "state"}, nil,
		),
		paused: prometheus.NewDesc(
			"chronos_queue_paused",
			"Whether a queue is paused (1) or not (0).",
			[]string{"queue"}, nil,
		),
		// Unlabelled on purpose: it answers "did this scrape work", not
		// "which queue". Without it a Redis outage is indistinguishable from
		// an empty deployment — every gauge simply disappears.
		up: prometheus.NewDesc(
			"chronos_collector_up",
			"Whether the last scrape read queue state successfully.",
			nil, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.tasks
	ch <- c.paused
	ch <- c.up
}

// Collect implements prometheus.Collector: it reads live queue stats per scrape.
func (c *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	queues, err := c.insp.Queues(ctx)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)

	for _, q := range queues {
		if q == nil {
			continue
		}
		g := func(state string, v int64) {
			ch <- prometheus.MustNewConstMetric(c.tasks, prometheus.GaugeValue, float64(v), q.Queue, state)
		}
		g("pending", q.Pending)
		g("active", q.Active)
		g("scheduled", q.Scheduled)
		g("retry", q.Retry)
		g("archived", q.Archived)
		g("completed", q.Completed)

		var paused float64
		if q.Paused {
			paused = 1
		}
		ch <- prometheus.MustNewConstMetric(c.paused, prometheus.GaugeValue, paused, q.Queue)
	}
}
