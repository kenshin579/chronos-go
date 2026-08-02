package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/kenshin579/chronos-go"
)

var _ prometheus.Collector = (*SchedulerCollector)(nil)

// SchedulerCollector exports scheduler liveness, read from a chronos.Inspector
// at scrape time.
//
// This answers the question the task counters cannot: is the cron still firing?
// A dead scheduler produces an empty queue, which looks exactly like a healthy
// idle one, so queue depth alone can never tell you.
type SchedulerCollector struct {
	insp    *chronos.Inspector
	timeout time.Duration

	lastFired *prometheus.Desc
	stale     *prometheus.Desc
	leader    *prometheus.Desc
}

// NewSchedulerCollector returns a collector over the given inspector. Register it
// with a prometheus registry.
func NewSchedulerCollector(insp *chronos.Inspector) *SchedulerCollector {
	return &SchedulerCollector{
		insp:    insp,
		timeout: 5 * time.Second,
		// A unix timestamp rather than an age: an age baked in at scrape time
		// drifts with the scrape interval. Compute the age in the query with
		// time() - <metric>.
		lastFired: prometheus.NewDesc(
			"chronos_schedule_last_fired_timestamp_seconds",
			"Unix time a schedule last fired.",
			[]string{"kind", "queue"}, nil,
		),
		// Unlabelled and aggregate on purpose. A per-schedule gauge would be more
		// precise, but this is the shape deployed alerts already query
		// (max(chronos_schedule_stale) > 0), and a single number is what an
		// operator wants for "is the scheduler registry being refreshed at all".
		stale: prometheus.NewDesc(
			"chronos_schedule_stale",
			"Whether any schedule registry entry has gone stale (1) or not (0).",
			nil, nil,
		),
		// One series per scraped instance, all carrying the same leader_id,
		// because the leader is global state read from Redis rather than local
		// state. count() therefore returns the replica count, not the leader
		// count — deduplicate first:
		//
		//	count(max by (leader_id) (chronos_scheduler_leader))
		//
		// 1 is healthy, 0 means no leader, 2+ means split brain.
		leader: prometheus.NewDesc(
			"chronos_scheduler_leader",
			"Set to 1 with the current scheduler leader's instance ID as a label.",
			[]string{"leader_id"}, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (c *SchedulerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.lastFired
	ch <- c.stale
	ch <- c.leader
}

// Collect implements prometheus.Collector: it reads scheduler state per scrape.
func (c *SchedulerCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// SchedulerStatus returns a *SchedulerStatus — guard the nil case explicitly.
	// A nil dereference here would panic inside Collect, and Prometheus swallows
	// that, so the failure would be silent.
	st, err := c.insp.SchedulerStatus(ctx)
	if err != nil || st == nil {
		// Skip this scrape. The health signal lives on chronos_collector_up
		// (QueueCollector); emitting it here too would produce two conflicting
		// series for one metric name when both collectors are registered.
		return
	}

	if st.LeaderID != "" {
		ch <- prometheus.MustNewConstMetric(c.leader, prometheus.GaugeValue, 1, st.LeaderID)
	}

	var anyStale float64
	for _, s := range st.Schedules {
		if s.Stale {
			anyStale = 1
		}
		if s.LastFired.IsZero() {
			// Never fired: exporting 0 would read as "fired at the epoch" and
			// make time() - metric enormous.
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			c.lastFired, prometheus.GaugeValue,
			float64(s.LastFired.Unix()), s.Kind, s.Queue,
		)
	}
	ch <- prometheus.MustNewConstMetric(c.stale, prometheus.GaugeValue, anyStale)
}
