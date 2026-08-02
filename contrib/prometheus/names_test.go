package prometheus

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/kenshin579/chronos-go"
)

type goldenArgs struct{}

func (goldenArgs) Kind() string { return "golden:test" }

// gatheredSurface returns one "name|label,names" line per gathered metric family,
// with the label names sorted so the result does not depend on the order the
// descriptors declare them in.
//
// This reads the gathered *dto.MetricFamily rather than prometheus.Desc: Desc
// exposes its name and labels only through String(), whose format is not part of
// the client's stable API, whereas the gathered protobuf is exactly what
// promhttp serialises and is documented. It also means the test asserts on what a
// scrape actually returns, not on what the collectors merely promised to return.
func gatheredSurface(t *testing.T, reg *prometheus.Registry) []string {
	t.Helper()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	out := make([]string, 0, len(mfs))
	for _, mf := range mfs {
		names := map[string]bool{}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				names[lp.GetName()] = true
			}
		}
		labels := make([]string, 0, len(names))
		for n := range names {
			labels = append(labels, n)
		}
		sort.Strings(labels)
		out = append(out, mf.GetName()+"|"+strings.Join(labels, ","))
	}
	sort.Strings(out)
	return out
}

// These names and labels are queried by deployed dashboards and alert rules.
// Renaming one silently blanks a panel, so changes must be deliberate: update
// this test and the dashboards in the same change.
//
// Every family has to be made non-empty first — an unused CounterVec exports
// nothing, and a queue or schedule that does not exist has no series — so the
// test drives real traffic through a real Redis and a real scheduler.
func TestMetricNamesAndLabels(t *testing.T) {
	// Label names are sorted, so these read alphabetically rather than in
	// declaration order. A histogram gathers as a single family
	// (chronos_task_duration_seconds), not as _bucket/_sum/_count.
	want := []string{
		`chronos_collector_up|`,
		`chronos_queue_paused|queue`,
		`chronos_queue_tasks|queue,state`,
		// id is load-bearing: without it two schedules of the same kind and queue
		// produce a duplicate label set, Gather() errors, and promhttp answers 500
		// for the entire endpoint.
		`chronos_schedule_last_fired_timestamp_seconds|id,kind,queue`,
		`chronos_schedule_stale|`,
		`chronos_scheduler_leader|leader_id`,
		`chronos_task_duration_seconds|kind,queue`,
		`chronos_tasks_processed_total|kind,outcome,queue`,
		`chronos_tasks_recovered_total|outcome,queue`,
	}
	sort.Strings(want)

	c := testRedis(t)
	ctx := context.Background()
	insp := chronos.NewInspector(c)

	// A queue must exist for chronos_queue_tasks and chronos_queue_paused.
	if _, err := chronos.Enqueue(ctx, chronos.NewClient(c), goldenArgs{}, chronos.WithQueue("default")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// A leader and a fired schedule must exist for the scheduler gauges.
	sched := chronos.NewScheduler(c, chronos.SchedulerConfig{})
	if err := chronos.RegisterInterval(sched, time.Second, goldenArgs{}); err != nil {
		t.Fatalf("register schedule: %v", err)
	}
	if err := sched.Start(ctx); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}
	defer sched.Shutdown(context.Background()) //nolint:errcheck // test cleanup

	deadline := time.After(20 * time.Second)
	for {
		st, err := insp.SchedulerStatus(ctx)
		if err == nil && st != nil && st.LeaderID != "" &&
			len(st.Schedules) > 0 && !st.Schedules[0].LastFired.IsZero() {
			break
		}
		select {
		case <-deadline:
			t.Fatal("schedule never fired within 20s")
		case <-time.After(500 * time.Millisecond):
		}
	}

	// An unused CounterVec/HistogramVec has no children and gathers as nothing,
	// so drive one observation of each through the Metrics hook.
	m := NewMetrics()
	m.ObserveTask("default", "golden:test", chronos.OutcomeSuccess, time.Millisecond)
	m.ObserveRecovered("default", 1, 1)

	reg := prometheus.NewRegistry()
	if err := reg.Register(m); err != nil {
		t.Fatalf("register metrics: %v", err)
	}
	if err := reg.Register(NewQueueCollector(insp)); err != nil {
		t.Fatalf("register queue collector: %v", err)
	}
	if err := reg.Register(NewSchedulerCollector(insp)); err != nil {
		t.Fatalf("register scheduler collector: %v", err)
	}

	got := gatheredSurface(t, reg)

	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("metric surface changed:\n got: %v\nwant: %v", got, want)
	}
}
