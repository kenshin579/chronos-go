package prometheus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	goredis "github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
)

type schedArgs struct{}

func (schedArgs) Kind() string { return "scheduler:test" }

// The metric an operator actually needs: proof that a schedule fired, and when.
func TestSchedulerCollector_ExportsLastFiredAndLeader(t *testing.T) {
	c := testRedis(t)
	ctx := context.Background()

	sched := chronos.NewScheduler(c, chronos.SchedulerConfig{})
	if err := chronos.RegisterInterval(sched, time.Second, schedArgs{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := sched.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sched.Shutdown(context.Background()) //nolint:errcheck // test cleanup

	// Wait for the first fire rather than sleeping a fixed amount.
	insp := chronos.NewInspector(c)
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

	reg := prometheus.NewRegistry()
	if err := reg.Register(NewSchedulerCollector(insp)); err != nil {
		t.Fatalf("register: %v", err)
	}

	if n := testutil.CollectAndCount(reg, "chronos_schedule_last_fired_timestamp_seconds"); n < 1 {
		t.Error("no chronos_schedule_last_fired_timestamp_seconds series")
	}
	if n := testutil.CollectAndCount(reg, "chronos_scheduler_leader"); n != 1 {
		t.Errorf("chronos_scheduler_leader series = %d, want 1", n)
	}

	want := `
# HELP chronos_schedule_stale Whether any schedule registry entry has gone stale (1) or not (0).
# TYPE chronos_schedule_stale gauge
chronos_schedule_stale 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "chronos_schedule_stale"); err != nil {
		t.Error(err)
	}
}

type multiSpecArgs struct{}

func (multiSpecArgs) Kind() string { return "scheduler:multispec" }

// A schedule's identity is <kind>:<spec>, so one kind can legitimately have
// several schedules. Without the id label they collide, Gather() errors, and
// promhttp answers 500 for the whole endpoint — taking chronos_collector_up
// down with it, which is the one metric that should survive a failure.
func TestSchedulerCollector_SameKindDifferentSpecs(t *testing.T) {
	c := testRedis(t)
	ctx := context.Background()

	sched := chronos.NewScheduler(c, chronos.SchedulerConfig{})
	// Same kind and same queue, two different specs: identical (kind, queue)
	// label pair, two distinct schedule IDs.
	if err := chronos.RegisterInterval(sched, time.Second, multiSpecArgs{}); err != nil {
		t.Fatalf("register 1s: %v", err)
	}
	if err := chronos.RegisterInterval(sched, 2*time.Second, multiSpecArgs{}); err != nil {
		t.Fatalf("register 2s: %v", err)
	}
	if err := sched.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sched.Shutdown(context.Background()) //nolint:errcheck // test cleanup

	// Wait for both schedules to fire rather than sleeping a fixed amount.
	insp := chronos.NewInspector(c)
	deadline := time.After(20 * time.Second)
	for {
		st, err := insp.SchedulerStatus(ctx)
		if err == nil && st != nil && len(st.Schedules) == 2 &&
			!st.Schedules[0].LastFired.IsZero() && !st.Schedules[1].LastFired.IsZero() {
			break
		}
		select {
		case <-deadline:
			t.Fatal("both schedules never fired within 20s")
		case <-time.After(500 * time.Millisecond):
		}
	}

	reg := prometheus.NewRegistry()
	if err := reg.Register(NewSchedulerCollector(insp)); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Gather() is what promhttp calls; a duplicate label set makes it error and
	// the whole scrape 500s, not just this metric.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	var series []*dto.Metric
	for _, mf := range mfs {
		if mf.GetName() == "chronos_schedule_last_fired_timestamp_seconds" {
			series = mf.GetMetric()
		}
	}
	if len(series) != 2 {
		t.Fatalf("got %d last_fired series, want 2 (one per schedule)", len(series))
	}

	// The id label is what makes the two series distinct; kind and queue are
	// identical between them.
	ids := map[string]bool{}
	for _, m := range series {
		var id, kind string
		for _, lp := range m.GetLabel() {
			switch lp.GetName() {
			case "id":
				id = lp.GetValue()
			case "kind":
				kind = lp.GetValue()
			}
		}
		if id == "" {
			t.Error("series has no id label")
		}
		if kind != "scheduler:multispec" {
			t.Errorf("kind = %q, want scheduler:multispec", kind)
		}
		ids[id] = true
	}
	if len(ids) != 2 {
		t.Errorf("got %d distinct id labels, want 2", len(ids))
	}

	// The deployed dashboard queries max by (kind) (time() - metric), so the
	// extra label must not fan the panel out: both series still collapse to a
	// single kind.
	kinds := map[string]bool{}
	for _, m := range series {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "kind" {
				kinds[lp.GetValue()] = true
			}
		}
	}
	if len(kinds) != 1 {
		t.Errorf("max by (kind) would yield %d series, want 1", len(kinds))
	}
}

// A Redis failure must not silently drop the scheduler metrics either.
func TestSchedulerCollector_SilentOnError(t *testing.T) {
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379", DB: 14})
	_ = c.Close()
	insp := chronos.NewInspector(c)

	reg := prometheus.NewRegistry()
	if err := reg.Register(NewSchedulerCollector(insp)); err != nil {
		t.Fatalf("register: %v", err)
	}
	// No panic, no series. chronos_collector_up (QueueCollector) carries the
	// health signal; duplicating it here would produce two conflicting series
	// for the same metric name when both collectors are registered.
	if n := testutil.CollectAndCount(reg, "chronos_scheduler_leader"); n != 0 {
		t.Errorf("got %d series on error, want 0", n)
	}
}
