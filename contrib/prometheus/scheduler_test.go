package prometheus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
