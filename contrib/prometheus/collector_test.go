package prometheus

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	goredis "github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
)

// testRedis returns a client against a real Redis, skipping if unreachable.
// Uses DB 14 to stay clear of the core suite's DB 15.
func testRedis(t *testing.T) goredis.UniversalClient {
	t.Helper()
	addr := "127.0.0.1:6379"
	c := goredis.NewClient(&goredis.Options{Addr: addr, DB: 14})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis unreachable at %s: %v", addr, err)
	}
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	t.Cleanup(func() { _ = c.FlushDB(context.Background()); _ = c.Close() })
	return c
}

type qcArgs struct{}

func (qcArgs) Kind() string { return "collector:test" }

// All seven QueueInfo fields must be exported. The collector previously dropped
// Completed and Paused, so a paused queue was invisible.
func TestQueueCollector_EmitsAllStates(t *testing.T) {
	c := testRedis(t)
	ctx := context.Background()
	insp := chronos.NewInspector(c)

	if _, err := chronos.Enqueue(ctx, chronos.NewClient(c), qcArgs{}, chronos.WithQueue("default")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	reg := prometheus.NewRegistry()
	if err := reg.Register(NewQueueCollector(insp)); err != nil {
		t.Fatalf("register: %v", err)
	}

	for _, state := range []string{"pending", "active", "scheduled", "retry", "archived", "completed"} {
		if n := testutil.CollectAndCount(reg, "chronos_queue_tasks"); n == 0 {
			t.Fatalf("no chronos_queue_tasks series at all")
		}
		found := false
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatalf("gather: %v", err)
		}
		for _, mf := range mfs {
			if mf.GetName() != "chronos_queue_tasks" {
				continue
			}
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "state" && l.GetValue() == state {
						found = true
					}
				}
			}
		}
		if !found {
			t.Errorf("state %q not exported", state)
		}
	}
}

func TestQueueCollector_ExportsPaused(t *testing.T) {
	c := testRedis(t)
	ctx := context.Background()
	insp := chronos.NewInspector(c)

	if _, err := chronos.Enqueue(ctx, chronos.NewClient(c), qcArgs{}, chronos.WithQueue("default")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := insp.PauseQueue(ctx, "default"); err != nil {
		t.Fatalf("pause: %v", err)
	}

	reg := prometheus.NewRegistry()
	if err := reg.Register(NewQueueCollector(insp)); err != nil {
		t.Fatalf("register: %v", err)
	}

	want := `
# HELP chronos_queue_paused Whether a queue is paused (1) or not (0).
# TYPE chronos_queue_paused gauge
chronos_queue_paused{queue="default"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "chronos_queue_paused"); err != nil {
		t.Error(err)
	}
}

// A Redis failure must be visible. Previously Collect returned silently, so
// "Redis is down" and "there are no queues" looked identical — every panel zero.
func TestQueueCollector_ReportsCollectorDownOnError(t *testing.T) {
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379", DB: 14})
	_ = c.Close() // force every command to fail
	insp := chronos.NewInspector(c)

	reg := prometheus.NewRegistry()
	if err := reg.Register(NewQueueCollector(insp)); err != nil {
		t.Fatalf("register: %v", err)
	}

	want := `
# HELP chronos_collector_up Whether the last scrape read queue state successfully.
# TYPE chronos_collector_up gauge
chronos_collector_up 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "chronos_collector_up"); err != nil {
		t.Error(err)
	}
}

func TestQueueCollector_ReportsCollectorUpOnSuccess(t *testing.T) {
	c := testRedis(t)
	insp := chronos.NewInspector(c)

	reg := prometheus.NewRegistry()
	if err := reg.Register(NewQueueCollector(insp)); err != nil {
		t.Fatalf("register: %v", err)
	}

	want := `
# HELP chronos_collector_up Whether the last scrape read queue state successfully.
# TYPE chronos_collector_up gauge
chronos_collector_up 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "chronos_collector_up"); err != nil {
		t.Error(err)
	}
}
