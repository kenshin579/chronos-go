# Complete the Prometheus contrib module — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give adopters scheduler-liveness metrics out of the box, and make `contrib/prometheus` importable from another module at all.

**Architecture:** A new `SchedulerCollector` reads `Inspector.SchedulerStatus()` at scrape time; `QueueCollector` gains the two `QueueInfo` fields it drops plus a health gauge; `NewMetrics` stops self-registering. Metric names and labels are pinned to what a deployed dashboard already queries.

**Tech Stack:** Go 1.26, prometheus/client_golang v1.23.2

**Design doc:** `docs/superpowers/specs/2026-08-02-contrib-scheduler-metrics-design.md` — read it for why none of this belongs in the core, and why `oldest_retry_due` is deleted rather than added.

---

## Prerequisites

```bash
redis-cli ping   # PONG — the collector tests need a real Redis
```

`contrib/prometheus` is a **separate module**. Run its tests from inside its
directory, or via `make test-contrib` from the repo root.

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `contrib/prometheus/go.mod` | Modify | Require the core at a real version |
| `contrib/prometheus/metrics.go` | Modify | `NewMetrics` constructs without registering; implements `prometheus.Collector` |
| `contrib/prometheus/collector.go` | Modify | Emit all 7 `QueueInfo` fields + `chronos_collector_up` |
| `contrib/prometheus/scheduler.go` | Create | `SchedulerCollector` over `Inspector.SchedulerStatus()` |
| `contrib/prometheus/scheduler_test.go` | Create | Scheduler metric tests |
| `contrib/prometheus/collector_test.go` | Create | Queue collector + health tests |
| `contrib/prometheus/metrics_test.go` | Modify | Adapt the three existing tests to the new constructor |
| `contrib/prometheus/cmd/loadgen/main.go` | Modify | The in-repo caller |
| `contrib/prometheus/README.md` | Modify | Document the new metrics and the registration change |

---

### Task 1: Make the module consumable

**Files:** `contrib/prometheus/go.mod`

This is first because it is the defect that made everything else moot — the module
could not be imported at all.

- [ ] **Step 1: Reproduce the failure from outside the repo**

Do this before changing anything, so you know the fix worked rather than assuming:

```bash
cd $(mktemp -d)
printf 'module extcheck\n\ngo 1.26\n' > go.mod
GOFLAGS=-mod=mod go get github.com/kenshin579/chronos-go/contrib/prometheus@latest
```

Expected failure:

```
github.com/kenshin579/chronos-go@v0.0.0-00010101000000-000000000000:
    invalid version: unknown revision 000000000000
```

Record the exact output.

- [ ] **Step 2: Require a real version**

In `contrib/prometheus/go.mod`, change the core requirement from the placeholder
pseudo-version to:

```
require (
	github.com/kenshin579/chronos-go v1.2.0
	...
)
```

**Keep the `replace` line.** Consumers ignore `replace` in a dependency — that is
precisely why it is safe — and it keeps local development testing against the
working tree instead of the published core.

- [ ] **Step 3: Verify both directions**

```bash
cd contrib/prometheus && go build ./... && go test ./... 2>&1 | tail -5
```

Local build still uses the working tree via `replace`. Confirm by making a
trivial temporary change to a core file and checking that contrib sees it, then
revert — **or** simply confirm `go list -m github.com/kenshin579/chronos-go`
reports the replacement path.

The external check cannot pass until this branch is merged and tagged, so it moves
to Task 7.

- [ ] **Step 4: Commit**

```bash
git add contrib/prometheus/go.mod contrib/prometheus/go.sum
git commit -m "fix(contrib): require the core at a real version

contrib/prometheus required the core at a placeholder pseudo-version and
relied on a replace directive to resolve it. A replace in a dependency is
ignored by the consumer, so \`go get\` of this module failed outright with
'invalid version: unknown revision 000000000000'.

The module has therefore never been importable from outside this repo,
which is why adopters hand-rolled their own Prometheus wiring.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 2: Stop `NewMetrics` self-registering

**Files:** `contrib/prometheus/metrics.go`, `contrib/prometheus/metrics_test.go`, `contrib/prometheus/cmd/loadgen/main.go`

`NewMetrics(reg)` calls `reg.MustRegister(...)`, so a duplicate registration
panics. An adopter cannot follow the "log and continue" pattern — killing an app
because a metric collided trades availability for observability.

This is a **breaking change to contrib's API**, which is acceptable: `contrib/*`
is outside the v1 semver guarantee, and per Task 1 the module has never been
externally importable, so there are no external callers.

- [ ] **Step 1: Update the tests first**

In `metrics_test.go`, the three existing tests call `NewMetrics(reg)`. Change each
to the new two-step form:

```go
	reg := prometheus.NewRegistry()
	m := NewMetrics()
	if err := reg.Register(m); err != nil {
		t.Fatalf("register: %v", err)
	}
```

Add one new test:

```go
// NewMetrics must not register itself: an adopter needs to decide what happens
// when registration fails. Killing the app because a metric collided trades
// availability for observability.
func TestNewMetrics_DoesNotSelfRegister(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics()

	if err := reg.Register(m); err != nil {
		t.Fatalf("first register: %v", err)
	}
	// The second registration must return an error, not panic.
	err := reg.Register(m)
	if err == nil {
		t.Fatal("second Register returned nil, want AlreadyRegisteredError")
	}
	var are prometheus.AlreadyRegisteredError
	if !errors.As(err, &are) {
		t.Errorf("got %T, want prometheus.AlreadyRegisteredError", err)
	}
}
```

- [ ] **Step 2: Run — expect a compile failure**

```bash
cd contrib/prometheus && go test ./... 2>&1 | head -5
```

Expected: `too many arguments in call to NewMetrics`.

- [ ] **Step 3: Change the constructor**

In `metrics.go`, drop the parameter and the `MustRegister` line:

```go
// NewMetrics creates the task metrics. It does not register them — the caller
// decides what to do if registration fails, since a metric collision is not a
// reason to kill an application.
//
//	m := prometheus.NewMetrics()
//	if err := reg.Register(m); err != nil {
//		log.Warn("chronos metrics not registered", "error", err)
//	}
func NewMetrics() *Metrics {
	return &Metrics{
		// ... unchanged field initialisation ...
	}
}
```

Add `Describe`/`Collect` so `*Metrics` satisfies `prometheus.Collector`, and
extend the existing compile-time assertion block:

```go
var (
	_ chronos.Metrics         = (*Metrics)(nil)
	_ chronos.RecoveryMetrics = (*Metrics)(nil)
	_ prometheus.Collector    = (*Metrics)(nil)
)

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
```

- [ ] **Step 4: Update the in-repo caller**

`contrib/prometheus/cmd/loadgen/main.go:39-40`:

```go
	metrics := chronosprom.NewMetrics()
	reg.MustRegister(metrics)
	reg.MustRegister(chronosprom.NewQueueCollector(chronos.NewInspector(rdb)))
```

`MustRegister` is fine *here* — loadgen is a throwaway tool, not library code.

- [ ] **Step 5: Test**

```bash
cd contrib/prometheus && go test ./... -race 2>&1 | tail -8
```

- [ ] **Step 6: Commit**

```bash
git add contrib/prometheus/
git commit -m "feat(contrib)!: NewMetrics no longer registers itself

MustRegister panics on a duplicate, which forces adopters to let a metric
collision kill the process. Return an unregistered Collector instead and
let the caller decide.

Breaking for contrib, which is outside the v1 semver guarantee and — until
the previous commit — was not importable from outside this repo at all.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 3: Complete the queue collector

**Files:** `contrib/prometheus/collector.go`, create `contrib/prometheus/collector_test.go`

Two `QueueInfo` fields are dropped (`Completed`, `Paused`), and a Redis error makes
the metrics vanish silently.

- [ ] **Step 1: Write the failing tests**

Create `collector_test.go`:

```go
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
```

- [ ] **Step 2: Run — expect failures**

```bash
cd contrib/prometheus && go test ./... -run QueueCollector 2>&1 | head -20
```

Expected: `completed` and `paused` missing, `chronos_collector_up` absent.

- [ ] **Step 3: Implement**

Rewrite `collector.go`'s descriptors and `Collect`:

```go
type QueueCollector struct {
	insp    *chronos.Inspector
	timeout time.Duration

	tasks  *prometheus.Desc
	paused *prometheus.Desc
	up     *prometheus.Desc
}

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

func (c *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.tasks
	ch <- c.paused
	ch <- c.up
}

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
```

- [ ] **Step 4: Test**

```bash
cd contrib/prometheus && go test ./... -race 2>&1 | tail -8
```

- [ ] **Step 5: Commit**

```bash
git add contrib/prometheus/collector.go contrib/prometheus/collector_test.go
git commit -m "feat(contrib): export every queue field and collector health

The collector emitted five of QueueInfo's seven fields, so a paused queue
and retained completed tasks were invisible. It also returned silently when
Redis failed, making an outage look identical to an empty deployment — every
gauge vanished and every panel read zero.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 4: Scheduler collector

**Files:** Create `contrib/prometheus/scheduler.go`, `contrib/prometheus/scheduler_test.go`

This is the point of the whole change: answering "is my cron still firing?".

- [ ] **Step 1: Confirm the API shape before writing code**

```bash
cd contrib/prometheus
go doc github.com/kenshin579/chronos-go Inspector.SchedulerStatus
go doc github.com/kenshin579/chronos-go SchedulerStatus
go doc github.com/kenshin579/chronos-go ScheduleInfo
```

If any field differs from the design doc, **adapt to the real API and report**.

- [ ] **Step 2: Write the failing tests**

Create `scheduler_test.go`:

```go
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
	defer sched.Shutdown(context.Background())

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
```

- [ ] **Step 3: Implement**

Create `scheduler.go`:

```go
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
		//
		// id is what makes the series unique and must not be dropped. A
		// schedule's identity is ScheduleInfo.ID (<kind>:<spec>#<hash>, the hash
		// covering queue and payload) — kind alone is not an identity: one kind
		// can carry several specs, and fire-history-only entries have kind "".
		// Two such schedules would emit the same (kind, queue) pair, Gather()
		// would reject the duplicate, and promhttp would answer 500 for the
		// entire scrape — taking chronos_collector_up down with it, the one
		// metric that exists to stay visible when things break.
		//
		// The label is free to the deployed dashboard, whose only query is
		// max by (kind) (time() - <metric>): by (kind) collapses everything else.
		lastFired: prometheus.NewDesc(
			"chronos_schedule_last_fired_timestamp_seconds",
			"Unix time a schedule last fired.",
			[]string{"id", "kind", "queue"}, nil,
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
			float64(s.LastFired.Unix()), s.ID, s.Kind, s.Queue,
		)
	}
	ch <- prometheus.MustNewConstMetric(c.stale, prometheus.GaugeValue, anyStale)
}
```

- [ ] **Step 4: Test**

```bash
cd contrib/prometheus && go test ./... -race 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add contrib/prometheus/scheduler.go contrib/prometheus/scheduler_test.go
git commit -m "feat(contrib): export scheduler liveness metrics

Adds last-fired, stale and leader gauges read from Inspector.SchedulerStatus.
Nothing shipped answered 'is my cron still firing?' — a dead scheduler
produces an empty queue, indistinguishable from a healthy idle one — so every
adopter built this themselves on top of the same public API.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 5: Golden names test

**Files:** `contrib/prometheus/names_test.go` (create)

The acceptance criterion is that a deployed dashboard and four alert rules keep
working. Pin the names and labels so a future rename fails loudly here rather
than silently blanking someone's dashboard.

- [ ] **Step 1: Write the test**

```go
package prometheus

import (
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// These names and labels are queried by deployed dashboards and alert rules.
// Renaming one silently blanks a panel, so changes must be deliberate: update
// this test and the dashboards in the same change.
func TestMetricNamesAndLabels(t *testing.T) {
	want := []string{
		`chronos_collector_up|`,
		`chronos_queue_paused|queue`,
		`chronos_queue_tasks|queue,state`,
		`chronos_schedule_last_fired_timestamp_seconds|id,kind,queue`,
		`chronos_schedule_stale|`,
		`chronos_scheduler_leader|leader_id`,
		`chronos_task_duration_seconds|queue,kind`,
		`chronos_tasks_processed_total|queue,kind,outcome`,
		`chronos_tasks_recovered_total|queue,outcome`,
	}

	reg := prometheus.NewRegistry()
	if err := reg.Register(NewMetrics()); err != nil {
		t.Fatalf("register metrics: %v", err)
	}
	if err := reg.Register(NewQueueCollector(nil)); err != nil {
		t.Fatalf("register queue collector: %v", err)
	}
	if err := reg.Register(NewSchedulerCollector(nil)); err != nil {
		t.Fatalf("register scheduler collector: %v", err)
	}

	ch := make(chan *prometheus.Desc, 64)
	go func() {
		NewMetrics().Describe(ch)
		NewQueueCollector(nil).Describe(ch)
		NewSchedulerCollector(nil).Describe(ch)
		close(ch)
	}()

	var got []string
	for d := range ch {
		got = append(got, descSignature(d))
	}
	sort.Strings(got)
	sort.Strings(want)

	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("metric surface changed:\n got: %v\nwant: %v", got, want)
	}
}
```

**`descSignature` is not a standard helper — you must write it.** `prometheus.Desc`
has no public accessor for its name and labels; its `String()` output contains
both. Parse it, or use `dto.Metric` via a gather instead. **Pick an approach that
does not depend on `Desc.String()`'s exact formatting if you can** — that format
is not a stable API. A gather-based version that asserts on the collected metric
families is more robust; prefer it if `NewQueueCollector(nil)` proves awkward
(a nil inspector will panic in `Collect`, so gather-based assertions need a real
or fake inspector).

**Report which approach you chose and why.** If a gather-based test needs Redis,
that is acceptable — skip when unreachable, consistent with the other tests.

- [ ] **Step 2: Make it pass, then commit**

```bash
cd contrib/prometheus && go test ./... -race 2>&1 | tail -6
git add contrib/prometheus/names_test.go
git commit -m "test(contrib): pin the exported metric surface

Deployed dashboards and alert rules query these names and labels by hand.
A rename blanks a panel silently; this test makes it fail loudly instead.

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 6: Documentation

**Files:** `contrib/prometheus/README.md`, `contrib/prometheus/deploy/README.md`

**Two files reference the old API, not one.** Verified:

```
contrib/prometheus/README.md:14         Wire it via `NewMetrics(reg)` and pass it as
contrib/prometheus/deploy/README.md:28  metrics := chronosprom.NewMetrics(reg)
```

The `deploy/` snippet no longer compiles after Task 2. Before finishing, re-run
the check so a third file added later is not missed:

```bash
find . -name '*.md' -not -path './docs/superpowers/*' -exec grep -Hn "NewMetrics(" {} \;
```

Expected after this task: every hit shows `NewMetrics()` with no argument.

- [ ] **Step 1: Update**

The README must cover:

- The full metric table (9 metrics, with labels)
- The registration change — `NewMetrics()` takes no registry; the caller registers
- Register **all three** collectors; `chronos_collector_up` only comes from
  `QueueCollector`
- The `chronos_scheduler_leader` replica caveat, with the deduplicating query
- That `chronos_schedule_last_fired_timestamp_seconds` is a timestamp, so the
  dashboard computes `time() - metric`
- That schedules which have never fired are invisible (a `SchedulerStatus`
  limitation, not this module's)

- [ ] **Step 2: Commit**

```bash
git add contrib/prometheus/README.md
git commit -m "docs(contrib): document the scheduler metrics and the registration change

Co-Authored-By: Claude <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01VzVTM8ViMq87z461eA7Ure"
```

---

### Task 7: Gate and PR

- [ ] **Step 1: Full check**

```bash
cd /Users/frankoh/src/workspace_inspireme/chronos-go
make check 2>&1 | tail -15
```

- [ ] **Step 2: Confirm the core is untouched**

This change is contrib-only. Verify:

```bash
git diff --stat main...HEAD -- ':!contrib' ':!docs'
```

Expected: **no output.** If a core file changed, stop and report — the design says
none of this belongs in the core.

- [ ] **Step 3: PR, then stop**

```bash
git push -u origin feat/contrib-scheduler-metrics
gh pr create --base main --title "feat(contrib): scheduler liveness metrics, and make the module importable"
```

The body must lead with the two independent defects — nothing answers "is my cron
firing?", and the module cannot be imported at all — and state that the metric
names match what deployed dashboards already query.

**Stop after opening the PR.** Merging is the user's decision.

- [ ] **Step 4: After merge — verify external consumability**

This is the check that cannot pass from inside the repo, and the one that proves
Task 1 actually worked. It needs a contrib tag to exist.

```bash
cd $(mktemp -d)
printf 'module extcheck\n\ngo 1.26\n' > go.mod
GOFLAGS=-mod=mod go get github.com/kenshin579/chronos-go/contrib/prometheus@<tag>
cat > main.go <<'EOF'
package main

import (
	"github.com/kenshin579/chronos-go"
	chronosprom "github.com/kenshin579/chronos-go/contrib/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:6379"})
	insp := chronos.NewInspector(rdb)
	reg := prometheus.NewRegistry()
	_ = reg.Register(chronosprom.NewMetrics())
	_ = reg.Register(chronosprom.NewQueueCollector(insp))
	_ = reg.Register(chronosprom.NewSchedulerCollector(insp))
}
EOF
go build ./...
```

Expected: builds. Compare against the Task 1 Step 1 failure.

---

## Completion criteria

- [ ] `go get` of contrib from a scratch module outside the repo succeeds (Task 7 Step 4)
- [ ] All nine metrics exported with the names and labels the golden test pins
- [ ] `chronos_collector_up` reports 0 on a Redis failure instead of vanishing
- [ ] `chronos_queue_tasks` includes `completed`; `chronos_queue_paused` exists
- [ ] Scheduler metrics prove a real schedule fired, against a real Redis
- [ ] `NewMetrics` returns an unregistered collector; double registration errors rather than panics
- [ ] `make check` green
- [ ] **No file outside `contrib/` and `docs/` changed**

## Out of scope

- `chronos_queue_oldest_retry_due_seconds` — not derivable from the public API; the
  deployed panel querying it should be deleted (a dashboard change, not this one)
- Making never-fired schedules visible — belongs in the core's schedule registry
- Switching either application to contrib, and the dashboard rework — separate
  changes in separate repositories
