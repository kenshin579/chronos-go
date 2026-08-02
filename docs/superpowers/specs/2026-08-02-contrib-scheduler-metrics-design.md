# Complete the Prometheus contrib module

Date: 2026-08-02

## Background

`contrib/prometheus` exposes three task counters and one queue gauge:

```go
chronos_tasks_processed_total{queue,kind,outcome}
chronos_task_duration_seconds{queue,kind}
chronos_tasks_recovered_total{queue,outcome}
chronos_queue_tasks{queue,state}      // pending, active, scheduled, retry, archived
```

That is everything an operator gets from the library today.

## Problem

**Nothing tells you whether the scheduler is alive.**

The most valuable question about a scheduler — "is my cron still firing?" — has no
answer in the shipped metrics. `chronos_queue_tasks` reports depth, but a dead
scheduler produces an empty queue, which is indistinguishable from a healthy idle
one. So does a scheduler that lost leadership and never regained it.

Every adopter has to build this themselves. In the deployment that motivated this
design, one application hand-rolled five metrics on top of `Inspector`:

| metric | labels |
|---|---|
| `chronos_schedule_last_fired_timestamp_seconds` | `kind` |
| `chronos_schedule_stale` | — |
| `chronos_scheduler_leader` | `leader_id` |
| `chronos_queue_paused` | `queue` |
| `chronos_collector_up` | — |

A second application then adopted chronos-go and faced the same gap: either copy
those five, or accept a dashboard where half the panels read "No data". Copying
would have made three implementations of the same thing, and a fourth for the next
adopter.

**The data is already there.** Every one of those five is derivable from the
existing public, semver-stable API:

```go
type SchedulerStatus struct {
	LeaderID  string
	Schedules []ScheduleInfo
}

type ScheduleInfo struct {
	ID, Kind, Queue, Spec string
	LastFired, LastSeen   time.Time
	Stale                 bool
}

type QueueInfo struct {
	Queue                                                    string
	Pending, Active, Scheduled, Retry, Archived, Completed    int64
	Paused                                                   bool
}
```

`Inspector.SchedulerStatus()` is never called by the collector, and
`QueueCollector` emits five of `QueueInfo`'s seven fields — it drops `Completed`
and `Paused`. The library has the data and does not expose it.

### The module is also unpublishable

Independently of the metric gap, `contrib/prometheus` cannot be consumed from
outside this repository at all:

```
$ go get github.com/kenshin579/chronos-go/contrib/prometheus@latest
go: github.com/kenshin579/chronos-go@v0.0.0-00010101000000-000000000000:
    invalid version: unknown revision 000000000000
```

`contrib/prometheus/go.mod` requires the core at a placeholder pseudo-version and
relies on `replace ../../` to resolve it. **A `replace` directive in a dependency
is ignored by the consumer**, so the placeholder is what an external module
actually tries to fetch, and it does not exist.

This explains why the two applications in question both hand-rolled their metrics:
neither could have imported contrib even if it had been complete.

## Goals

- An adopter gets scheduler liveness metrics without writing any Prometheus code.
- `contrib/prometheus` is importable from another module.
- The metric names and labels match what is already deployed, so the existing
  dashboard and alert rules keep working when an application switches to contrib.

## Non-goals

- **Moving any of this into the core package.** The core `Metrics` interface is
  dependency-free and push-based — one observation per processed task. Scheduler
  liveness is pull-based state read at scrape time. Different shape, and adding a
  Prometheus dependency to the core would destroy the property that makes it
  embeddable. contrib is the right home.
- `chronos_queue_oldest_retry_due_seconds`. One deployed dashboard has a panel for
  it, but **no application has ever emitted it** — it has been empty since the
  dashboard shipped. It is not derivable from the current public API (it needs the
  minimum score of the retry ZSET), and `chronos_queue_tasks{state="retry"}`
  already signals retry buildup. The panel should be deleted rather than the
  metric added.
- Changing the core's semver guarantees. `contrib/*` is explicitly outside them.

---

## Design

### 1. Make the module consumable

In `contrib/prometheus/go.mod`, require a real published version:

```
require github.com/kenshin579/chronos-go v1.2.0
```

Keep `replace github.com/kenshin579/chronos-go => ../../`. It is ignored by
consumers — which is exactly what makes it safe — and it keeps `make test-contrib`
running against local core changes during development.

This works because every metric added here reads existing v1.2.0 API. If contrib
ever needs a core change, the core must be released first.

### 2. Scheduler metrics

A new collector reads `Inspector.SchedulerStatus()` at scrape time:

| metric | type | labels | source |
|---|---|---|---|
| `chronos_schedule_last_fired_timestamp_seconds` | gauge | `kind`, `queue` | `ScheduleInfo.LastFired` |
| `chronos_schedule_stale` | gauge | — | 1 if any `ScheduleInfo.Stale` |
| `chronos_scheduler_leader` | gauge | `leader_id` | `SchedulerStatus.LeaderID` |

`chronos_schedule_stale` is deliberately unlabelled and aggregate. A per-schedule
version would be more precise, but the deployed alert is `max(chronos_schedule_stale) > 0`
and the deployed dashboard panel is `max(chronos_schedule_stale) or vector(0)` —
both work against an unlabelled gauge. Adding labels would leave those queries
correct but change the panel from a single number to a series per schedule.
Matching what is deployed is worth more than the extra precision here.

`chronos_scheduler_leader` carries the leader's instance ID as a label and the
value 1. Instances that are not the leader report the same series, because the
leader ID is global state read from Redis, not local state — so a deployment with
N replicas produces N identical series. The deployed dashboard already accounts
for this with `count(max by (leader_id) (chronos_scheduler_leader))`, which
deduplicates to 1. **This is documented on the metric**, because the naive
`count(chronos_scheduler_leader)` returns the replica count and reads as
split-brain.

`LastFired` is exported as a unix timestamp rather than an age, so the dashboard
computes `time() - metric`. Ages baked in at scrape time drift with scrape
interval; timestamps do not.

Schedules that have never fired are invisible here — `SchedulerStatus`'s own doc
says so. That is a real gap, but closing it belongs in the core's schedule
registry, not in a metrics wrapper.

### 3. Complete the queue collector

Add the two `QueueInfo` fields the collector currently drops:

- `chronos_queue_tasks{state="completed"}`
- `chronos_queue_paused{queue}` — 1 when paused

`completed` becomes the sixth state on an existing metric, so the deployed
`max by (state) (chronos_queue_tasks{...})` panel gains a series without breaking.

### 4. Collector health

```
chronos_collector_up   gauge, no labels — 1 if the last scrape read Redis successfully
```

Today `QueueCollector.Collect` returns silently when `Inspector.Queues()` errors.
The metrics vanish, and "Redis is unreachable" looks identical to "there are no
queues" — on a dashboard where every panel then reads zero. Emitting `0` instead
makes the failure visible, and it is what the deployed alert
`min(chronos_collector_up) == bool 0` already expects.

### 5. Registration ergonomics

`NewMetrics(reg)` calls `reg.MustRegister(...)` internally and panics on duplicate
registration. That prevents an adopter from following the common "log and continue
if a metric fails to register" pattern — killing an application because a metric
collided trades availability for observability.

Change `NewMetrics` to **construct without registering** and implement
`prometheus.Collector`, matching `NewQueueCollector`, which already gets this
right. The caller registers:

```go
m := prometheus.NewMetrics()
if err := reg.Register(m); err != nil { log.Warn(...) }
```

**This is a breaking change to contrib's API.** It is acceptable — `contrib/*` is
outside the v1 semver guarantee and, as established above, has never been
consumable from outside this repository, so there are no external callers to break.
The in-repo caller (`contrib/prometheus/cmd`) is updated in the same change.

---

## Compatibility

The names and labels above are exactly what one deployment already emits and what
the deployed Grafana dashboard and four alert rules already query. Verified
against the live dashboard JSON and alerting ConfigMap:

```promql
max by (kind) (time() - chronos_schedule_last_fired_timestamp_seconds)
max(chronos_schedule_stale) or vector(0)
count(max by (leader_id) (chronos_scheduler_leader)) or vector(0)
max(chronos_queue_paused{queue="..."}) or vector(0)
min(chronos_collector_up) or vector(0)
```

An application switching from its hand-rolled implementation to contrib must see
no dashboard change. That is the acceptance criterion.

## Testing

| Test | What it proves |
|---|---|
| Golden metric names and labels | The deployed dashboard's queries still match. Assert the exact `Desc` strings. |
| Scheduler collector against a real Redis with a running scheduler | `last_fired` advances, `leader` reports the elected instance |
| `stale` when the registry is not refreshed | The alert's condition can actually become true |
| `collector_up = 0` on Redis failure | The silent-vanish failure mode is gone. Point the Inspector at a closed client. |
| All 7 `QueueInfo` fields emitted | `completed` and `paused` are no longer dropped |
| `NewMetrics` does not self-register | Registering twice returns `AlreadyRegisteredError` instead of panicking |
| **External consumability** | From a scratch module outside the repo, `go get` the published contrib version and compile a program that uses it. This is the check that the `replace`/pseudo-version defect is actually fixed — it cannot be caught from inside the repo, where `replace` masks it. |

## Rollout

1. This change, released as a contrib tag
2. The application that hand-rolled the metrics deletes its implementation and
   imports contrib — its dashboard must not change
3. The second application does the same, gaining the five metrics it lacked
4. The dashboard drops its `queue` hardcoding and its dead
   `oldest_retry_due` panel, and becomes app-selectable

Steps 2-4 are separate changes in separate repositories.
