# chronos-go Prometheus metrics

> **Experimental.** This contrib module is versioned separately and is **not**
> covered by the core library's v1 stability guarantee. Its API and behavior
> may change between releases.

A Prometheus implementation of chronos-go's core `Metrics` hook, plus collectors
for live queue depth and scheduler liveness. It lives in a separate module
(`github.com/kenshin579/chronos-go/contrib/prometheus`) so the core stays free
of the Prometheus dependency.

```bash
go get github.com/kenshin579/chronos-go/contrib/prometheus
```

## Wiring

There are **three** things to register, and they are independent — registering
only some of them leaves gaps described under each metric below.

```go
reg := prometheus.NewRegistry()
insp := chronos.NewInspector(rdb)

// NewMetrics no longer takes a registry and no longer registers itself: a
// metric collision is not a reason to kill an application, so the caller
// decides what happens when registration fails.
metrics := chronosprom.NewMetrics()
for _, c := range []prometheus.Collector{
    metrics,
    chronosprom.NewQueueCollector(insp),
    chronosprom.NewSchedulerCollector(insp),
} {
    if err := reg.Register(c); err != nil {
        log.Printf("chronos metrics not registered: %v", err) // log and continue
    }
}

srv := chronos.NewServer(rdb, chronos.ServerConfig{Metrics: metrics /* ... */})
http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
```

`metrics` also implements the optional `chronos.RecoveryMetrics`, so recoverer
activity is exported with no extra wiring — the server discovers it by type
assertion.

## Metrics

| Metric | Type | Labels | Source |
|---|---|---|---|
| `chronos_tasks_processed_total` | counter | `queue`, `kind`, `outcome` | `Metrics` |
| `chronos_task_duration_seconds` | histogram | `queue`, `kind` | `Metrics` |
| `chronos_tasks_recovered_total` | counter | `queue`, `outcome` | `Metrics` |
| `chronos_queue_tasks` | gauge | `queue`, `state` | `QueueCollector` |
| `chronos_queue_paused` | gauge | `queue` | `QueueCollector` |
| `chronos_collector_up` | gauge | — | `QueueCollector` |
| `chronos_schedule_last_fired_timestamp_seconds` | gauge | `id`, `kind`, `queue` | `SchedulerCollector` |
| `chronos_schedule_stale` | gauge | — | `SchedulerCollector` |
| `chronos_scheduler_leader` | gauge | `leader_id` | `SchedulerCollector` |

`state` is one of `pending`, `active`, `scheduled`, `retry`, `archived`,
`completed`. These names and labels are pinned by `names_test.go` — a rename
blanks someone's dashboard panel silently, so it has to fail loudly in CI first.

### `chronos_collector_up`

`QueueCollector` reads Redis at scrape time. When that read fails it emits
`chronos_collector_up 0` and nothing else, so a Redis outage is distinguishable
from an empty deployment — otherwise every gauge simply disappears and every
panel reads zero.

**Only `QueueCollector` exports it.** `SchedulerCollector` deliberately does not:
two collectors emitting the same unlabelled metric would produce a duplicate
label set, `Gather()` would error, and `promhttp` would answer 500 for the whole
endpoint. So if you register `SchedulerCollector` alone, you get no health
signal — register `QueueCollector` too.

## Scheduler liveness

This answers the question the task counters cannot: **is my cron still firing?**
A dead scheduler produces an empty queue, which looks exactly like a healthy idle
one, so queue depth alone can never tell you.

### `chronos_schedule_last_fired_timestamp_seconds`

A **unix timestamp**, not an age. An age baked in at scrape time drifts with the
scrape interval, so compute it in the query:

```promql
max by (kind) (time() - chronos_schedule_last_fired_timestamp_seconds)
```

The `id` label is a schedule's real identity (`<kind>:<spec>#<hash>`). It is
required, not decorative: one kind can carry several specs, and two such
schedules would otherwise emit the same `(kind, queue)` pair — a duplicate label
set that makes `Gather()` error and takes the entire scrape down with it,
including `chronos_collector_up`. Dashboards that aggregate `by (kind)` collapse
it away and are unaffected.

**Schedules that have never fired are invisible.** Exporting `0` for them would
read as "fired at the epoch" and make `time() - metric` enormous, so they are
skipped instead. Distinguishing "registered but never fired" from "not
registered" needs the core's schedule registry to expose it; that is a core
limitation, not one this module can fix.

### `chronos_scheduler_leader`

The leader is global state read from Redis, so **every replica exports the same
`leader_id`** — one series per scraped instance, all identical. A naive
`count(chronos_scheduler_leader)` therefore returns your replica count and reads
as split brain. Deduplicate first:

```promql
count(max by (leader_id) (chronos_scheduler_leader))
```

`1` is healthy, `0` means no leader is holding the lock, `2+` is genuine split
brain. When the scrape fails, no series is emitted at all — `absent()` catches
that case, and `chronos_collector_up` says whether Redis is the reason.

### `chronos_schedule_stale`

Unlabelled and aggregate on purpose: `1` if **any** schedule registry entry has
gone stale, `0` otherwise. Alert with `max(chronos_schedule_stale) > 0`.

## Recovery metrics

`chronos_tasks_recovered_total{queue, outcome}` counts tasks the recoverer
reclaimed from a crashed worker's pending list:

| `outcome` | meaning |
|---|---|
| `requeued` | reclaimed and made available for another attempt |
| `dead_letter` | reclaimed with the retry budget already exhausted, so archived without ever reaching a handler |

There is no `kind` label: the recoverer reports a count for requeued tasks, not
the messages, so the kind is not knowable for that half.

The server reports every recoverer sweep, including one that reclaimed nothing,
so both series exist from the first sweep onward. That is deliberate — it lets
you alert on a recoverer that has stopped running (`absent(...)`), not just on
one that is busy.

**Boundary with `chronos_tasks_processed_total`.**
`chronos_tasks_processed_total{outcome="dead_letter"}` counts only tasks that
failed through a handler. A task dead-lettered by the recoverer never reached a
handler and has no duration, so it appears **only** as
`chronos_tasks_recovered_total{outcome="dead_letter"}`. The label value is
spelled the same in both families, but they are separate metrics — union them
explicitly to count every abandoned task:

```promql
(sum(increase(chronos_tasks_processed_total{outcome="dead_letter"}[5m])) or vector(0))
+ (sum(increase(chronos_tasks_recovered_total{outcome="dead_letter"}[5m])) or vector(0))
```

`or vector(0)` on each operand is not optional: a bare `+` returns no data
whenever either side has no series yet — which is the normal state before the
first handler-path dead-letter — so the panel would read "No data" instead of
`0`. It is safe here only because both operands are bare `sum()` with no `by`
clause, so the zero-label `vector(0)` matches.

The dashboard in [`deploy/`](deploy) uses exactly this query.

A ready-to-run Prometheus + Grafana stack lives in [`deploy/`](deploy).
