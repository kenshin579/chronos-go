# chronos-go

[![CI](https://github.com/kenshin579/chronos-go/actions/workflows/ci.yml/badge.svg)](https://github.com/kenshin579/chronos-go/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/kenshin579/chronos-go.svg)](https://pkg.go.dev/github.com/kenshin579/chronos-go)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Redis-backed **distributed task queue and scheduler** for Go, with a
type-safe generic API.

chronos-go is a modern alternative to [asynq](https://github.com/hibiken/asynq)
(now in maintenance mode). It keeps what people loved about asynq — a simple
`enqueue → handle` model, at-least-once delivery, crash recovery — and fixes its
biggest gaps: a **distributed scheduler that runs each job once across many
instances**, unbounded stream/dead-letter growth, and unique-lock expiry during
long processing.

> Status: **v0.x** — usable and covered by tests against real Redis, but the API
> may still evolve before v1.0.0.

## Highlights

- **Type-safe, generic API** — no `interface{}` payloads, no manual
  `json.Unmarshal`. Define a task type, register a `Handler[T]`.
- **Redis Streams + ZSETs** — immediate work rides a Streams consumer group;
  delayed/retry/archived tasks live in sorted sets. Cluster-safe (hash-tagged
  keys).
- **Reliable** — automatic retries with exponential backoff + jitter, crash
  recovery (`XAUTOCLAIM`), dead-letter with an `OnDeadLetter` hook.
- **Distributed scheduler** — interval & cron jobs. A Redis leader election
  ensures **only one instance enqueues** each trigger; a deterministic task ID
  fences duplicates during leader hand-off.
- **Delayed execution & de-duplication** — `WithProcessIn` / `WithProcessAt`,
  and `WithUnique` to collapse duplicate work.
- **Weighted priority queues** — queue weights are honored via smooth weighted
  round-robin (no starvation), or set `StrictPriority` to always drain
  higher-weight queues first.
- **Heartbeat** — refreshes the lease and unique lock of in-flight tasks, so a
  long-running task is neither reclaimed nor loses its lock mid-processing.
- **Self-cleaning** — a janitor trims dead-lettered tasks by age and count, so
  Redis memory stays bounded.
- **Observable** — an `Inspector` API + a `chronos` CLI, a runnable tour, and a
  Prometheus + Grafana stack in `contrib/prometheus`.

## Requirements

- Go 1.26+
- Redis 6.2+ (uses `XAUTOCLAIM`)

## Install

```bash
go get github.com/kenshin579/chronos-go
```

## Quick start

Define a task, enqueue it with a client, and process it with a server.

```go
package main

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/kenshin579/chronos-go"
)

// A task type: any struct with a stable Kind() (value receiver).
type EmailArgs struct {
	UserID string `json:"user_id"`
	Body   string `json:"body"`
}

func (EmailArgs) Kind() string { return "email:send" }

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	ctx := context.Background()

	// --- worker side ---
	mux := chronos.NewMux()
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[EmailArgs]) error {
		// t.Args is strongly typed — no casting, no json.Unmarshal.
		log.Printf("sending to %s: %s", t.Args.UserID, t.Args.Body)
		return nil
	})

	srv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 10,
	})
	if err := srv.Start(ctx, mux); err != nil {
		log.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	// --- producer side ---
	client := chronos.NewClient(rdb)
	defer client.Close()

	if _, err := chronos.Enqueue(ctx, client, EmailArgs{UserID: "u1", Body: "hi"}); err != nil {
		log.Fatal(err)
	}

	select {} // keep the worker running
}
```

### Enqueue options

```go
chronos.Enqueue(ctx, client, EmailArgs{...},
	chronos.WithQueue("critical"),          // route to a queue
	chronos.WithMaxRetry(5),                 // retry budget (default 25)
	chronos.WithProcessIn(30*time.Minute),   // run later (delayed)
	chronos.WithUnique(10*time.Minute),      // dedup identical (kind+payload) tasks
	chronos.WithDeadLetterDiscard(),         // drop instead of archive on exhaustion
)
```

### Handler outcomes

- return `nil` → success (acked and removed).
- return an `error` → retried with backoff until `MaxRetry` is exhausted, then
  dead-lettered.
- return `chronos.SkipRetry(err)` → dead-lettered immediately (permanent error).
- `panic` → recovered, treated as a retryable error.

Set an `OnDeadLetter` hook on `ServerConfig` to alert on / inspect exhausted tasks.

## Queue priority

`ServerConfig.Queues` maps queue name → weight. While every queue has work, a
queue with weight 6 is dequeued about 6× as often as a queue with weight 1
(smooth weighted round-robin — lower-weight queues never starve). When the
queue chosen for a round is empty, that round falls through to the
highest-weight queue that does have work, so an idle high-priority queue never
holds up lower ones. Weights `<= 0` are treated as `1`.

```go
srv := chronos.NewServer(rdb, chronos.ServerConfig{
	Queues: map[string]int{
		"critical": 6,
		"default":  3,
		"low":      1,
	},
	// StrictPriority: true, // always drain critical first, then default, then low
})
```

With `StrictPriority: true`, higher-weight queues are always drained first; a
lower-weight queue runs only while every higher one is empty.

## Scheduling (interval & cron)

Register periodic jobs on a `Scheduler`. Every instance may call `Start`; only
the elected leader enqueues, so a job fires **once** cluster-wide.

```go
sched := chronos.NewScheduler(rdb, chronos.SchedulerConfig{})

// every 30s (interval must be >= 1s)
chronos.RegisterInterval(sched, 30*time.Second, HealthCheckArgs{})

// standard 5-field cron
chronos.RegisterCron(sched, "0 0 * * *", DailyReportArgs{})

sched.Start(ctx)          // safe on every instance
defer sched.Shutdown(ctx)
```

Missed triggers (after a leader-election gap) are handled by a per-job
`WithMisfirePolicy` — `MisfireSkip` (default) or `MisfireFireOnce`.

## Observability

chronos-go is a headless library, so it ships tools to *see* what it is doing:

- **Runnable tour** — every feature end-to-end, printed as it happens:
  ```bash
  go run ./examples/tour
  ```
- **CLI** — inspect and administer queues/tasks:
  ```bash
  go run ./cmd/chronos queue ls
  go run ./cmd/chronos task ls default archived
  go run ./cmd/chronos task run default <id>   # re-run a dead-letter
  go run ./cmd/chronos task rm  default <id>
  ```
- **Inspector API** — the same data programmatically (`chronos.NewInspector`).
- **Prometheus + Grafana** — a `Metrics` hook (core, dependency-free) plus a
  ready stack in [`contrib/prometheus`](contrib/prometheus):
  ```bash
  cd contrib/prometheus/deploy && docker compose up --build
  # Grafana: http://localhost:3000  (dashboard "chronos-go")
  ```

See [`docs/OBSERVING.md`](docs/OBSERVING.md) for Redis-level inspection.

## How it works

- **Immediate queue**: a Redis Stream per queue with one consumer group. Workers
  `XREADGROUP` (blocking), run the handler, then `XACK` + `XDEL`. Queues are
  selected by smooth weighted round-robin (or strictly by weight with
  `StrictPriority`).
- **Delayed / retry / archived**: sorted sets scored by run-at / retry-at /
  died-at; a forwarder promotes due entries back into the stream.
- **Crash recovery**: a recoverer `XAUTOCLAIM`s entries whose worker went silent
  and re-queues or dead-letters them (attempts tracked in the task hash).
- **Scheduler**: a leader (Redis `SET NX PX` lock + pub/sub resignation) runs the
  tick loop; each trigger is enqueued under a deterministic dedup key
  (`<schedule>:<trigger-unix>`) so a split-brain hand-off cannot double-enqueue.
- **Heartbeat**: refreshes in-flight tasks' PEL idle (`XCLAIM ... JUSTID`) and
  unique-lock TTL so long-running work is safe.
- **Keys** are wrapped in a `{queue}` hash tag so every multi-key Lua script is
  Redis Cluster-safe.

## Redis Cluster

chronos-go works on Redis Cluster out of the box. Every key of a queue is
wrapped in a `{queue}` hash tag, so a queue's keys share one slot (multi-key
Lua stays atomic) while different queues spread across the cluster.

```go
rdb := redis.NewClusterClient(&redis.ClusterOptions{
	Addrs: []string{"node1:6379", "node2:6379", "node3:6379"},
})
srv := chronos.NewServer(rdb, chronos.ServerConfig{ /* ... */ })
```

The CLI connects with `--cluster` (seed nodes, comma-separated — one is enough):

```bash
chronos --cluster --redis node1:6379,node2:6379 queue ls
```

Notes:
- Redis Cluster has only logical database 0 (`--db` is standalone-only).
- The global keys (`chronos:queues`, the scheduler leader lock) are accessed
  with single-key commands only, so they are cluster-safe without a hash tag.
- Sentinel: inject a `redis.NewFailoverClient` — it satisfies the same
  `redis.UniversalClient` interface — but Sentinel is not part of our tested
  matrix yet.

### Verifying against a real cluster

The repo ships a disposable 6-node cluster and a script-complete integration
suite (every Lua script and command pattern runs on cluster at least once):

```bash
cd deploy/redis-cluster && docker compose up -d && cd ../..
make test-cluster
```

## Delivery semantics

chronos-go is **at-least-once**: a task can run more than once (e.g. a worker
crashes after finishing but before acking). **Make handlers idempotent.**

## vs. asynq

| | asynq | chronos-go |
|---|---|---|
| Payload API | `[]byte` + manual unmarshal | generic `Task[T]` (type-safe) |
| Scheduler across instances | app must ensure a single scheduler | built-in leader election + deterministic dedup |
| Unique lock during long processing | can expire (TTL only) | heartbeat renews it |
| Stream / dead-letter growth | — | trimmed (`XDEL`) + janitor retention |
| Backend | Redis | Redis |

## Known limitations / roadmap

- Scheduler fencing relies on a dedup-key TTL (no monotonic fencing token); a
  leader paused longer than the TTL could theoretically re-enqueue a trigger.
  All instances must share the same `Location` and reasonably synced clocks.
- The unique lock is heartbeat-renewed only while a task is *actively
  processing*; while it waits in the scheduled/retry set, it is covered by its
  TTL — set the TTL comfortably above expected waiting time.
- Not yet built: completed-task retention, a web UI, workflows (chains/groups).

## Development

Tests run against a real Redis (skipped if none is reachable at `$REDIS_ADDR`,
default `127.0.0.1:6379`). They share a logical DB, so run packages serially:

```bash
make check        # gofmt + vet + go test ./... -race -p 1 + contrib tests
```

Cluster integration tests are opt-in: `make test-cluster` (see
[`deploy/redis-cluster`](deploy/redis-cluster)).

## License

[MIT](LICENSE)
