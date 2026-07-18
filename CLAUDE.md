# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

chronos-go is a Redis-backed **distributed task queue and scheduler** for Go with a type-safe generic API (`Task[T]`), positioned as a modern alternative to asynq. The public API is the root package `github.com/kenshin579/chronos-go`; everything under `internal/` is private, and `contrib/` modules are experimental and versioned separately.

## Commands

Tests run against a **real Redis** (no fakes). A test is skipped — not failed — if Redis is unreachable at `$REDIS_ADDR` (default `127.0.0.1:6379`).

```bash
make test          # go test ./... -p 1
make test-race     # same, with -race
make check         # fmt-check + vet + test-race + contrib tests + bench build (must be green for a PR)
make vet
make fmt           # gofmt -w .

# Run a single test (still needs -p 1 across packages, but for one package/test just target it):
go test -run TestServer_Heartbeat ./... -p 1
go test -run TestScheduler ./ -p 1        # root package only
```

**`-p 1` is mandatory.** Package test binaries share one logical Redis DB (DB 15) and FLUSH it; running packages in parallel clobbers each other's state. Never drop `-p 1` from a multi-package `go test` invocation.

Other targets:

```bash
make test-cluster  # opt-in; needs the 6-node cluster from deploy/redis-cluster (docker compose up -d)
make bench         # benchmark matrix vs asynq (benchmarks/ is a separate module, FLUSHES DB 15)
make soak          # 1h leak soak; run 4h before a major release
make soak-quick    # 3m wiring check (informational)
```

`contrib/prometheus` and `contrib/webui` are **separate Go modules** — run their tests from inside their directory (`make test-contrib` handles prometheus).

## Architecture

The core idea: **immediate work rides a Redis Stream; everything time-based lives in ZSETs; a forwarder promotes due ZSET entries back into the Stream.**

- **Immediate queue** — one Redis Stream per queue with a single consumer group. Workers `XREADGROUP` (blocking, batched via `COUNT` + pipelining), run the handler, then `XACK` + `XDEL`. Queues are selected by smooth weighted round-robin (`wrr.go`), or strictly by weight with `StrictPriority`.
- **Delayed / retry / archived / completed** — sorted sets scored by process-at / retry-at / died-at / expire-at. A forwarder (`internal/rdb/forward.go`) promotes due entries into the Stream.
- **Crash recovery** — a recoverer `XAUTOCLAIM`s Stream entries whose worker went silent and re-queues or dead-letters them; attempt counts live in the task HASH.
- **Scheduler** — a Redis leader election (`SET NX PX` lock + pub/sub resignation, `internal/rdb/leader.go`) ensures **one instance** runs the tick loop. Each trigger enqueues under a deterministic dedup key `<schedule>:<trigger-unix>`, so a split-brain hand-off cannot double-enqueue.
- **Heartbeat** — refreshes in-flight tasks' PEL idle (`XCLAIM ... JUSTID`) and unique-lock TTL, so long-running work is neither reclaimed nor loses its lock mid-processing.
- **Janitor** — trims dead-lettered and retained-completed tasks by age and count so Redis memory stays bounded.

### Key layers

- **`internal/base`** — the single source of truth for the Redis **key layout**, task states, and message serialization. Read `internal/base/keys.go` before touching anything Redis-facing: every key is wrapped in a `{queue}` **hash tag** (`chronos:{queue}:...`) so all of a queue's keys share one Redis Cluster slot and multi-key Lua scripts stay atomic. Global keys (`chronos:queues`, `chronos:leader`, `chronos:schedules`) intentionally have **no** hash tag and are only ever touched by single-key commands/scripts — that is what keeps them cluster-safe.
- **`internal/rdb`** — all Redis operations, mostly **Lua scripts** for atomicity (enqueue, forward, retry, recover, group/chain completion, leader, heartbeat, janitor, pause, unique lock). This is where the real logic lives.
- **Root package** — the public API surface: `chronos.go` (Client/Enqueue/`Task[T]`), `server.go`, `scheduler.go`, `handler.go` (Mux + `AddHandler`/`AddHandlerR`), `inspector.go`, `chain.go`, `group.go`, `schedule.go`, `retry.go`, `codec.go`, `metrics.go`, `wrr.go`.

### Workflows (chains & groups)

`NewChain` runs tasks in sequence; `NewGroup` fans out in parallel and fires `OnComplete` when all members finish. Results relay between steps: a handler registered with `AddHandlerR` exposes its result to the next chain link via `PrevResult[R]` and to a group callback via `GroupResults`. **Nesting is exactly one level** — a group member may be a chain (`AddChain`), and a chain may embed a parallel stage (`ThenGroup`), but no deeper. Group/chain completion is driven by atomic Lua that decrements a pending SET and fires the callback when it empties.

## Invariants to preserve

- **At-least-once delivery.** A task can run more than once (crash after finishing but before ack; recoverer reclaiming an idle task). Handler logic added anywhere must stay idempotent-friendly.
- **Cluster safety.** Any new key must either share its queue's `{queue}` hash tag (if a multi-key script touches it) or be accessed only by single-key commands. Adding a multi-key Lua script that crosses hash tags will break on Redis Cluster.
- **Public API stability (v1.0.0+).** The root package follows semver; breaking changes need a new major version. Mark removals with `// Deprecated:` for at least one minor release first. `internal/` and `contrib/` are exempt.
- **Redis 6.2+** (uses `XAUTOCLAIM`), **Go 1.26+** (two most recent stable releases supported).

## Conventions

- Branch from `main`; **never commit to `main` directly**. New features generally follow spec → plan → implementation (see `docs/superpowers/`).
- **Write all Git/GitHub artifacts in English.** Commit messages, PR titles/bodies, and release notes must be in English (this is a public open-source project). Conversational replies may still be in Korean.
- Test connection helper is `internal/testutil.NewRedis(t)` (standalone) / `NewClusterRedis(t)` (skips unless `REDIS_CLUSTER_ADDRS` is set).
- Prefer a single atomic Lua script over multiple round-trips when an operation must be all-or-nothing across keys.
