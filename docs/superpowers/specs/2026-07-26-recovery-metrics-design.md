# Recovery metrics design

Date: 2026-07-26

## Background

`Metrics` is a single-method hook the server calls when a task reaches a terminal
outcome:

```go
type Metrics interface {
    ObserveTask(queue, kind string, outcome TaskOutcome, dur time.Duration)
}
```

`Server.observe` is reached from exactly three places in `process()` — success,
dead-letter, and retry — all after a handler has run.

## Problem

**`OutcomeDeadLetter` undercounts.** When a worker crashes, its in-flight tasks
sit in the stream's PEL until `recovererLoop` reclaims them via `XAUTOCLAIM`.
Tasks whose retry budget is exhausted at that point are archived and
`OnDeadLetter` fires — but `observe` is never called.

So a metrics implementation counting `outcome="dead_letter"` is wrong precisely
when it matters most: during a worker crash storm. An operator alerting on that
counter sees nothing while tasks are being abandoned.

This was found while instrumenting a downstream consumer, which had to work
around it by reading the `archived` queue gauge instead of the counter.

## Why not just call `observe` from the recoverer

The obvious fix — `s.observe(msg, OutcomeDeadLetter, 0)` in `recovererLoop` — is
wrong, for a reason that is easy to miss.

`ObserveTask` bundles a duration, and implementations record it in a histogram.
For a recovered task there **is no handler duration**: the handler started on a
worker that died, and we have no idea how long it ran. Passing `0` would push a
zero-second observation into `chronos_task_duration_seconds` (or whatever the
implementation calls it) for every recovered task.

That histogram has no `outcome` label, so the false observations cannot be
filtered out downstream. The result is perverse: **the more workers crash, the
better latency looks.**

## Why not widen `Metrics`

`Metrics` is part of the frozen v1 public API (`docs/superpowers/v1-api-audit.md`).
Adding a method breaks every existing implementation at compile time.

## Design

Add a second, **optional** interface that the server discovers by type assertion.
Implementations opt in by adding the method; existing ones keep compiling and
behave exactly as before.

```go
// RecoveryMetrics is an optional companion to Metrics. A Metrics implementation
// that also implements it receives recoverer activity, which ObserveTask cannot
// express because a reclaimed task has no handler duration.
type RecoveryMetrics interface {
    ObserveRecovered(queue string, recovered, deadLettered int)
}
```

`recovererLoop` calls it once per queue per sweep with the counts that
`rdb.Recover` already returns and currently discards.

### Why a separate event rather than a completion

A reclaimed task is not a task that "completed with outcome dead_letter" — no
handler ever returned. Modelling it as a distinct event is more honest than
stretching `ObserveTask`, and it lets us surface `recovered` (tasks successfully
returned to the stream), which is a useful signal on its own and is currently
thrown away.

### Semantics

- `recovered` — tasks reclaimed from the PEL and made available again
- `deadLettered` — tasks that were reclaimed and immediately archived because
  their retry budget was already exhausted

Both are per-sweep deltas, not cumulative. Implementations should add them to
counters.

A sweep that reclaims nothing calls the hook with `(0, 0)`. This is deliberate:
it lets an implementation distinguish "the recoverer is running and finding
nothing" from "the recoverer is not running at all", which a lazily-created
counter cannot express.

## Documented boundary

`Metrics`'s doc comment gains an explicit statement that `OutcomeDeadLetter`
covers only the handler path, and points at `RecoveryMetrics` for the rest.
The undercount then becomes a documented boundary rather than a surprise.

## Non-goals

- Widening or otherwise changing `Metrics`
- Instrumenting the janitor, forwarder, or heartbeat loops — each discards
  counts the same way and deserves the same treatment, but they are separate
  changes with their own semantics
- Backfilling the duration for recovered tasks. It is not knowable

## Compatibility

Additive. A `Metrics` implementation that does not implement `RecoveryMetrics`
sees no behaviour change, and the type assertion costs one interface check per
sweep (default: every 15s per queue).
