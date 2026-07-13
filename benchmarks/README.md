# chronos-go benchmarks

Throughput and end-to-end latency measurements for chronos-go, with an
apples-to-apples comparison against [asynq](https://github.com/hibiken/asynq).
Methodology and current numbers: [docs/BENCHMARKS.md](../docs/BENCHMARKS.md).

## Run

Requires a local Redis; **DB 15 is flushed** between runs.

```bash
# one scenario
go run ./cmd/bench -target chronos -scenario e2e -tasks 20000 -concurrency 16

# the full matrix (from the repo root)
make bench
```

Flags: `-target chronos|asynq`, `-scenario enqueue|e2e|chain|group`, `-tasks`,
`-concurrency`, `-producers`, `-payload`, `-chainlen`, `-groupsize`, `-repeats`
(median reported), `-redis`, `-db`, `-json`.

## Fairness

Both libraries run with their **default configs**; only shared knobs
(concurrency, payload size, task count) are set, with identical payload schema
and latency measurement (timestamp embedded at enqueue, delta taken in the
handler, first 10% excluded as warmup). Numbers vary by machine — run it
yourself, and asynq configuration improvements are welcome.

## Soak test (leak detection)

`cmd/soak` runs a mixed workload — plain/failing/discarded tasks, delayed
tasks, unique dedup, retention, chains, groups, an interval schedule and
periodic queue pause/resume — for hours against a local Redis, sampling
heap (after GC), goroutine count, `DBSIZE` and per-family key counts every
30s (stdout + JSONL). chronos operational logs go to `-serverlog` (default
`soak-server.log`) so deliberate failures don't drown the sample lines.

```bash
make soak          # 1 hour (from the repo root)
make soak-quick    # 3 minutes — wiring check only, verdict not trustworthy
cd benchmarks && go run ./cmd/soak -duration 4h   # before a major release
```

The verdict trims the first 10% of samples as warmup, then compares
first-half vs second-half means: heap may grow at most 1.2x, goroutines at
most +10, `DBSIZE` at most 1.1x. Any violation on a run of 30 minutes or
longer exits 1. Shorter runs always exit 0 (informational).
