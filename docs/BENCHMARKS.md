# Benchmarks

Throughput and end-to-end latency for chronos-go, with an apples-to-apples
comparison against [asynq](https://github.com/hibiken/asynq). Reproduce with
`make bench` (see [benchmarks/](../benchmarks/)).

## Methodology

- **Fairness**: both libraries run with their **default configs**; only the
  shared knobs are set identically — concurrency, payload (100-byte JSON),
  task count (20,000), producers (4). Same machine, same Redis, runs
  interleaved. Latency is measured the same way on both sides: the enqueue
  timestamp is embedded in the payload and the delta taken at handler start;
  the first 10% of tasks are excluded as warmup.
- **Repeats**: every number is the **median-throughput run of 3** (the Redis DB
  is flushed between runs).
- **Model**: producers enqueue as fast as they can while the server consumes —
  a *saturation* benchmark. Throughput is the meaningful headline number.
  Latency percentiles under saturation include backlog wait; when consumption
  keeps up with production (high concurrency), they approach true per-task
  latency.
- **Local Redis** (not Docker) to avoid container networking variance.

Measured 2026-07-12 on: Apple M4 Pro (12 cores), Redis 8.0.3, Go 1.26.4,
chronos-go @ this commit, asynq v0.26.0.

## Enqueue throughput

Pure client-side enqueue, 4 producers, no server.

| library | tasks/s |
|---|---|
| chronos-go | 25,872 |
| asynq | 27,710 |

## End-to-end throughput (produce + consume)

20k tasks, 4 producers, worker concurrency C.

| C | chronos-go | asynq | ratio |
|---|---|---|---|
| 1 | 2,605 | 3,939 | 0.66x |
| 4 | 5,370 | 5,321 | 1.01x |
| 16 | **15,257** | 6,096 | **2.50x** |
| 64 | **19,859** | 4,908 | **4.05x** |

chronos-go scales with concurrency (batched `XREADGROUP COUNT=k` + pipelined
body loads amortize Redis round trips across free workers); asynq peaks around
C=16 and declines. At C=64 chronos-go's consumers keep up with the producers,
so tasks see **p50 ≈ 1ms** with no backlog; at lower concurrency the
percentiles are backlog-dominated (see Methodology).

| C | chronos p50 / p95 | asynq p50 / p95 |
|---|---|---|
| 1 | 3,910ms / 6,552ms | 2,874ms / 4,146ms |
| 4 | 1,675ms / 2,737ms | 2,191ms / 2,877ms |
| 16 | 268ms / 354ms | 1,286ms / 2,128ms |
| 64 | **1ms / 1ms** | 2,481ms / 3,234ms |

**Honest note**: at C=1 asynq is ~1.5x faster — with a batch of one, chronos-go
still pays 3 round trips per task (read, load body, mark active) where asynq
uses a single script. Low-concurrency single-worker deployments are not
chronos-go's sweet spot; C≥4 is.

## Workflows (no asynq equivalent)

C=16, 20k tasks total.

| scenario | tasks/s | notes |
|---|---|---|
| chain (2,000 × 10 links) | 13,489 | whole-chain p50 1.1s under saturation (all 2,000 chains progress in waves) |
| group (2,000 × 10 members) | 6,664 | callback fires **p50 3ms** after the last member |

## What these numbers found (and fixed)

This benchmark exists to find bottlenecks, and its first run did:

1. **Enqueue did 2 round trips per task** (a global queue-index `SADD` on every
   call). Now cached per queue per process → 14.2k → 25.9k tasks/s.
2. **The fetch path did 3 round trips per task from a single loop**, capping
   consumption at ~2.9k tasks/s regardless of concurrency. Now one
   `XREADGROUP COUNT=k` plus two pipelines per batch → 15–20k tasks/s and real
   concurrency scaling.

## Caveats

- Single machine + local Redis: absolute numbers are optimistic versus a
  networked deployment; the *relative* comparisons are the point.
- No-op handlers isolate library overhead. Real handlers doing real work will
  dominate these costs long before the queue does.
- asynq configuration improvements are welcome — the harness is public and a
  PR away.
