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
