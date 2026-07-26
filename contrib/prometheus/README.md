# chronos-go Prometheus metrics

> **Experimental.** This contrib module is versioned separately and is **not**
> covered by the core library's v1 stability guarantee. Its API and behavior
> may change between releases.

A Prometheus implementation of chronos-go's core `Metrics` hook, plus a
collector for live queue-depth gauges. It lives in a separate module
(`github.com/kenshin579/chronos-go/contrib/prometheus`) so the core stays free
of the Prometheus dependency.

- **`Metrics`** — implements `chronos.Metrics`, exporting
  `chronos_tasks_processed_total` (by queue, kind, outcome) and
  `chronos_task_duration_seconds`. Wire it via `NewMetrics(reg)` and pass it as
  the server's metrics hook. It also implements the optional
  `chronos.RecoveryMetrics`, exporting `chronos_tasks_recovered_total` (by queue
  and outcome) — no extra wiring needed, the server discovers it by type
  assertion.
- **`QueueCollector`** — a `prometheus.Collector` over a `chronos.Inspector`
  that reports `chronos_queue_tasks` per queue and state
  (pending/active/scheduled/retry/archived), read live at scrape time. Register
  it with `NewQueueCollector(insp)`.

## Recovery metrics

`chronos_tasks_recovered_total{queue, outcome}` counts tasks the recoverer
reclaimed from a crashed worker's pending list:

| `outcome` | meaning |
|---|---|
| `requeued` | reclaimed and made available for another attempt |
| `dead_lettered` | reclaimed with the retry budget already exhausted, so archived without ever reaching a handler |

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
`chronos_tasks_recovered_total{outcome="dead_lettered"}`. Alert on both to cover
every abandoned task.

A ready-to-run Prometheus + Grafana stack lives in [`deploy/`](deploy).
