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
  the server's metrics hook.
- **`QueueCollector`** — a `prometheus.Collector` over a `chronos.Inspector`
  that reports `chronos_queue_tasks` per queue and state
  (pending/active/scheduled/retry/archived), read live at scrape time. Register
  it with `NewQueueCollector(insp)`.

A ready-to-run Prometheus + Grafana stack lives in [`deploy/`](deploy).
