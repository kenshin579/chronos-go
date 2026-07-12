# chronos-go web console

A browser-based **task-management console** for chronos-go, backed entirely by
the public `chronos.Inspector`. It complements Grafana (metrics) and the CLI:

- **Card dashboard** — per-queue cards with live counts (5s auto-refresh),
  in-memory sparklines, and red highlighting for queues with dead-letters.
- **Workflow visualization** — a chain stepper (`✓ → ✗ → waiting`) and group
  member grid on the task page; re-running a dead-letter resumes the flow.
- **Bulk actions** — re-run or delete every archived task in a queue.
- **Search** — jump to a task by ID across all queues.
- **Queue pause/resume** — stop consuming a queue with one click (work
  accumulates as pending; takes effect within ~1s).
- **Scheduler status** — current leader and the full schedule registry
  (including registered-but-never-fired schedules; stale entries greyed).
- **Dark mode** — follows your system, with a manual toggle.
- **Cluster-aware** — connects to standalone Redis or a Redis Cluster.

## Run

```bash
go run ./cmd/webui --db 0                                  # standalone (default)
go run ./cmd/webui --cluster --redis n1:7000,n2:7001       # Redis Cluster
```

Flags: `--addr` (default `127.0.0.1:8080`), `--standalone`/`--cluster`
(mutually exclusive; default standalone), `--redis` (comma-separated seeds for
`--cluster`), `--db` (standalone only — Redis Cluster has only database 0),
`--no-open` (skip opening a browser).

## Try it with sample data

```bash
# in the repo root, generate tasks (chains, groups, dead-letters) on DB 15
go run ./examples/tour
# then point the console at the same DB
cd contrib/webui && go run ./cmd/webui --db 15
```

Open the dashboard, click the red-highlighted queue, open a dead-lettered
chain link (🔗), and use **Re-run — resumes the chain**.

## Security

The console binds `127.0.0.1` by default and **ships no authentication**. Its
actions (re-run, delete, and the bulk variants) are destructive. To expose it
beyond localhost, set `--addr 0.0.0.0:8080` **and put it behind an
authenticating reverse proxy** (nginx, oauth2-proxy, Cloudflare Access, …).
Do not expose it directly. Destructive POSTs are Origin-checked and bulk
actions require a browser confirmation, but neither replaces authentication.

## Mounting in your own server

The console uses absolute URLs (`/static/…`, `/api/stats`, redirects), so it
must be mounted at the **root** of its host (a dedicated port or subdomain):

```go
srv := &http.Server{
	Addr:    "127.0.0.1:8080",
	Handler: webui.Handler(insp, webui.WithConnInfo("prod cluster")),
}
```

Path-prefix mounting (e.g. `/chronos/` via `StripPrefix`) is not supported yet.

## Notes

- Sparklines live in the console process's memory (a short pulse, not
  history) — for real time-series use [`contrib/prometheus`](../prometheus).
- The scheduler page lists schedules that have **fired at least once**;
  registered-but-never-fired schedules live only in scheduler process memory.
