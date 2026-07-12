# chronos-go web console

A browser-based **task-management console** for chronos-go, backed entirely by
the public `chronos.Inspector`. It complements Grafana (metrics) and the CLI:
open a dead-lettered task, read its payload and the error that killed it, then
re-run or delete it.

## Run

```bash
go run ./cmd/webui --db 0
```

Flags: `--addr` (default `127.0.0.1:8080`), `--redis` (default
`127.0.0.1:6379`), `--db` (default `0`), `--no-open` (skip opening a browser).

## Try it with sample data

```bash
# in the repo root, generate tasks (incl. dead-letters) on DB 15
go run ./examples/tour
# then point the console at the same DB
cd contrib/webui && go run ./cmd/webui --db 15
```

Open the dashboard, click a queue's **archived** count, open a task, and use
**Re-run now** / **Delete**.

## Security

The console binds `127.0.0.1` by default and **ships no authentication**. Its
actions (re-run, delete) are destructive. To expose it beyond localhost, set
`--addr 0.0.0.0:8080` **and put it behind an authenticating reverse proxy**
(nginx, oauth2-proxy, Cloudflare Access, …). Do not expose it directly.

## Mounting in your own server

```go
mux := http.NewServeMux()
mux.Handle("/chronos/", http.StripPrefix("/chronos", webui.Handler(insp)))
```
