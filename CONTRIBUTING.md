# Contributing

Thanks for helping improve chronos-go.

## Prerequisites

- Go (the two most recent stable releases)
- A local Redis for tests: `redis-server --port 6379` (or Docker)

## Running tests

```bash
make check          # fmt-check + vet + race tests + contrib + benchmarks build
go test ./... -p 1  # -p 1 is required: packages share Redis DB 15 and flush it,
                    # so parallel package execution clobbers state
```

## Redis Cluster (optional)

Cluster-safety smoke tests run against a disposable local cluster:

```bash
cd deploy/redis-cluster && docker compose up -d   # 6-node cluster
make test-cluster                                  # opt-in; skipped without the env
```

## Soak test (optional)

Long-running leak check (heap / goroutine / Redis keyspace trend):

```bash
make soak                                    # 1 hour
cd benchmarks && go run ./cmd/soak -duration 4h   # before a major release
```

## Pull requests

- Branch from `main`; never commit to `main` directly.
- Keep the public API stable (see the Stability section in the README). Breaking
  changes to the core package require a major version and prior discussion.
- Include tests; `make check` must be green.
- New features generally follow spec → plan → implementation (see
  `docs/superpowers/`).
