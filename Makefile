.PHONY: test test-race vet fmt fmt-check check test-contrib test-cluster bench bench-build soak soak-quick

# Tests require a Redis reachable at $REDIS_ADDR (default 127.0.0.1:6379).
# -p 1 runs package test binaries sequentially: they share a single logical
# Redis DB and flush it, so parallel package execution would clobber each other.

test:
	go test ./... -p 1

test-race:
	go test ./... -race -p 1

test-contrib:
	cd contrib/prometheus && go test ./... -race

# Cluster integration tests. Requires the disposable cluster from
# deploy/redis-cluster (docker compose up -d). Skipped when the env is unset.
test-cluster:
	REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
		go test -run 'TestCluster_' -p 1 -race .

# Run the full benchmark matrix against a local Redis (DB 15 is FLUSHED).
# See docs/BENCHMARKS.md for methodology. Scaling: C in {1,4,16,64}.
bench:
	cd benchmarks && go run ./cmd/bench -target chronos -scenario enqueue
	cd benchmarks && go run ./cmd/bench -target asynq   -scenario enqueue
	cd benchmarks && for c in 1 4 16 64; do \
		go run ./cmd/bench -target chronos -scenario e2e -concurrency $$c; \
		go run ./cmd/bench -target asynq   -scenario e2e -concurrency $$c; \
	done
	cd benchmarks && go run ./cmd/bench -target chronos -scenario chain
	cd benchmarks && go run ./cmd/bench -target chronos -scenario group

bench-build:
	cd benchmarks && go build ./... && go vet ./... && go test ./soak/ -p 1

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

check: fmt-check vet test-race test-contrib bench-build

# Long-running leak soak against a local Redis (DB 15 is FLUSHED).
# Trustworthy verdicts need >=30m; run 4h before a major release.
soak:
	cd benchmarks && go run ./cmd/soak -duration 1h

# 3-minute wiring check — verdict is informational only (always exit 0).
soak-quick:
	cd benchmarks && go run ./cmd/soak -duration 3m
