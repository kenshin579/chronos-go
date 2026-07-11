.PHONY: test test-race vet fmt fmt-check check test-contrib test-cluster

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

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

check: fmt-check vet test-race test-contrib
