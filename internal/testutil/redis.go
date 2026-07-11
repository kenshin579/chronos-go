// Package testutil provides shared test helpers for chronos-go.
package testutil

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestDB is the Redis logical database dedicated to tests.
const TestDB = 15

// NewRedis connects to a test Redis instance and returns a client whose
// database is flushed before the test and cleaned up afterwards. If no Redis
// is reachable the test is skipped rather than failed.
//
// All packages share a single logical database (TestDB) and flush it, so their
// test binaries must not run concurrently against it. Run the suite with
// `go test ./... -p 1` (see the Makefile's `test` target) so package binaries
// execute sequentially; the default per-package parallelism would let one
// package's FlushDB wipe another's data mid-run.
func NewRedis(t *testing.T) redis.UniversalClient {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr, DB: TestDB})

	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("redis not available at %s: %v", addr, err)
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush test db: %v", err)
	}

	t.Cleanup(func() {
		_ = client.FlushDB(ctx).Err()
		_ = client.Close()
	})

	return client
}

// NewClusterRedis connects to a disposable test Redis Cluster listed in
// REDIS_CLUSTER_ADDRS (comma-separated seed addresses, e.g. the cluster from
// deploy/redis-cluster). It skips the test when the variable is unset and
// fails it when the variable is set but the cluster is unreachable, flushes
// every master before the test, and cleans up
// afterwards. Unlike NewRedis there is no DB selection: Redis Cluster has only
// logical database 0, so the cluster must be dedicated to tests.
func NewClusterRedis(t *testing.T) redis.UniversalClient {
	t.Helper()

	addrs := os.Getenv("REDIS_CLUSTER_ADDRS")
	if addrs == "" {
		t.Skip("REDIS_CLUSTER_ADDRS not set; skipping cluster integration test")
	}

	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: splitAddrs(addrs)})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		// The env var is an explicit statement that a cluster should be there
		// (e.g. make test-cluster): failing loudly beats a false-green skip.
		t.Fatalf("REDIS_CLUSTER_ADDRS=%s set but cluster unreachable: %v", addrs, err)
	}

	flush := func() error {
		return client.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
			return m.FlushAll(ctx).Err()
		})
	}
	if err := flush(); err != nil {
		t.Fatalf("flush cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = flush()
		_ = client.Close()
	})
	return client
}

// splitAddrs splits a comma-separated address list, trimming whitespace and
// dropping empty entries.
func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			addrs = append(addrs, p)
		}
	}
	return addrs
}
