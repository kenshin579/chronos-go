// Package testutil provides shared test helpers for chronos-go.
package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestDB is the Redis logical database dedicated to tests.
const TestDB = 15

// NewRedis connects to a test Redis instance and returns a client whose
// database is flushed before the test and cleaned up afterwards. If no Redis
// is reachable the test is skipped rather than failed.
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
