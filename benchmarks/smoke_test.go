package benchmarks

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/benchmarks/bench"
	"github.com/kenshin579/chronos-go/benchmarks/chronosbench"
)

func benchConfig(t *testing.T) bench.Config {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	_ = c.Close()
	return bench.Config{
		RedisAddr: addr, RedisDB: 15,
		Tasks: 200, Concurrency: 4, Producers: 2, PayloadSize: 100,
	}
}

func TestSmoke_ChronosEnqueue(t *testing.T) {
	cfg := benchConfig(t)
	r, err := bench.Run(context.Background(), chronosbench.Enqueue(), cfg, 1)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if r.Throughput <= 0 {
		t.Errorf("throughput = %v, want > 0", r.Throughput)
	}
}

func TestSmoke_ChronosE2E(t *testing.T) {
	cfg := benchConfig(t)
	r, err := bench.Run(context.Background(), chronosbench.E2E(), cfg, 1)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if r.Throughput <= 0 || r.P50 <= 0 || r.P99 < r.P50 {
		t.Errorf("suspicious stats: %+v", r)
	}
}
