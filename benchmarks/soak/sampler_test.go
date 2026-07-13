package soak

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestSamplerCollect(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	t.Cleanup(func() { rdb.FlushDB(ctx); rdb.Close() })
	rdb.FlushDB(ctx)

	// 패밀리별 키를 심는다 (soak가 세는 모든 패턴).
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: "chronos:{soak-a}:stream", Values: map[string]any{"task_id": "t1"}})
	rdb.ZAdd(ctx, "chronos:{soak-a}:retry", redis.Z{Score: 1, Member: "t2"})
	rdb.ZAdd(ctx, "chronos:{soak-b}:scheduled", redis.Z{Score: 1, Member: "t3"})
	rdb.ZAdd(ctx, "chronos:{soak-a}:archived", redis.Z{Score: 1, Member: "t4"})
	rdb.ZAdd(ctx, "chronos:{soak-b}:completed", redis.Z{Score: 1, Member: "t5"})
	rdb.Set(ctx, "chronos:{soak-a}:unique:soak:task:abc", "t6", 0)
	rdb.SAdd(ctx, "chronos:{soak-a}:group:g1", "m1")
	rdb.HSet(ctx, "chronos:schedules", "soak:sched:@every 1s", "{}")

	var done atomic.Int64
	done.Store(100)
	s := NewSampler(rdb, []string{"soak-a", "soak-b"}, &done)
	// 처리량 측정 기준점을 과거로 밀어 0-division/0-tput을 피한다.
	s.prevAt = time.Now().Add(-10 * time.Second)

	got, err := s.Collect(ctx)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if got.Stream != 1 || got.Retry != 1 || got.Scheduled != 1 || got.Archived != 1 ||
		got.Completed != 1 || got.Unique != 1 || got.Groups != 1 || got.Schedules != 1 {
		t.Errorf("family counts wrong: %+v", got)
	}
	if got.DBSize == 0 || got.HeapBytes == 0 || got.Goroutines == 0 {
		t.Errorf("process/db stats empty: %+v", got)
	}
	if got.Throughput < 5 || got.Throughput > 20 { // 100 done / ~10s
		t.Errorf("throughput %v, want ~10", got.Throughput)
	}

	// 두 번째 수집: 추가 완료 없음 → 처리량 0.
	got2, err := s.Collect(ctx)
	if err != nil {
		t.Fatalf("collect2: %v", err)
	}
	if got2.Throughput != 0 {
		t.Errorf("second throughput %v, want 0", got2.Throughput)
	}
}
