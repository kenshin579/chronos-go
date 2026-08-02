package soak

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testRedis returns a flushed DB 15 client, skipping if Redis is unreachable.
func testRedis(t *testing.T) (*redis.Client, context.Context) {
	t.Helper()
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
	return rdb, ctx
}

// seed writes one key per family the sampler counts, under prefix.
func seed(ctx context.Context, rdb *redis.Client, prefix string) {
	a, b := prefix+":{soak-a}:", prefix+":{soak-b}:"
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: a + "stream", Values: map[string]any{"task_id": "t1"}})
	rdb.ZAdd(ctx, a+"retry", redis.Z{Score: 1, Member: "t2"})
	rdb.ZAdd(ctx, b+"scheduled", redis.Z{Score: 1, Member: "t3"})
	rdb.ZAdd(ctx, a+"archived", redis.Z{Score: 1, Member: "t4"})
	rdb.ZAdd(ctx, b+"completed", redis.Z{Score: 1, Member: "t5"})
	rdb.Set(ctx, a+"unique:soak:task:abc", "t6", 0)
	rdb.SAdd(ctx, a+"group:g1", "m1")
	rdb.HSet(ctx, prefix+":schedules", "soak:sched:@every 1s", "{}")
}

func TestSamplerCollect(t *testing.T) {
	rdb, ctx := testRedis(t)

	// 패밀리별 키를 심는다 (soak가 세는 모든 패턴).
	seed(ctx, rdb, DefaultPrefix)

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

// TestSamplerPrefix pins the prefix down in both directions. A sampler reading
// the wrong prefix reports zeros rather than an error — the worst failure mode
// for a leak detector, since a soak run would look perfectly clean while the
// keys pile up under the namespace nobody is watching.
func TestSamplerPrefix(t *testing.T) {
	rdb, ctx := testRedis(t)
	seed(ctx, rdb, "otherapp")

	var done atomic.Int64
	queues := []string{"soak-a", "soak-b"}

	// Matching prefix: every family is found.
	match, err := NewSamplerWithPrefix(rdb, queues, &done, "otherapp").Collect(ctx)
	if err != nil {
		t.Fatalf("collect matching prefix: %v", err)
	}
	if match.Stream != 1 || match.Retry != 1 || match.Scheduled != 1 || match.Archived != 1 ||
		match.Completed != 1 || match.Unique != 1 || match.Groups != 1 || match.Schedules != 1 {
		t.Errorf("matching prefix counted %+v, want 1 per family", match)
	}

	// Default prefix against otherapp's keys: nothing belongs to us.
	miss, err := NewSampler(rdb, queues, &done).Collect(ctx)
	if err != nil {
		t.Fatalf("collect default prefix: %v", err)
	}
	if miss.Stream != 0 || miss.Retry != 0 || miss.Scheduled != 0 || miss.Archived != 0 ||
		miss.Completed != 0 || miss.Unique != 0 || miss.Groups != 0 || miss.Schedules != 0 {
		t.Errorf("default prefix counted %+v of another namespace's keys, want all zero", miss)
	}
	if miss.DBSize == 0 {
		t.Error("DBSize is 0, so the seed never landed and the zeros above prove nothing")
	}
}
