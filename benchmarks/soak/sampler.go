package soak

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Sampler collects one Sample per call. Key patterns are the stable layout
// documented in docs/OBSERVING.md — the soak intentionally observes Redis
// from the outside, like an operator would.
type Sampler struct {
	rdb       redis.UniversalClient
	queues    []string
	completed *atomic.Int64 // shared with the workload's handlers

	start    time.Time
	prevDone int64
	prevAt   time.Time
}

func NewSampler(rdb redis.UniversalClient, queues []string, completed *atomic.Int64) *Sampler {
	now := time.Now()
	return &Sampler{rdb: rdb, queues: queues, completed: completed, start: now, prevAt: now}
}

// Collect gathers process and Redis stats. It forces a GC first so HeapAlloc
// is comparable across samples.
func (s *Sampler) Collect(ctx context.Context) (Sample, error) {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	now := time.Now()
	done := s.completed.Load()
	tput := 0.0
	if dt := now.Sub(s.prevAt).Seconds(); dt > 0 {
		tput = float64(done-s.prevDone) / dt
	}

	out := Sample{
		ElapsedSec: now.Sub(s.start).Seconds(),
		HeapBytes:  ms.HeapAlloc,
		Goroutines: runtime.NumGoroutine(),
		Throughput: tput,
	}

	var err error
	if out.DBSize, err = s.rdb.DBSize(ctx).Result(); err != nil {
		return out, fmt.Errorf("dbsize: %w", err)
	}
	for _, q := range s.queues {
		p := "chronos:{" + q + "}:"
		n, err := s.rdb.XLen(ctx, p+"stream").Result()
		if err != nil {
			return out, fmt.Errorf("xlen %s: %w", q, err)
		}
		out.Stream += n
		for _, fam := range []struct {
			key string
			dst *int64
		}{
			{p + "retry", &out.Retry}, {p + "scheduled", &out.Scheduled},
			{p + "archived", &out.Archived}, {p + "completed", &out.Completed},
		} {
			n, err := s.rdb.ZCard(ctx, fam.key).Result()
			if err != nil {
				return out, fmt.Errorf("zcard %s: %w", fam.key, err)
			}
			*fam.dst += n
		}
	}
	if out.Unique, err = scanCount(ctx, s.rdb, "chronos:*:unique:*"); err != nil {
		return out, err
	}
	if out.Groups, err = scanCount(ctx, s.rdb, "chronos:*:group:*"); err != nil {
		return out, err
	}
	if out.Schedules, err = s.rdb.HLen(ctx, "chronos:schedules").Result(); err != nil {
		return out, fmt.Errorf("hlen schedules: %w", err)
	}
	s.prevDone, s.prevAt = done, now
	return out, nil
}

// scanCount returns the number of keys matching pattern.
func scanCount(ctx context.Context, rdb redis.UniversalClient, pattern string) (int64, error) {
	var count int64
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return count, fmt.Errorf("scan %s: %w", pattern, err)
		}
		count += int64(len(keys))
		if next == 0 {
			return count, nil
		}
		cursor = next
	}
}
