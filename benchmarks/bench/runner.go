package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config is the common knob set every scenario receives.
type Config struct {
	RedisAddr   string
	RedisDB     int
	Tasks       int // total tasks to process (per run)
	Concurrency int // server worker concurrency
	Producers   int // concurrent enqueueing goroutines
	PayloadSize int // payload bytes (padding included)
}

// Scenario measures one workload shape against one target library.
type Scenario interface {
	Name() string
	Target() string
	Run(ctx context.Context, cfg Config) (Result, error)
}

// Run executes s `repeats` times (flushing the Redis DB before each run) and
// returns the median-throughput run. The flush keeps runs independent; the
// median keeps latency and throughput from the same execution.
func Run(ctx context.Context, s Scenario, cfg Config, repeats int) (Result, error) {
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return Result{}, fmt.Errorf("redis not reachable at %s: %w", cfg.RedisAddr, err)
	}

	results := make([]Result, 0, repeats)
	for i := 0; i < repeats; i++ {
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			return Result{}, err
		}
		r, err := s.Run(ctx, cfg)
		if err != nil {
			return Result{}, fmt.Errorf("run %d: %w", i+1, err)
		}
		r.Scenario, r.Target = s.Name(), s.Target()
		r.Tasks, r.Concurrency, r.Producers, r.PayloadSize =
			cfg.Tasks, cfg.Concurrency, cfg.Producers, cfg.PayloadSize
		results = append(results, r)
		time.Sleep(200 * time.Millisecond) // settle between runs
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil { // leave the DB clean
		return Result{}, err
	}
	return MedianByThroughput(results), nil
}
