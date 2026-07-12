// Package asynqbench implements the comparison scenarios against hibiken/asynq
// using its default Config — fairness: both libraries run with defaults, only
// the shared knobs (concurrency, payload, task count) are set.
package asynqbench

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"

	"github.com/kenshin579/chronos-go/benchmarks/bench"
)

type benchPayload struct {
	TS  int64  `json:"ts"`
	Pad string `json:"pad"`
}

func pad(size int) string {
	const overhead = 40
	if size <= overhead {
		return ""
	}
	return strings.Repeat("x", size-overhead)
}

func opt(cfg bench.Config) asynq.RedisClientOpt {
	return asynq.RedisClientOpt{Addr: cfg.RedisAddr, DB: cfg.RedisDB}
}

// --- enqueue ---

type enqueueScenario struct{}

// Enqueue measures pure client-side enqueue throughput.
func Enqueue() bench.Scenario { return enqueueScenario{} }

func (enqueueScenario) Name() string   { return "enqueue" }
func (enqueueScenario) Target() string { return "asynq" }

func (enqueueScenario) Run(ctx context.Context, cfg bench.Config) (bench.Result, error) {
	client := asynq.NewClient(opt(cfg))
	defer client.Close()

	body, err := json.Marshal(benchPayload{Pad: pad(cfg.PayloadSize)})
	if err != nil {
		return bench.Result{}, err
	}
	per := cfg.Tasks / cfg.Producers
	var wg sync.WaitGroup
	var firstErr atomic.Value
	start := time.Now()
	for p := 0; p < cfg.Producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := client.EnqueueContext(ctx, asynq.NewTask("bench:task", body)); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if err, _ := firstErr.Load().(error); err != nil {
		return bench.Result{}, err
	}
	n := per * cfg.Producers
	return bench.Result{Elapsed: elapsed, Throughput: float64(n) / elapsed.Seconds()}, nil
}

// --- e2e ---

type e2eScenario struct{}

// E2E mirrors chronosbench.E2E: sustained produce+consume, latency from the
// embedded timestamp, warmup 10% excluded.
func E2E() bench.Scenario { return e2eScenario{} }

func (e2eScenario) Name() string   { return "e2e" }
func (e2eScenario) Target() string { return "asynq" }

func (e2eScenario) Run(ctx context.Context, cfg bench.Config) (bench.Result, error) {
	client := asynq.NewClient(opt(cfg))
	defer client.Close()

	latCh := make(chan time.Duration, cfg.Tasks)
	var done atomic.Int64
	mux := asynq.NewServeMux()
	mux.HandleFunc("bench:task", func(ctx context.Context, t *asynq.Task) error {
		var p benchPayload
		if err := json.Unmarshal(t.Payload(), &p); err == nil && p.TS > 0 {
			latCh <- time.Since(time.Unix(0, p.TS))
		}
		done.Add(1)
		return nil
	})
	srv := asynq.NewServer(opt(cfg), asynq.Config{Concurrency: cfg.Concurrency})
	if err := srv.Start(mux); err != nil {
		return bench.Result{}, err
	}
	defer srv.Shutdown()

	per := cfg.Tasks / cfg.Producers
	total := per * cfg.Producers
	warmup := total / 10
	var enqueued atomic.Int64
	var wg sync.WaitGroup
	var firstErr atomic.Value
	padding := pad(cfg.PayloadSize)
	start := time.Now()
	for p := 0; p < cfg.Producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				pl := benchPayload{Pad: padding}
				if enqueued.Add(1) > int64(warmup) {
					pl.TS = time.Now().UnixNano()
				}
				body, err := json.Marshal(pl)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				if _, err := client.EnqueueContext(ctx, asynq.NewTask("bench:task", body)); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err, _ := firstErr.Load().(error); err != nil {
		return bench.Result{}, err
	}
	for done.Load() < int64(total) {
		select {
		case <-ctx.Done():
			return bench.Result{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	elapsed := time.Since(start)
	close(latCh)
	lats := make([]time.Duration, 0, total)
	for d := range latCh {
		lats = append(lats, d)
	}
	p50, p95, p99, max := bench.Percentiles(lats)
	return bench.Result{
		Elapsed: elapsed, Throughput: float64(total) / elapsed.Seconds(),
		P50: p50, P95: p95, P99: p99, Max: max,
	}, nil
}
