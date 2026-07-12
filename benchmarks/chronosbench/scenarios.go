// Package chronosbench implements the benchmark scenarios against chronos-go's
// public API (no internal tuning — fairness).
package chronosbench

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
	"github.com/kenshin579/chronos-go/benchmarks/bench"
)

// BenchArgs is the benchmark payload: the enqueue timestamp for latency, plus
// padding to reach the configured payload size.
type BenchArgs struct {
	TS  int64  `json:"ts"` // enqueue time, unix nanos (0 = don't measure)
	Pad string `json:"pad"`
}

func (BenchArgs) Kind() string { return "bench:task" }

// pad returns filler so the marshalled payload is roughly size bytes.
func pad(size int) string {
	const overhead = 40 // {"ts":...,"pad":""} approximation
	if size <= overhead {
		return ""
	}
	return strings.Repeat("x", size-overhead)
}

func newRedis(cfg bench.Config) redis.UniversalClient {
	return redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
}

// --- enqueue: client-side throughput, no server ---

type enqueueScenario struct{}

// Enqueue measures pure client-side enqueue throughput.
func Enqueue() bench.Scenario { return enqueueScenario{} }

func (enqueueScenario) Name() string   { return "enqueue" }
func (enqueueScenario) Target() string { return "chronos" }

func (enqueueScenario) Run(ctx context.Context, cfg bench.Config) (bench.Result, error) {
	rdb := newRedis(cfg)
	defer rdb.Close()
	client := chronos.NewClient(rdb)
	defer client.Close()

	per := cfg.Tasks / cfg.Producers
	var wg sync.WaitGroup
	var firstErr atomic.Value
	start := time.Now()
	for p := 0; p < cfg.Producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := BenchArgs{Pad: pad(cfg.PayloadSize)}
			for i := 0; i < per; i++ {
				if _, err := chronos.Enqueue(ctx, client, args); err != nil {
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

// --- e2e: producers + server, latency from payload timestamp ---

type e2eScenario struct{}

// E2E measures sustained produce+consume throughput and end-to-end latency
// (enqueue call to handler start), warmup 10% excluded.
func E2E() bench.Scenario { return e2eScenario{} }

func (e2eScenario) Name() string   { return "e2e" }
func (e2eScenario) Target() string { return "chronos" }

func (e2eScenario) Run(ctx context.Context, cfg bench.Config) (bench.Result, error) {
	rdb := newRedis(cfg)
	defer rdb.Close()
	client := chronos.NewClient(rdb)
	defer client.Close()

	latCh := make(chan time.Duration, cfg.Tasks)
	var done atomic.Int64
	mux := chronos.NewMux()
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[BenchArgs]) error {
		if t.Args.TS > 0 {
			latCh <- time.Since(time.Unix(0, t.Args.TS))
		}
		done.Add(1)
		return nil
	})
	srv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: cfg.Concurrency,
	})
	if err := srv.Start(ctx, mux); err != nil {
		return bench.Result{}, err
	}
	defer srv.Shutdown(context.Background())

	per := cfg.Tasks / cfg.Producers
	total := per * cfg.Producers
	warmup := total / 10 // first 10% excluded from latency stats (TS=0)
	var enqueued atomic.Int64
	var wg sync.WaitGroup
	var firstErr atomic.Value
	start := time.Now()
	for p := 0; p < cfg.Producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			padding := pad(cfg.PayloadSize)
			for i := 0; i < per; i++ {
				args := BenchArgs{Pad: padding}
				if enqueued.Add(1) > int64(warmup) {
					args.TS = time.Now().UnixNano()
				}
				if _, err := chronos.Enqueue(ctx, client, args); err != nil {
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

// --- chain: K chains of length L; whole-chain latency from creation ts ---

// ChainTailArgs is a chain link payload; TS is the CHAIN's creation time so the
// last link measures whole-chain latency.
type ChainTailArgs struct {
	TS   int64  `json:"ts"`
	Last bool   `json:"last"`
	Pad  string `json:"pad"`
}

func (ChainTailArgs) Kind() string { return "bench:chainlink" }

type chainScenario struct{ length int }

// Chain measures K chains of `length` links completing end to end.
func Chain(length int) bench.Scenario { return chainScenario{length: length} }

func (chainScenario) Name() string   { return "chain" }
func (chainScenario) Target() string { return "chronos" }

func (s chainScenario) Run(ctx context.Context, cfg bench.Config) (bench.Result, error) {
	rdb := newRedis(cfg)
	defer rdb.Close()
	client := chronos.NewClient(rdb)
	defer client.Close()

	chains := cfg.Tasks / s.length
	if chains == 0 {
		chains = 1
	}
	latCh := make(chan time.Duration, chains)
	var done atomic.Int64
	mux := chronos.NewMux()
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[ChainTailArgs]) error {
		if t.Args.Last {
			latCh <- time.Since(time.Unix(0, t.Args.TS))
		}
		done.Add(1)
		return nil
	})
	srv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: cfg.Concurrency,
	})
	if err := srv.Start(ctx, mux); err != nil {
		return bench.Result{}, err
	}
	defer srv.Shutdown(context.Background())

	padding := pad(cfg.PayloadSize)
	start := time.Now()
	for c := 0; c < chains; c++ {
		ch := chronos.NewChain()
		ts := time.Now().UnixNano()
		for l := 0; l < s.length; l++ {
			ch.Then(ChainTailArgs{TS: ts, Last: l == s.length-1, Pad: padding})
		}
		if _, err := ch.Enqueue(ctx, client); err != nil {
			return bench.Result{}, err
		}
	}
	total := int64(chains * s.length)
	for done.Load() < total {
		select {
		case <-ctx.Done():
			return bench.Result{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	elapsed := time.Since(start)
	close(latCh)
	lats := make([]time.Duration, 0, chains)
	for d := range latCh {
		lats = append(lats, d)
	}
	p50, p95, _, _ := bench.Percentiles(lats)
	return bench.Result{
		Elapsed:    elapsed,
		Throughput: float64(total) / elapsed.Seconds(),
		Extra: map[string]float64{
			"chains":           float64(chains),
			"chain_len":        float64(s.length),
			"chain_e2e_p50_ms": float64(p50.Microseconds()) / 1000,
			"chain_e2e_p95_ms": float64(p95.Microseconds()) / 1000,
			"per_hop_ms":       float64(p50.Microseconds()) / 1000 / float64(s.length),
		},
	}, nil
}

// --- group: K groups of N members; group e2e = creation → callback ---

// GroupCbArgs is the group callback payload carrying the group creation ts.
type GroupCbArgs struct {
	TS  int64  `json:"ts"`
	Pad string `json:"pad"`
}

func (GroupCbArgs) Kind() string { return "bench:groupcb" }

type groupScenario struct{ size int }

// Group measures K groups of `size` members completing through their callback.
func Group(size int) bench.Scenario { return groupScenario{size: size} }

func (groupScenario) Name() string   { return "group" }
func (groupScenario) Target() string { return "chronos" }

func (s groupScenario) Run(ctx context.Context, cfg bench.Config) (bench.Result, error) {
	rdb := newRedis(cfg)
	defer rdb.Close()
	client := chronos.NewClient(rdb)
	defer client.Close()

	groups := cfg.Tasks / s.size
	if groups == 0 {
		groups = 1
	}
	latCh := make(chan time.Duration, groups)
	var done atomic.Int64
	mux := chronos.NewMux()
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[BenchArgs]) error {
		done.Add(1) // member
		return nil
	})
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[GroupCbArgs]) error {
		latCh <- time.Since(time.Unix(0, t.Args.TS))
		done.Add(1) // callback
		return nil
	})
	srv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: cfg.Concurrency,
	})
	if err := srv.Start(ctx, mux); err != nil {
		return bench.Result{}, err
	}
	defer srv.Shutdown(context.Background())

	padding := pad(cfg.PayloadSize)
	start := time.Now()
	for gi := 0; gi < groups; gi++ {
		g := chronos.NewGroup()
		for m := 0; m < s.size; m++ {
			g.Add(BenchArgs{Pad: padding})
		}
		if _, err := g.OnComplete(GroupCbArgs{TS: time.Now().UnixNano(), Pad: padding}).
			Enqueue(ctx, client); err != nil {
			return bench.Result{}, err
		}
	}
	total := int64(groups * (s.size + 1)) // members + callbacks
	for done.Load() < total {
		select {
		case <-ctx.Done():
			return bench.Result{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	elapsed := time.Since(start)
	close(latCh)
	lats := make([]time.Duration, 0, groups)
	for d := range latCh {
		lats = append(lats, d)
	}
	p50, p95, _, _ := bench.Percentiles(lats)
	return bench.Result{
		Elapsed:    elapsed,
		Throughput: float64(total) / elapsed.Seconds(),
		Extra: map[string]float64{
			"groups":           float64(groups),
			"group_size":       float64(s.size),
			"group_e2e_p50_ms": float64(p50.Microseconds()) / 1000,
			"group_e2e_p95_ms": float64(p95.Microseconds()) / 1000,
		},
	}, nil
}
