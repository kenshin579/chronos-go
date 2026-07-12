# 성능 벤치마크 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `benchmarks/` 별도 모듈에 chronos-go(+asynq 비교) 성능 측정 하네스를 만들고, 측정 결과를 `docs/BENCHMARKS.md`·README에 싣는다.

**Architecture:** `bench`(러너·통계·리포터, Redis 무관 단위 테스트 가능) / `chronosbench`·`asynqbench`(시나리오 구현) / `cmd/bench`(CLI) 3층. 지연은 payload에 enqueue 시각(ns)을 내장해 핸들러에서 delta 수집. 각 시나리오 3회 실행 후 처리량 중앙값 실행을 보고. **Task 6·7은 측정 체크포인트 — 수치를 사용자에게 보고하고 해석·개선 논의 후 진행.**

**Tech Stack:** Go, redis/go-redis v9, hibiken/asynq(benchmarks 모듈에만), 로컬 standalone Redis.

---

## File Structure

```
benchmarks/
  go.mod                     replace ../ => 코어, require asynq
  bench/stats.go             Result, Percentiles, 중앙값 선택
  bench/stats_test.go        (Redis 불필요)
  bench/runner.go            Config, Scenario 인터페이스, Run(3회+flush+중앙값)
  bench/report.go            사람용 표 + JSONL
  chronosbench/scenarios.go  Enqueue/E2E/Chain/Group
  asynqbench/scenarios.go    Enqueue/E2E
  cmd/bench/main.go          CLI
  smoke_test.go              소량 실행 스모크 (Redis 없으면 skip)
  README.md
docs/BENCHMARKS.md           (Task 8에서 수치와 함께)
Makefile                     bench 타깃
README.md                    Performance 섹션 (Task 8)
```

**구현자 참고:** 코어 공개 API만 사용 — `chronos.NewClient/Enqueue/NewServer/NewMux/AddHandler/NewChain/NewGroup`. `contrib/prometheus/go.mod`가 별도 모듈 형식(replace) 선례. asynq API: `asynq.NewClient(asynq.RedisClientOpt{Addr, DB})`, `client.Enqueue(asynq.NewTask(kind, payload))`, `asynq.NewServer(opt, asynq.Config{Concurrency: c})`, `mux := asynq.NewServeMux(); mux.HandleFunc(kind, fn)`, `srv.Start(mux)`/`srv.Shutdown()`.

---

## Task 1: 모듈 스캐폴드 + bench 라이브러리 (통계·러너·리포터)

**Files:**
- Create: `benchmarks/go.mod`, `benchmarks/bench/stats.go`, `benchmarks/bench/runner.go`, `benchmarks/bench/report.go`
- Test: `benchmarks/bench/stats_test.go`

- [ ] **Step 1: go.mod 생성**

`benchmarks/go.mod` (asynq는 Task 4에서 추가):

```
module github.com/kenshin579/chronos-go/benchmarks

go 1.26

replace github.com/kenshin579/chronos-go => ../

require (
	github.com/kenshin579/chronos-go v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.21.0
)
```

- [ ] **Step 2: 실패 테스트 작성 (통계)**

`benchmarks/bench/stats_test.go`:

```go
package bench

import (
	"testing"
	"time"
)

func TestPercentiles(t *testing.T) {
	// 1..100ms — p50=50ms(중앙), p95=95ms, p99=99ms, max=100ms (최근접 순위법).
	lat := make([]time.Duration, 100)
	for i := range lat {
		lat[i] = time.Duration(i+1) * time.Millisecond
	}
	p50, p95, p99, max := Percentiles(lat)
	if p50 != 50*time.Millisecond || p95 != 95*time.Millisecond ||
		p99 != 99*time.Millisecond || max != 100*time.Millisecond {
		t.Errorf("got p50=%v p95=%v p99=%v max=%v", p50, p95, p99, max)
	}
}

func TestPercentiles_Empty(t *testing.T) {
	p50, _, _, max := Percentiles(nil)
	if p50 != 0 || max != 0 {
		t.Errorf("empty input should yield zeros, got p50=%v max=%v", p50, max)
	}
}

func TestMedianByThroughput(t *testing.T) {
	rs := []Result{{Throughput: 100}, {Throughput: 300}, {Throughput: 200}}
	if got := MedianByThroughput(rs); got.Throughput != 200 {
		t.Errorf("median = %v, want 200", got.Throughput)
	}
}
```

- [ ] **Step 3: 실패 확인**

Run: `cd benchmarks && go mod tidy && go test ./bench/`
Expected: FAIL — undefined Percentiles/Result/MedianByThroughput.

- [ ] **Step 4: 구현**

`benchmarks/bench/stats.go`:

```go
// Package bench is a small harness for measuring task-queue throughput and
// end-to-end latency: scenarios produce a Result, the runner repeats them and
// picks the median, and the reporter renders tables / JSONL.
package bench

import (
	"sort"
	"time"
)

// Result is one scenario execution's outcome.
type Result struct {
	Scenario    string             `json:"scenario"`
	Target      string             `json:"target"` // "chronos" | "asynq"
	Tasks       int                `json:"tasks"`
	Concurrency int                `json:"concurrency"`
	Producers   int                `json:"producers"`
	PayloadSize int                `json:"payload_size"`
	Elapsed     time.Duration      `json:"elapsed_ns"`
	Throughput  float64            `json:"tasks_per_sec"`
	P50         time.Duration      `json:"p50_ns"`
	P95         time.Duration      `json:"p95_ns"`
	P99         time.Duration      `json:"p99_ns"`
	Max         time.Duration      `json:"max_ns"`
	Extra       map[string]float64 `json:"extra,omitempty"` // scenario-specific metrics
}

// Percentiles returns p50/p95/p99/max using nearest-rank on a sorted copy.
// Zero values for empty input.
func Percentiles(latencies []time.Duration) (p50, p95, p99, max time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0, 0
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := func(p float64) time.Duration {
		idx := int(p*float64(len(sorted))+0.5) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	return rank(0.50), rank(0.95), rank(0.99), sorted[len(sorted)-1]
}

// MedianByThroughput returns the run whose throughput is the median — reporting
// a real run (not an average of runs) keeps latency and throughput consistent.
func MedianByThroughput(rs []Result) Result {
	sorted := make([]Result, len(rs))
	copy(sorted, rs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Throughput < sorted[j].Throughput })
	return sorted[len(sorted)/2]
}
```

`benchmarks/bench/runner.go`:

```go
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
```

`benchmarks/bench/report.go`:

```go
package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// PrintTable renders results as an aligned human-readable table.
func PrintTable(w io.Writer, rs []Result) {
	fmt.Fprintf(w, "%-10s %-8s %8s %6s %12s %10s %10s %10s %10s\n",
		"SCENARIO", "TARGET", "TASKS", "CONC", "TASKS/S", "P50", "P95", "P99", "MAX")
	for _, r := range rs {
		fmt.Fprintf(w, "%-10s %-8s %8d %6d %12.0f %10s %10s %10s %10s\n",
			r.Scenario, r.Target, r.Tasks, r.Concurrency, r.Throughput,
			fmtDur(r.P50), fmtDur(r.P95), fmtDur(r.P99), fmtDur(r.Max))
		for k, v := range r.Extra {
			fmt.Fprintf(w, "    %s = %.2f\n", k, v)
		}
	}
}

// PrintJSONL writes one JSON object per result (machine-readable).
func PrintJSONL(w io.Writer, rs []Result) error {
	enc := json.NewEncoder(w)
	for _, r := range rs {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	return d.Round(10 * time.Microsecond).String()
}
```

- [ ] **Step 5: 통과 확인**

Run: `cd benchmarks && go mod tidy && go test ./bench/ && go vet ./... && gofmt -l .`
Expected: PASS, clean.

- [ ] **Step 6: 커밋**

```bash
git add benchmarks/
git commit -m "feat: benchmarks 모듈 스캐폴드 + bench 라이브러리(통계/러너/리포터)"
```

---

## Task 2: chronosbench — enqueue·e2e 시나리오 + CLI + 스모크

**Files:**
- Create: `benchmarks/chronosbench/scenarios.go`, `benchmarks/cmd/bench/main.go`
- Test: `benchmarks/smoke_test.go`

- [ ] **Step 1: 실패 스모크 테스트 작성**

`benchmarks/smoke_test.go` (Redis 없으면 skip — testutil은 다른 모듈이므로 자체 skip):

```go
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
```

- [ ] **Step 2: 실패 확인**

Run: `cd benchmarks && go test . -run TestSmoke -p 1`
Expected: FAIL — undefined chronosbench.

- [ ] **Step 3: chronosbench 구현**

`benchmarks/chronosbench/scenarios.go`:

```go
// Package chronosbench implements the benchmark scenarios against chronos-go's
// public API (no internal tuning — fairness).
package chronosbench

import (
	"context"
	"fmt"
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

// pad returns filler so the marshalled payload is roughly cfg.PayloadSize bytes.
func pad(size int) string {
	const overhead = 40 // {"ts":...,"pad":""} 근사
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
	warmup := total / 10 // 앞 10%는 지연 통계 제외(TS=0)
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
	// 전량 처리 대기.
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

var _ = fmt.Sprintf // placeholder guard (fmt는 Chain/Group 시나리오(Task 3)에서 사용)
```

(Task 3에서 `fmt` 사용 시 위 placeholder guard 라인은 제거.)

- [ ] **Step 4: CLI 구현**

`benchmarks/cmd/bench/main.go`:

```go
// Command bench measures chronos-go (and asynq, for comparison) against a
// local Redis. See docs/BENCHMARKS.md for methodology.
//
//	go run ./cmd/bench -target chronos -scenario e2e -tasks 20000 -concurrency 16
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/kenshin579/chronos-go/benchmarks/bench"
	"github.com/kenshin579/chronos-go/benchmarks/chronosbench"
)

func main() {
	target := flag.String("target", "chronos", "chronos | asynq")
	scenario := flag.String("scenario", "e2e", "enqueue | e2e | chain | group")
	tasks := flag.Int("tasks", 20000, "total tasks per run")
	concurrency := flag.Int("concurrency", 16, "server worker concurrency")
	producers := flag.Int("producers", 4, "concurrent producers")
	payload := flag.Int("payload", 100, "payload size (bytes)")
	repeats := flag.Int("repeats", 3, "runs per scenario (median reported)")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	db := flag.Int("db", 15, "Redis DB (FLUSHED between runs — use a dedicated DB)")
	jsonOut := flag.Bool("json", false, "emit JSONL instead of a table")
	flag.Parse()

	cfg := bench.Config{
		RedisAddr: *redisAddr, RedisDB: *db,
		Tasks: *tasks, Concurrency: *concurrency,
		Producers: *producers, PayloadSize: *payload,
	}
	s, err := pick(*target, *scenario)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(2)
	}
	r, err := bench.Run(context.Background(), s, cfg, *repeats)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
	if *jsonOut {
		_ = bench.PrintJSONL(os.Stdout, []bench.Result{r})
	} else {
		bench.PrintTable(os.Stdout, []bench.Result{r})
	}
}

// pick maps target×scenario to an implementation. asynq scenarios are wired in
// a later task; until then they return an error.
func pick(target, scenario string) (bench.Scenario, error) {
	switch target {
	case "chronos":
		switch scenario {
		case "enqueue":
			return chronosbench.Enqueue(), nil
		case "e2e":
			return chronosbench.E2E(), nil
		}
	case "asynq":
		return nil, fmt.Errorf("asynq scenarios not wired yet")
	}
	return nil, fmt.Errorf("unknown target/scenario: %s/%s", target, scenario)
}
```

- [ ] **Step 5: 통과 확인**

Run: `cd benchmarks && go mod tidy && go test . -run TestSmoke -p 1 -v 2>&1 | tail -5`
Expected: 2개 PASS. `go build ./... && go vet ./... && gofmt -l .` clean.
CLI 동작: `go run ./cmd/bench -scenario e2e -tasks 500 -concurrency 4 -repeats 1` → 표 1행 출력.

- [ ] **Step 6: 커밋**

```bash
git add benchmarks/
git commit -m "feat: chronosbench enqueue/e2e 시나리오 + bench CLI + 스모크"
```

---

## Task 3: chronosbench — chain·group 시나리오

**Files:**
- Modify: `benchmarks/chronosbench/scenarios.go`, `benchmarks/cmd/bench/main.go`
- Test: `benchmarks/smoke_test.go`

- [ ] **Step 1: 실패 스모크 추가**

`benchmarks/smoke_test.go`에 추가:

```go
func TestSmoke_ChronosChain(t *testing.T) {
	cfg := benchConfig(t)
	cfg.Tasks = 60 // 체인 6개 × 길이 10
	r, err := bench.Run(context.Background(), chronosbench.Chain(10), cfg, 1)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if r.Extra["chain_e2e_p50_ms"] <= 0 || r.Extra["per_hop_ms"] <= 0 {
		t.Errorf("suspicious chain stats: %+v", r.Extra)
	}
}

func TestSmoke_ChronosGroup(t *testing.T) {
	cfg := benchConfig(t)
	cfg.Tasks = 60 // 그룹 6개 × 멤버 10
	r, err := bench.Run(context.Background(), chronosbench.Group(10), cfg, 1)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if r.Extra["group_e2e_p50_ms"] <= 0 {
		t.Errorf("suspicious group stats: %+v", r.Extra)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd benchmarks && go test . -run 'TestSmoke_ChronosChain|TestSmoke_ChronosGroup' -p 1`
Expected: FAIL — undefined Chain/Group.

- [ ] **Step 3: 구현**

`benchmarks/chronosbench/scenarios.go`에 추가 (placeholder guard 라인 제거, `fmt`는 미사용이면 import 정리):

```go
// --- chain: K chains of length L; per-hop latency from chain-creation ts ---

// ChainTailArgs is a chain link payload; TS is the CHAIN's creation time so the
// last link measures whole-chain latency.
type ChainTailArgs struct {
	TS   int64  `json:"ts"`
	Last bool   `json:"last"`
	Pad  string `json:"pad"`
}

func (ChainTailArgs) Kind() string { return "bench:chainlink" }

type chainScenario struct{ length int }

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
```

`cmd/bench/main.go`의 `pick`에 케이스 추가 (+ `-chainlen`, `-groupsize` 플래그, 기본 10):

```go
	chainLen := flag.Int("chainlen", 10, "links per chain (chain scenario)")
	groupSize := flag.Int("groupsize", 10, "members per group (group scenario)")
```
pick 시그니처를 `pick(target, scenario string, chainLen, groupSize int)`로 바꾸고:
```go
		case "chain":
			return chronosbench.Chain(chainLen), nil
		case "group":
			return chronosbench.Group(groupSize), nil
```

- [ ] **Step 4: 통과 확인**

Run: `cd benchmarks && go test . -run TestSmoke -p 1` → 4개 PASS. vet/gofmt clean.

- [ ] **Step 5: 커밋**

```bash
git add benchmarks/
git commit -m "feat: chronosbench chain/group 시나리오"
```

---

## Task 4: asynqbench — enqueue·e2e

**Files:**
- Create: `benchmarks/asynqbench/scenarios.go`
- Modify: `benchmarks/go.mod`(asynq), `benchmarks/cmd/bench/main.go`(pick), `benchmarks/smoke_test.go`

- [ ] **Step 1: 실패 스모크 추가**

`benchmarks/smoke_test.go`에 추가 (import에 asynqbench):

```go
func TestSmoke_AsynqE2E(t *testing.T) {
	cfg := benchConfig(t)
	r, err := bench.Run(context.Background(), asynqbench.E2E(), cfg, 1)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if r.Throughput <= 0 || r.P50 <= 0 {
		t.Errorf("suspicious stats: %+v", r)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd benchmarks && go test . -run TestSmoke_AsynqE2E -p 1`
Expected: FAIL — undefined asynqbench.

- [ ] **Step 3: 구현**

`go get github.com/hibiken/asynq@latest` (benchmarks 모듈에서). `benchmarks/asynqbench/scenarios.go`:

```go
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
```

주의: asynq의 실제 API 시그니처가 다르면(버전에 따라 `EnqueueContext` 부재 등) `client.Enqueue(task)`로 대체하고 보고서에 기록. chronos e2e의 지연 계산과 **정확히 같은 방식**(payload ts, 앞 10% 제외)이어야 공정.

`cmd/bench/main.go` pick의 asynq 케이스 연결:
```go
	case "asynq":
		switch scenario {
		case "enqueue":
			return asynqbench.Enqueue(), nil
		case "e2e":
			return asynqbench.E2E(), nil
		}
		return nil, fmt.Errorf("asynq supports enqueue|e2e only")
```

- [ ] **Step 4: 통과 확인**

Run: `cd benchmarks && go mod tidy && go test . -run TestSmoke -p 1` → 5개 PASS. vet/gofmt clean.

- [ ] **Step 5: 커밋**

```bash
git add benchmarks/
git commit -m "feat: asynqbench 비교 시나리오 (enqueue/e2e, 기본 설정 공정성)"
```

---

## Task 5: Makefile + benchmarks/README

**Files:**
- Modify: `Makefile`
- Create: `benchmarks/README.md`

- [ ] **Step 1: Makefile 타깃 추가**

`.PHONY`에 `bench bench-build` 추가, 타깃:

```make
# Run the full benchmark matrix against a local Redis (DB 15 is FLUSHED).
# See docs/BENCHMARKS.md for methodology. Scaling: C in {1,4,16,64}.
bench:
	cd benchmarks && go run ./cmd/bench -target chronos -scenario enqueue
	cd benchmarks && go run ./cmd/bench -target asynq   -scenario enqueue
	cd benchmarks && for c in 1 4 16 64; do \
		go run ./cmd/bench -target chronos -scenario e2e -concurrency $$c; \
		go run ./cmd/bench -target asynq   -scenario e2e -concurrency $$c; \
	done
	cd benchmarks && go run ./cmd/bench -target chronos -scenario chain
	cd benchmarks && go run ./cmd/bench -target chronos -scenario group

bench-build:
	cd benchmarks && go build ./... && go vet ./...
```

`check` 타깃에 `bench-build` 추가: `check: fmt-check vet test-race test-contrib bench-build` (벤치는 CI에서 빌드만 검증 — 스모크 테스트는 로컬 `cd benchmarks && go test .`).

주의: CI(ci.yml)는 건드리지 않음 — `make check`에 bench-build가 들어가면 CI가 자동으로 벤치 모듈 빌드를 검증하지만, asynq 다운로드가 필요함. ci.yml의 `make check` 사용 여부 확인: CI는 개별 스텝(gofmt/vet/test)이라 영향 없음 — 그대로 두되 확인 후 보고.

- [ ] **Step 2: benchmarks/README.md 작성**

```markdown
# chronos-go benchmarks

Throughput and end-to-end latency measurements for chronos-go, with an
apples-to-apples comparison against [asynq](https://github.com/hibiken/asynq).
Methodology and current numbers: [docs/BENCHMARKS.md](../docs/BENCHMARKS.md).

## Run

Requires a local Redis; **DB 15 is flushed** between runs.

```bash
# one scenario
go run ./cmd/bench -target chronos -scenario e2e -tasks 20000 -concurrency 16

# the full matrix (from the repo root)
make bench
```

Flags: `-target chronos|asynq`, `-scenario enqueue|e2e|chain|group`, `-tasks`,
`-concurrency`, `-producers`, `-payload`, `-chainlen`, `-groupsize`, `-repeats`
(median reported), `-redis`, `-db`, `-json`.

## Fairness

Both libraries run with their **default configs**; only shared knobs
(concurrency, payload size, task count) are set, with identical payload schema
and latency measurement (timestamp embedded at enqueue, delta taken in the
handler, first 10% excluded as warmup). Numbers vary by machine — run it
yourself, and asynq configuration improvements are welcome.
```

- [ ] **Step 3: 검증 + 커밋**

Run: `make bench-build && make check 2>&1 | tail -3` → PASS.

```bash
git add Makefile benchmarks/README.md
git commit -m "build: make bench/bench-build + benchmarks README"
```

---

## Task 6: 📊 측정 1단계 — chronos 단독 (체크포인트)

**컨트롤러(메인 세션)가 직접 실행하는 태스크** — 서브에이전트에 위임해도 되지만 결과 보고가 목적.

- [ ] **Step 1: 실행 환경 기록**

`sysctl -n machdep.cpu.brand_string; sysctl -n hw.ncpu; redis-server --version; go version` — 결과를 기록(BENCHMARKS.md 머신 사양에 사용).

- [ ] **Step 2: chronos 전 시나리오 실행**

```bash
cd benchmarks
go run ./cmd/bench -target chronos -scenario enqueue -json | tee /tmp/bench-results.jsonl
for c in 1 4 16 64; do go run ./cmd/bench -target chronos -scenario e2e -concurrency $c -json; done | tee -a /tmp/bench-results.jsonl
go run ./cmd/bench -target chronos -scenario chain -json | tee -a /tmp/bench-results.jsonl
go run ./cmd/bench -target chronos -scenario group -json | tee -a /tmp/bench-results.jsonl
```
(백그라운드 실행 권장 — 수 분 소요. tasks 기본 20000.)

- [ ] **Step 3: 결과를 사용자에게 보고** ⏸

표로 정리해 보고: 이상 수치(스케일링이 역전되는 구간, p99 스파이크, chain/group 오버헤드 과대)가 있으면 짚는다. **사용자와 해석 후 다음 태스크 진행.**

---

## Task 7: 📊 측정 2단계 — asynq 비교 (체크포인트)

- [ ] **Step 1: 비교 실행**

```bash
cd benchmarks
go run ./cmd/bench -target asynq -scenario enqueue -json | tee -a /tmp/bench-results.jsonl
for c in 1 4 16 64; do go run ./cmd/bench -target asynq -scenario e2e -concurrency $c -json; done | tee -a /tmp/bench-results.jsonl
```

- [ ] **Step 2: 비교표 작성 + 사용자 보고** ⏸

chronos vs asynq 나란히 표. 어느 쪽이 얼마나 빠른/느린지, 원인 가설(예: chronos의 XREADGROUP 블로킹 vs asynq의 폴링, Lua 왕복 수). **느린 항목이 있으면 개선할지 사용자와 결정** — 개선하면 이 브랜치에서 수정 후 재측정.

---

## Task 8: 문서화 + 리뷰 + PR

**Files:**
- Create: `docs/BENCHMARKS.md`
- Modify: `README.md`

- [ ] **Step 1: docs/BENCHMARKS.md 작성 (측정 수치 포함)**

구성: 방법론(공정성 원칙 — benchmarks/README와 동일 서술 + 3회 중앙값 + 워밍업 10% 제외 + 로컬 Redis 근거), 머신 사양(Task 6에서 기록), 시나리오별 표(enqueue / e2e 스케일링 chronos vs asynq / chain / group), 해석 요약(수치가 말하는 것과 말하지 않는 것), 재현 방법(`make bench`).

- [ ] **Step 2: README Performance 섹션**

"## Redis Cluster" 앞에 삽입 — 핵심 수치 3~4줄(대표: e2e C=16 처리량·p95, asynq 대비 관계) + 정직한 캐비앳 + BENCHMARKS.md 링크. 수치는 Task 6·7 실측값.

- [ ] **Step 3: 최종 검증**

`make check` + `cd benchmarks && go test . -p 1`(스모크) + README fence 짝수 확인.

- [ ] **Step 4: 코드 리뷰 + PR**

k:code-reviewer로 브랜치 리뷰 — 특히: 측정 방법론의 함정(경과시간에 enqueue 포함 여부의 일관성, 워밍업 처리, chan 수집의 병목 가능성), 공정성(asynq 시나리오가 chronos와 동일 조건인지), CLI 플래그 정합. 반영 후 PR:

```bash
gh pr create --assignee kenshin579 --title "feat: 성능 벤치마크 (chronos vs asynq) + BENCHMARKS.md" --body "$(cat <<'EOF'
## 배경
큐 라이브러리인데 성능 수치가 없었다. 벤치마크로 병목 조기 발견 + README 채택 근거 + v1.0.0 동결 근거를 확보한다.

## 변경
- `benchmarks/` 별도 모듈(asynq 의존 격리): bench 라이브러리(러너/백분위/리포터) + chronosbench(enqueue/e2e/chain/group) + asynqbench(enqueue/e2e) + `cmd/bench` CLI.
- 공정성: 양쪽 기본 설정, 동일 payload 스키마·지연 측정(ts 내장, 워밍업 10% 제외), 3회 중앙값, 방법론·재현 스크립트 공개(`make bench`).
- `docs/BENCHMARKS.md`(방법론+실측표) + README Performance 섹션.

## 측정 요약
(Task 6·7 결과를 여기에 기입)

## 테스트 계획
- [x] bench 단위 테스트(백분위/중앙값) + 스모크 5개(실제 Redis)
- [x] make check 무회귀 (+bench-build)
- [x] 실측 완료 — BENCHMARKS.md 수치는 본 머신 기준

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review (계획 작성자 확인 완료)

- **스펙 커버리지**: 모듈 구조 A(T1-4) / 시나리오 B 5개(T2-4, 3회 중앙값·워밍업은 runner·시나리오에 구현) / 공정성 C(T4 주의문 + benchmarks/README + T8 문서) / 문서화 D(T5, T8) / 진행 방식(T6·T7 체크포인트) — 전 항목 매핑.
- **placeholder**: T8의 "수치 기입"은 T6·T7 실측 결과를 넣는 명시적 절차(플레이스홀더 아님 — 측정이 선행 태스크).
- **타입 일관성**: `bench.Config/Result/Scenario/Run/Percentiles/MedianByThroughput`(T1)를 T2-4가 동일 시그니처로 사용. `chronosbench.Enqueue()/E2E()/Chain(n)/Group(n)`·`asynqbench.Enqueue()/E2E()`가 CLI pick과 일치.
- **주의**: asynq API가 버전에 따라 다를 수 있음(T4에 대체 지침). e2e 경과시간은 "첫 enqueue 시작→마지막 처리 완료"로 양쪽 동일(생산·소비 동시 진행 모델).
