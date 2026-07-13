// Command soak runs a long mixed workload against a local Redis and judges
// heap / goroutine / Redis-keyspace trends for leaks. See
// docs/superpowers/specs/2026-07-13-soak-test-design.md.
//
//	go run ./cmd/soak -duration 1h -rate 200
//
// Exit code: 1 when any check fails on a run of 30m or longer; runs shorter
// than 30m are informational only and always exit 0.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
	"github.com/kenshin579/chronos-go/benchmarks/soak"
)

const (
	sampleEvery = 30 * time.Second
	minReliable = 30 * time.Minute
)

func main() {
	duration := flag.Duration("duration", time.Hour, "soak length (>=30m for a trustworthy verdict)")
	rate := flag.Int("rate", 200, "base enqueue rate (tasks/sec)")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	db := flag.Int("db", 15, "Redis DB (FLUSHED at start — use a dedicated DB)")
	out := flag.String("out", "soak.jsonl", "JSONL sample log path")
	serverLog := flag.String("serverlog", "soak-server.log", "chronos server/scheduler log path (deliberate failures are logged per task — kept out of the terminal)")
	flag.Parse()

	if err := run(*duration, *rate, *redisAddr, *db, *out, *serverLog); err != nil {
		fmt.Fprintln(os.Stderr, "soak:", err)
		os.Exit(1)
	}
}

func run(duration time.Duration, rate int, redisAddr string, db int, outPath, serverLogPath string) error {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: db})
	defer rdb.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis at %s: %w", redisAddr, err)
	}
	fmt.Printf("⚠ FLUSHING Redis DB %d at %s\n", db, redisAddr)
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("flushdb: %w", err)
	}

	client := chronos.NewClient(rdb)
	insp := chronos.NewInspector(rdb)
	w := soak.NewWorkload(client, insp, soak.Config{Rate: rate})

	jsonl, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", outPath, err)
	}
	defer jsonl.Close()

	// 의도적 실패(dead-letter/discard/fail-once)가 태스크마다 ERROR 로그를
	// 남기므로, 서버/스케줄러 운영 로그는 터미널 대신 파일로 보낸다.
	logFile, err := os.Create(serverLogPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", serverLogPath, err)
	}
	defer logFile.Close()
	logger := slog.New(slog.NewTextHandler(logFile, nil))

	srv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"soak-a": 3, "soak-b": 1},
		Concurrency: 16,
		Logger:      logger,
	})
	if err := srv.Start(ctx, w.Mux()); err != nil {
		return fmt.Errorf("server start: %w", err)
	}

	sched := chronos.NewScheduler(rdb, chronos.SchedulerConfig{Logger: logger})
	if err := chronos.RegisterInterval(sched, time.Second, soak.SchedArgs(), chronos.WithQueue("soak-a")); err != nil {
		return fmt.Errorf("register schedule: %w", err)
	}
	if err := sched.Start(ctx); err != nil {
		return fmt.Errorf("scheduler start: %w", err)
	}

	loadCtx, cancelLoad := context.WithTimeout(ctx, duration)
	defer cancelLoad()
	loadDone := make(chan struct{})
	go func() { defer close(loadDone); w.Run(loadCtx) }()

	fmt.Printf("soak: duration=%s rate=%d/s queues=soak-a(3):soak-b(1) out=%s serverlog=%s\n", duration, rate, outPath, serverLogPath)
	sampler := soak.NewSampler(rdb, []string{"soak-a", "soak-b"}, w.Completed())
	var samples []soak.Sample
	started := time.Now()
	tick := time.NewTicker(sampleEvery)
	defer tick.Stop()

collect:
	for {
		select {
		case <-loadCtx.Done():
			break collect
		case <-tick.C:
			s, err := sampler.Collect(ctx)
			if err != nil {
				fmt.Fprintln(os.Stderr, "soak: sample:", err)
				continue
			}
			fmt.Println(s.Line())
			if err := soak.WriteJSONL(jsonl, s); err != nil {
				fmt.Fprintln(os.Stderr, "soak: jsonl:", err)
			}
			samples = append(samples, s)
		}
	}

	elapsed := time.Since(started)
	stop() // 시그널 등록 해제 — 셧다운 중 2차 Ctrl-C가 기본 동작으로 동작하도록

	<-loadDone
	shutdownFailed := false
	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sched.Shutdown(shutCtx); err != nil {
		fmt.Fprintln(os.Stderr, "soak: scheduler shutdown:", err)
		shutdownFailed = true
	}
	if err := srv.Shutdown(shutCtx); err != nil {
		fmt.Fprintln(os.Stderr, "soak: server shutdown:", err)
		shutdownFailed = true
	}

	checks, usable := soak.Evaluate(samples)
	fmt.Printf("\n=== soak verdict (%d samples over %s) ===\n", len(samples), elapsed.Round(time.Second))
	if !usable {
		fmt.Println("샘플 부족 — 판정 불가 (참고용 실행)")
		return nil
	}
	failed := false
	fmt.Printf("%-12s %14s %14s  %-24s %s\n", "METRIC", "FIRST-HALF", "SECOND-HALF", "RULE", "RESULT")
	for _, c := range checks {
		res := "PASS"
		if !c.Pass {
			res = "FAIL"
			failed = true
		}
		fmt.Printf("%-12s %14.1f %14.1f  %-24s %s\n", c.Name, c.First, c.Second, c.Rule, res)
	}
	last := samples[len(samples)-1]
	fmt.Printf("families(last): stream=%d retry=%d sched=%d arch=%d comp=%d uniq=%d grp=%d reg=%d\n",
		last.Stream, last.Retry, last.Scheduled, last.Archived, last.Completed,
		last.Unique, last.Groups, last.Schedules)
	if elapsed < minReliable {
		fmt.Printf("⚠ 실행 시간 %s < %s — 판정 신뢰 불가, 참고용으로만 사용 (exit 0)\n",
			elapsed.Round(time.Second), minReliable)
		return nil
	}
	if shutdownFailed {
		fmt.Println("shutdown: FAIL (hang — goroutine leak signal)")
	}
	if failed || shutdownFailed {
		return fmt.Errorf("leak check failed")
	}
	fmt.Println("✓ 누수 징후 없음")
	return nil
}
