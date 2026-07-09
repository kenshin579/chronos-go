// Command tour is a narrated, runnable walkthrough of everything chronos-go can
// do: the core queue, reliability (retry / dead-letter), delayed + unique tasks,
// the Inspector, the distributed scheduler with leader failover, the retention
// janitor, the heartbeat, and weighted priority queues. It is not a test — it
// is meant to be *watched*:
// run it and read the output to see tasks being enqueued, processed, retried,
// dead-lettered, delayed, deduplicated, scheduled across a leader hand-off,
// auto-cleaned, and kept alive past RecoverMinIdle by the heartbeat.
//
//	go run ./examples/tour
//
// It talks to a Redis at $REDIS_ADDR (default 127.0.0.1:6379) on DB 15 and
// flushes that DB at start so each run is clean. While it runs you can watch the
// underlying Redis state from another terminal — see docs/OBSERVING.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
)

// --- Task types -------------------------------------------------------------

// GreetArgs is a trivial task used for the basic and unique demos.
type GreetArgs struct {
	Name string `json:"name"`
}

func (GreetArgs) Kind() string { return "demo:greet" }

// FlakyArgs fails on its first attempt and succeeds on the retry.
type FlakyArgs struct {
	ID int `json:"id"`
}

func (FlakyArgs) Kind() string { return "demo:flaky" }

// PoisonArgs always fails, so it exhausts its retries and is dead-lettered.
type PoisonArgs struct {
	ID int `json:"id"`
}

func (PoisonArgs) Kind() string { return "demo:poison" }

// ReminderArgs is scheduled for the future (delayed execution).
type ReminderArgs struct {
	Note string `json:"note"`
}

func (ReminderArgs) Kind() string { return "demo:reminder" }

// LongArgs is a slow task used to demonstrate the heartbeat: it runs longer than
// RecoverMinIdle, yet must execute exactly once.
type LongArgs struct {
	ID int `json:"id"`
}

func (LongArgs) Kind() string { return "demo:long" }

// PrioArgs carries its queue name so the priority demo can print, in processing
// order, which queue each task came from.
type PrioArgs struct {
	Queue string `json:"queue"`
}

func (PrioArgs) Kind() string { return "demo:prio" }

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("\n⚠️  Redis에 연결할 수 없습니다 (%s): %v\n", addr, err)
		fmt.Println("   로컬에서 띄우려면:  redis-server --daemonize yes --port 6379")
		os.Exit(1)
	}
	// Clean slate so the tour's output is easy to follow.
	rdb.FlushDB(ctx)

	client := chronos.NewClient(rdb)
	defer client.Close()

	// flakyAttempts tracks retries for the FlakyArgs handler.
	var flakyAttempts atomic.Int32

	mux := chronos.NewMux()
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[GreetArgs]) error {
		fmt.Printf("   👋 [greet] 안녕하세요, %s! (task=%s)\n", t.Args.Name, t.ID())
		return nil
	})
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[FlakyArgs]) error {
		if flakyAttempts.Add(1) == 1 {
			fmt.Printf("   💥 [flaky] id=%d 첫 시도 실패 — 재시도될 예정\n", t.Args.ID)
			return errors.New("일시적 오류")
		}
		fmt.Printf("   ✅ [flaky] id=%d 재시도에서 성공\n", t.Args.ID)
		return nil
	})
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[PoisonArgs]) error {
		fmt.Printf("   ☠️  [poison] id=%d 실행 (항상 실패)\n", t.Args.ID)
		return errors.New("영구 오류")
	})
	chronos.AddHandler(mux, func(ctx context.Context, t *chronos.Task[ReminderArgs]) error {
		fmt.Printf("   ⏰ [reminder] 도착: %q (task=%s)\n", t.Args.Note, t.ID())
		return nil
	})

	srv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 8,
		// Short intervals + fast backoff so the tour finishes in a few seconds.
		ForwardInterval: 200 * time.Millisecond,
		RetryDelayFunc:  func(retried int, err error) time.Duration { return 300 * time.Millisecond },
		OnDeadLetter: func(ctx context.Context, info *chronos.TaskInfo, err error) {
			fmt.Printf("   📮 [dead-letter] kind=%s task=%s 재시도 소진 → archived (err=%v)\n",
				info.Kind, info.ID, err)
		},
	})
	if err := srv.Start(ctx, mux); err != nil {
		fmt.Printf("서버 시작 실패: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	section("1) 기본 처리 (M1): enqueue → 워커가 타입 안전 핸들러로 처리")
	if _, err := chronos.Enqueue(ctx, client, GreetArgs{Name: "Frank"}); err != nil {
		fmt.Printf("enqueue 실패: %v\n", err)
	}
	time.Sleep(600 * time.Millisecond)

	section("2) 재시도 (M2): 첫 시도 실패 → 지수 백오프 후 재시도에서 성공")
	if _, err := chronos.Enqueue(ctx, client, FlakyArgs{ID: 1}, chronos.WithMaxRetry(3)); err != nil {
		fmt.Printf("enqueue 실패: %v\n", err)
	}
	time.Sleep(1200 * time.Millisecond)

	section("3) dead-letter (M2): 항상 실패하는 태스크는 MaxRetry+1회 후 보관 + 훅 발화")
	fmt.Println("   (MaxRetry=2 → 총 3회 실행 후 dead-letter)")
	if _, err := chronos.Enqueue(ctx, client, PoisonArgs{ID: 9}, chronos.WithMaxRetry(2)); err != nil {
		fmt.Printf("enqueue 실패: %v\n", err)
	}
	time.Sleep(1500 * time.Millisecond)

	section("4) 지연 실행 (M3): 1.5초 뒤에 실행되도록 예약")
	fmt.Printf("   지금 시각: %s — 예약: +1.5s\n", time.Now().Format("15:04:05.000"))
	if _, err := chronos.Enqueue(ctx, client, ReminderArgs{Note: "회의 시작"}, chronos.WithProcessIn(1500*time.Millisecond)); err != nil {
		fmt.Printf("enqueue 실패: %v\n", err)
	}
	fmt.Println("   ... 예약된 시각까지 대기 중 (Redis의 scheduled ZSET에 있음) ...")
	time.Sleep(2200 * time.Millisecond)

	section("5) 중복 억제 (M3): 동일 (kind+payload) 태스크는 처리 중 중복 enqueue가 거부됨")
	if _, err := chronos.Enqueue(ctx, client, GreetArgs{Name: "Dedup"}, chronos.WithUnique(30*time.Second)); err != nil {
		fmt.Printf("   1차 enqueue 실패: %v\n", err)
	} else {
		fmt.Println("   1차 enqueue: 성공 (unique 락 획득)")
	}
	_, err := chronos.Enqueue(ctx, client, GreetArgs{Name: "Dedup"}, chronos.WithUnique(30*time.Second))
	if errors.Is(err, chronos.ErrDuplicateTask) {
		fmt.Println("   2차 enqueue(동일 payload): 거부됨 → ErrDuplicateTask ✅")
	} else {
		fmt.Printf("   2차 enqueue 결과 예상과 다름: err=%v\n", err)
	}
	time.Sleep(600 * time.Millisecond)

	section("6) Inspector (현재 적재 상태 조회): 큐 카운트를 프로그램에서 읽기")
	insp := chronos.NewInspector(rdb)
	// A future-scheduled task so there is something to show.
	if _, err := chronos.Enqueue(ctx, client, ReminderArgs{Note: "나중에"}, chronos.WithProcessIn(time.Hour)); err != nil {
		fmt.Printf("enqueue 실패: %v\n", err)
	}
	if queues, err := insp.Queues(ctx); err == nil {
		for _, q := range queues {
			fmt.Printf("   📊 queue=%s pending=%d active=%d scheduled=%d retry=%d archived=%d\n",
				q.Queue, q.Pending, q.Active, q.Scheduled, q.Retry, q.Archived)
		}
	}
	fmt.Println("   (같은 정보를 CLI로: go run ./cmd/chronos --db 15 queue ls)")

	section("7) 분산 스케줄러 + 페일오버 (M4): 리더만 enqueue, 리더가 죽으면 자동 인계")
	// 같은 1초 잡을 등록한 스케줄러 두 개를 띄운다. 둘 다 등록하지만 리더 하나만
	// enqueue하고 결정적 dedup으로 중복이 없다. 리더를 종료하면 다른 하나가 승격된다.
	mkSched := func(name string) *chronos.Scheduler {
		s := chronos.NewScheduler(rdb, chronos.SchedulerConfig{LeaderTTL: time.Second})
		if err := chronos.RegisterInterval(s, 1*time.Second, GreetArgs{Name: "매초"}); err != nil {
			fmt.Printf("   [%s] register 실패: %v\n", name, err)
		}
		if err := s.Start(ctx); err != nil {
			fmt.Printf("   [%s] start 실패: %v\n", name, err)
		}
		return s
	}
	shutdownSched := func(s *chronos.Scheduler) {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = s.Shutdown(c)
		cancel()
	}

	fmt.Println("   [1] 스케줄러 A 시작 — 리더로 선출되어 매초 실행('became scheduler leader' 로그):")
	schedA := mkSched("A")
	time.Sleep(2200 * time.Millisecond)

	fmt.Println("   [2] 스케줄러 B 추가 — 팔로워로 대기(여전히 1초당 1회만 실행, 중복 없음):")
	schedB := mkSched("B")
	time.Sleep(1200 * time.Millisecond)

	fmt.Println("   [3] 리더 A를 graceful 종료 — B가 리더로 승격되어 스케줄 지속:")
	shutdownSched(schedA)
	time.Sleep(2500 * time.Millisecond) // B가 인계받아 계속 실행하는 것을 관찰

	shutdownSched(schedB)

	section("8) retention/janitor (M5): dead-letter는 보관되되 TTL 후 자동 정리됨")
	// A dedicated server with a short retention so cleanup is visible in seconds.
	jmux := chronos.NewMux()
	chronos.AddHandler(jmux, func(ctx context.Context, t *chronos.Task[PoisonArgs]) error {
		return errors.New("항상 실패") // dead-letter immediately (MaxRetry 0 below)
	})
	jsrv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:            map[string]int{"janitor-demo": 1},
		Concurrency:       4,
		ArchivedRetention: 2 * time.Second,        // demo: expire archived after 2s
		JanitorInterval:   500 * time.Millisecond, // clean often
	})
	if err := jsrv.Start(ctx, jmux); err != nil {
		fmt.Printf("janitor 서버 start 실패: %v\n", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := chronos.Enqueue(ctx, client, PoisonArgs{ID: 100 + i}, chronos.WithQueue("janitor-demo"), chronos.WithMaxRetry(0)); err != nil {
			fmt.Printf("enqueue 실패: %v\n", err)
		}
	}
	time.Sleep(1 * time.Second) // let them dead-letter into archived
	fmt.Printf("   5개 즉시 dead-letter → archived=%d (보관됨)\n", janitorDemoArchived(ctx, insp))
	fmt.Println("   ... retention(2s) 경과 대기, janitor가 정리 ...")
	time.Sleep(3 * time.Second)
	fmt.Printf("   janitor 실행 후 → archived=%d (자동 정리됨)\n", janitorDemoArchived(ctx, insp))
	shutSrvCtx, cancelJ := context.WithTimeout(context.Background(), 3*time.Second)
	_ = jsrv.Shutdown(shutSrvCtx)
	cancelJ()

	section("9) heartbeat (M5): RecoverMinIdle보다 오래 걸리는 태스크도 정확히 1회 실행")
	var longRuns atomic.Int32
	hmux := chronos.NewMux()
	chronos.AddHandler(hmux, func(ctx context.Context, t *chronos.Task[LongArgs]) error {
		n := longRuns.Add(1)
		fmt.Printf("   🫀 [long] 실행 #%d — 2초 처리(RecoverMinIdle 700ms 초과)\n", n)
		time.Sleep(2 * time.Second)
		return nil
	})
	hsrv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:            map[string]int{"hb-demo": 1},
		Concurrency:       2,
		RecoverMinIdle:    700 * time.Millisecond, // recoverer would reclaim a >700ms task...
		RecoverInterval:   300 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond, // ...but heartbeat keeps its lease fresh
	})
	if err := hsrv.Start(ctx, hmux); err != nil {
		fmt.Printf("heartbeat 서버 start 실패: %v\n", err)
	}
	if _, err := chronos.Enqueue(ctx, client, LongArgs{ID: 1}, chronos.WithQueue("hb-demo")); err != nil {
		fmt.Printf("enqueue 실패: %v\n", err)
	}
	time.Sleep(3 * time.Second) // > processing; recoverer had chances to (wrongly) duplicate
	fmt.Printf("   → 총 실행 횟수: %d (heartbeat가 lease를 갱신해 recoverer 오회수 없음)\n", longRuns.Load())
	shutHbCtx, cancelH := context.WithTimeout(context.Background(), 3*time.Second)
	_ = hsrv.Shutdown(shutHbCtx)
	cancelH()

	section("10) 우선순위 큐: 가중치 비율로 dequeue (critical:4 vs low:1)")
	// critical 8개 + low 4개를 먼저 쌓아두고 Concurrency 1로 처리해 순서를 관찰한다.
	// 두 큐 모두 일감이 있는 동안 critical이 4배 자주 나오고, low도 굶지 않는다.
	for i := 0; i < 8; i++ {
		if _, err := chronos.Enqueue(ctx, client, PrioArgs{Queue: "critical"}, chronos.WithQueue("critical")); err != nil {
			fmt.Printf("enqueue 실패: %v\n", err)
		}
	}
	for i := 0; i < 4; i++ {
		if _, err := chronos.Enqueue(ctx, client, PrioArgs{Queue: "low"}, chronos.WithQueue("low")); err != nil {
			fmt.Printf("enqueue 실패: %v\n", err)
		}
	}
	prioOrder := make(chan string, 12)
	pmux := chronos.NewMux()
	chronos.AddHandler(pmux, func(ctx context.Context, t *chronos.Task[PrioArgs]) error {
		prioOrder <- t.Args.Queue
		return nil
	})
	psrv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"critical": 4, "low": 1},
		Concurrency: 1, // 순차 처리라 dequeue 순서가 그대로 보인다
	})
	if err := psrv.Start(ctx, pmux); err != nil {
		fmt.Printf("priority 서버 start 실패: %v\n", err)
	}
	fmt.Print("   처리 순서: ")
	for i := 0; i < 12; i++ {
		select {
		case q := <-prioOrder:
			if q == "critical" {
				fmt.Print("🔴")
			} else {
				fmt.Print("🔵")
			}
		case <-time.After(10 * time.Second):
			fmt.Print(" (시간 초과)")
			i = 12
		}
	}
	fmt.Println("  (🔴=critical 🔵=low)")
	fmt.Println("   → 🔴가 4:1로 자주 나오되 🔵도 사이사이 처리됨 (smooth weighted round-robin).")
	fmt.Println("   → StrictPriority: true로 바꾸면 🔴 8개가 전부 끝난 뒤에야 🔵이 시작된다.")
	shutPrioCtx, cancelP := context.WithTimeout(context.Background(), 3*time.Second)
	_ = psrv.Shutdown(shutPrioCtx)
	cancelP()

	fmt.Println("\n───────────────────────────────────────────────")
	fmt.Println("투어 완료. 위 로그가 chronos-go가 실제로 동작하는 모습입니다.")
	fmt.Println("Redis 내부 상태를 직접 보려면 docs/OBSERVING.md 를 참고하세요.")
}

// janitorDemoArchived returns the archived count for the janitor-demo queue.
func janitorDemoArchived(ctx context.Context, insp *chronos.Inspector) int64 {
	queues, err := insp.Queues(ctx)
	if err != nil {
		return -1
	}
	for _, q := range queues {
		if q.Queue == "janitor-demo" {
			return q.Archived
		}
	}
	return 0
}

func section(title string) {
	fmt.Printf("\n=== %s ===\n", title)
}
