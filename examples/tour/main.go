// Command tour is a narrated, runnable walkthrough of everything chronos-go can
// do so far (M1 core queue, M2 reliability, M3 delayed + unique). It is not a
// test — it is meant to be *watched*: run it and read the output to see tasks
// being enqueued, processed, retried, dead-lettered, delayed, and deduplicated.
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

	fmt.Println("\n───────────────────────────────────────────────")
	fmt.Println("투어 완료. 위 로그가 chronos-go가 실제로 동작하는 모습입니다.")
	fmt.Println("Redis 내부 상태를 직접 보려면 docs/OBSERVING.md 를 참고하세요.")
}

func section(title string) {
	fmt.Printf("\n=== %s ===\n", title)
}
