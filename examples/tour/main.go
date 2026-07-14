// Command tour is a narrated, runnable walkthrough of everything chronos-go can
// do: the core queue, reliability (retry / dead-letter), delayed + unique tasks,
// the Inspector, the distributed scheduler with leader failover, the retention
// janitor, the heartbeat, weighted priority queues, completed-task retention,
// task chains, task groups, workflows (fan-out/fan-in with results), group
// member chains (fan-out of pipelines), and queue
// pause/resume. It is not a test — it is meant to be *watched*:
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

// ChainStepArgs is one step of the chain demo.
type ChainStepArgs struct {
	Step int `json:"step"`
}

func (ChainStepArgs) Kind() string { return "demo:chainstep" }

// GroupMemberArgs is one member of the group demo.
type GroupMemberArgs struct {
	N int `json:"n"`
}

func (GroupMemberArgs) Kind() string { return "demo:groupmember" }

// GroupReportArgs is the group demo's callback.
type GroupReportArgs struct {
	Batch string `json:"batch"`
}

func (GroupReportArgs) Kind() string { return "demo:groupreport" }

// OcrArgs is the first step of the result-passing demo (OCR → translate).
type OcrArgs struct {
	Image string `json:"image"`
}

func (OcrArgs) Kind() string { return "tour:ocr" }

// OcrOut is the OCR step's result, relayed to the translate step.
type OcrOut struct {
	Text string `json:"text"`
}

// TranslateArgs is a parallel stage member; it reads the previous step's result.
type TranslateArgs struct {
	Lang string `json:"lang"`
}

func (TranslateArgs) Kind() string { return "tour:translate" }

// MergeArgs is the group callback; it fans the parallel translations back in.
type MergeArgs struct{}

func (MergeArgs) Kind() string { return "tour:merge" }

// MigDump is the first link of a per-tenant migration chain (group member chain
// demo): dump → load, one chain per tenant, run in parallel.
type MigDump struct {
	Tenant string `json:"tenant"`
}

func (MigDump) Kind() string { return "tour:mig-dump" }

// MigLoad is the migration chain's final link; it reports the member's result.
type MigLoad struct {
	Tenant string `json:"tenant"`
}

func (MigLoad) Kind() string { return "tour:mig-load" }

// MigOut is the row count relayed dump → load and collected by the callback.
type MigOut struct {
	Rows int `json:"rows"`
}

// MigVerify is the group callback that fans in every tenant's migration result.
type MigVerify struct{}

func (MigVerify) Kind() string { return "tour:mig-verify" }

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

	section("11) completed retention: 성공한 태스크를 잠시 보관해 눈으로 확인")
	rmux := chronos.NewMux()
	chronos.AddHandler(rmux, func(ctx context.Context, t *chronos.Task[GreetArgs]) error {
		fmt.Printf("   ✅ [retention] %s 처리 완료 — 3초간 completed로 보관됨\n", t.Args.Name)
		return nil
	})
	rsrv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:          map[string]int{"ret-demo": 1},
		Concurrency:     2,
		JanitorInterval: 500 * time.Millisecond,
	})
	if err := rsrv.Start(ctx, rmux); err != nil {
		fmt.Printf("retention 서버 start 실패: %v\n", err)
	}
	if _, err := chronos.Enqueue(ctx, client, GreetArgs{Name: "보관테스트"}, chronos.WithQueue("ret-demo"), chronos.WithRetention(3*time.Second)); err != nil {
		fmt.Printf("enqueue 실패: %v\n", err)
	}
	time.Sleep(1 * time.Second)
	retCompleted := func() int64 {
		qs, err := insp.Queues(ctx)
		if err != nil {
			return -1
		}
		for _, q := range qs {
			if q.Queue == "ret-demo" {
				return q.Completed
			}
		}
		return 0
	}
	fmt.Printf("   처리 직후 → completed=%d (조회 가능: task ls ret-demo completed)\n", retCompleted())
	fmt.Println("   ... retention(3s) 경과 대기, janitor가 정리 ...")
	time.Sleep(4 * time.Second)
	fmt.Printf("   janitor 실행 후 → completed=%d (자동 정리됨)\n", retCompleted())
	shutRetCtx, cancelR := context.WithTimeout(context.Background(), 3*time.Second)
	_ = rsrv.Shutdown(shutRetCtx)
	cancelR()

	section("12) chain: A 성공 → B → C 연쇄 실행, 실패하면 중단 + 재실행으로 재개")
	var chainFail atomic.Bool
	chainFail.Store(true)
	cmux := chronos.NewMux()
	chronos.AddHandler(cmux, func(ctx context.Context, t *chronos.Task[ChainStepArgs]) error {
		if t.Args.Step == 2 && chainFail.Load() {
			fmt.Printf("   💥 [chain] %d단계 실패 — 체인 중단 (뒤 단계는 대기)\n", t.Args.Step)
			return errors.New("2단계 오류")
		}
		fmt.Printf("   🔗 [chain] %d단계 실행\n", t.Args.Step)
		return nil
	})
	csrv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"chain-demo": 1},
		Concurrency: 2,
	})
	if err := csrv.Start(ctx, cmux); err != nil {
		fmt.Printf("chain 서버 start 실패: %v\n", err)
	}
	if _, err := chronos.NewChain().
		Then(ChainStepArgs{Step: 1}, chronos.WithQueue("chain-demo")).
		Then(ChainStepArgs{Step: 2}, chronos.WithQueue("chain-demo"), chronos.WithMaxRetry(0)).
		Then(ChainStepArgs{Step: 3}, chronos.WithQueue("chain-demo")).
		Enqueue(ctx, client); err != nil {
		fmt.Printf("chain enqueue 실패: %v\n", err)
	}
	time.Sleep(1500 * time.Millisecond) // 1단계 성공, 2단계 dead-letter
	tasks, _ := insp.ListTasks(ctx, "chain-demo", "archived", 10)
	for _, ti := range tasks {
		fmt.Printf("   📮 dead-letter: %s (뒤에 %d단계 대기 중)\n", ti.ID, ti.ChainPending)
		chainFail.Store(false)
		fmt.Println("   원인 해소 후 RunTask로 재실행 → 체인 재개:")
		if err := insp.RunTask(ctx, "chain-demo", ti.ID); err != nil {
			fmt.Printf("   run 실패: %v\n", err)
		}
	}
	time.Sleep(1500 * time.Millisecond) // 2단계 재실행 + 3단계 완주
	shutChainCtx, cancelC := context.WithTimeout(context.Background(), 3*time.Second)
	_ = csrv.Shutdown(shutChainCtx)
	cancelC()

	section("13) group: N개 병렬 실행 → 전부 성공하면 콜백 1회 (실패 시 대기, 재실행으로 재개)")
	var gFail atomic.Bool
	gFail.Store(true)
	gmux := chronos.NewMux()
	chronos.AddHandler(gmux, func(ctx context.Context, t *chronos.Task[GroupMemberArgs]) error {
		if t.Args.N == 2 && gFail.Load() {
			fmt.Printf("   💥 [group] 멤버 %d 실패 — 그룹 대기\n", t.Args.N)
			return errors.New("멤버 2 오류")
		}
		fmt.Printf("   🧩 [group] 멤버 %d 완료\n", t.Args.N)
		return nil
	})
	chronos.AddHandler(gmux, func(ctx context.Context, t *chronos.Task[GroupReportArgs]) error {
		fmt.Printf("   🎉 [group] 콜백 실행 — 배치 %s 전원 완료!\n", t.Args.Batch)
		return nil
	})
	gsrv := chronos.NewServer(rdb, chronos.ServerConfig{
		Queues:      map[string]int{"group-demo": 1},
		Concurrency: 4,
	})
	if err := gsrv.Start(ctx, gmux); err != nil {
		fmt.Printf("group 서버 start 실패: %v\n", err)
	}
	ginfo, err := chronos.NewGroup().
		Add(GroupMemberArgs{N: 1}, chronos.WithQueue("group-demo")).
		Add(GroupMemberArgs{N: 2}, chronos.WithQueue("group-demo"), chronos.WithMaxRetry(0)).
		Add(GroupMemberArgs{N: 3}, chronos.WithQueue("group-demo")).
		OnComplete(GroupReportArgs{Batch: "demo"}, chronos.WithQueue("group-demo")).
		Enqueue(ctx, client)
	if err != nil {
		fmt.Printf("group enqueue 실패: %v\n", err)
	}
	time.Sleep(1500 * time.Millisecond) // 1·3 완료, 2 dead-letter → 그룹 대기
	if got, gerr := insp.GetTask(ctx, "group-demo", ginfo.MemberIDs[1]); gerr == nil {
		fmt.Printf("   📮 dead-letter 멤버: %s (그룹 잔여 %d명 — 콜백 대기 중)\n", got.ID, got.GroupPending)
		gFail.Store(false)
		fmt.Println("   원인 해소 후 RunTask로 재실행 → 그룹 재개:")
		if rerr := insp.RunTask(ctx, "group-demo", got.ID); rerr != nil {
			fmt.Printf("   run 실패: %v\n", rerr)
		}
	}
	time.Sleep(1500 * time.Millisecond) // 멤버 2 재실행 + 콜백
	shutGroupCtx, cancelG := context.WithTimeout(context.Background(), 3*time.Second)
	_ = gsrv.Shutdown(shutGroupCtx)
	cancelG()

	section("14) pause/resume: 큐 소비를 일시정지 — 쌓이는 게 보이고, 재개하면 이어서")
	pmux2 := chronos.NewMux()
	chronos.AddHandler(pmux2, func(ctx context.Context, t *chronos.Task[GreetArgs]) error {
		fmt.Printf("   ▶ [pause-demo] %s 처리\n", t.Args.Name)
		return nil
	})
	psrv2 := chronos.NewServer(rdb, chronos.ServerConfig{Queues: map[string]int{"pause-demo": 1}, Concurrency: 2})
	if err := psrv2.Start(ctx, pmux2); err != nil {
		fmt.Printf("pause 서버 start 실패: %v\n", err)
	}
	_ = insp.PauseQueue(ctx, "pause-demo")
	fmt.Println("   ⏸ 큐 일시정지 → 태스크 3개 enqueue (소비되지 않음)")
	time.Sleep(1500 * time.Millisecond) // pause 캐시 반영
	for i := 1; i <= 3; i++ {
		_, _ = chronos.Enqueue(ctx, client, GreetArgs{Name: fmt.Sprintf("대기-%d", i)}, chronos.WithQueue("pause-demo"))
	}
	time.Sleep(2 * time.Second)
	if qs, err := insp.Queues(ctx); err == nil {
		for _, q := range qs {
			if q.Queue == "pause-demo" {
				fmt.Printf("   pending=%d paused=%v (쌓여 있음)\n", q.Pending, q.Paused)
			}
		}
	}
	fmt.Println("   ▶ resume → 쌓인 3개가 이어서 처리:")
	_ = insp.ResumeQueue(ctx, "pause-demo")
	time.Sleep(2500 * time.Millisecond)
	shutPause2, cancelP2 := context.WithTimeout(context.Background(), 3*time.Second)
	_ = psrv2.Shutdown(shutPause2)
	cancelP2()

	section("15) 워크플로: OCR → [번역 2개 병렬] → 병합 — 결과가 스테이지를 타고 흐른다")
	resMux := chronos.NewMux()
	chronos.AddHandlerR(resMux, func(ctx context.Context, t *chronos.Task[OcrArgs]) (OcrOut, error) {
		fmt.Printf("   ▶ [ocr] %s 인식\n", t.Args.Image)
		return OcrOut{Text: "hello chronos"}, nil
	})
	chronos.AddHandlerR(resMux, func(ctx context.Context, t *chronos.Task[TranslateArgs]) (OcrOut, error) {
		src, err := chronos.PrevResult[OcrOut](t)
		if err != nil {
			return OcrOut{}, err
		}
		fmt.Printf("   ▶ [translate:%s] %q 번역\n", t.Args.Lang, src.Text)
		return OcrOut{Text: t.Args.Lang + "(" + src.Text + ")"}, nil
	})
	chronos.AddHandler(resMux, func(ctx context.Context, t *chronos.Task[MergeArgs]) error {
		rs, err := chronos.GroupResults[OcrOut](t)
		if err != nil {
			return err
		}
		fmt.Printf("   ▶ [merge] 병렬 결과 수신: %q + %q\n", rs[0].Text, rs[1].Text)
		return nil
	})
	resSrv := chronos.NewServer(rdb, chronos.ServerConfig{Queues: map[string]int{"results": 1}, Concurrency: 4})
	if err := resSrv.Start(ctx, resMux); err != nil {
		fmt.Printf("results 서버 start 실패: %v\n", err)
	}
	_, _ = chronos.NewChain().
		Then(OcrArgs{Image: "scan-001.png"}, chronos.WithQueue("results")).
		ThenGroup(chronos.NewGroup().
			Add(TranslateArgs{Lang: "ko"}, chronos.WithQueue("results")).
			Add(TranslateArgs{Lang: "ja"}, chronos.WithQueue("results")).
			OnComplete(MergeArgs{}, chronos.WithQueue("results"))).
		Enqueue(ctx, client)
	time.Sleep(3 * time.Second)
	shutRes, cancelRes := context.WithTimeout(context.Background(), 3*time.Second)
	_ = resSrv.Shutdown(shutRes)
	cancelRes()

	section("16) 그룹 멤버 체인: 테넌트별 '덤프→변환→적재' 파이프라인을 병렬로, 전부 끝나면 검증")
	migMux := chronos.NewMux()
	chronos.AddHandlerR(migMux, func(ctx context.Context, t *chronos.Task[MigDump]) (MigOut, error) {
		fmt.Printf("   ▶ [dump] %s\n", t.Args.Tenant)
		return MigOut{Rows: len(t.Args.Tenant) * 10}, nil
	})
	chronos.AddHandlerR(migMux, func(ctx context.Context, t *chronos.Task[MigLoad]) (MigOut, error) {
		prev, _ := chronos.PrevResult[MigOut](t)
		fmt.Printf("   ▶ [load] %s (%d rows)\n", t.Args.Tenant, prev.Rows)
		return MigOut{Rows: prev.Rows}, nil
	})
	chronos.AddHandler(migMux, func(ctx context.Context, t *chronos.Task[MigVerify]) error {
		rs, _ := chronos.GroupResults[MigOut](t)
		total := 0
		for _, r := range rs {
			total += r.Rows
		}
		fmt.Printf("   ▶ [verify] 테넌트 %d개 마이그레이션 완료, 총 %d rows\n", len(rs), total)
		return nil
	})
	migSrv := chronos.NewServer(rdb, chronos.ServerConfig{Queues: map[string]int{"mig": 1}, Concurrency: 4})
	if err := migSrv.Start(ctx, migMux); err != nil {
		fmt.Printf("mig 서버 start 실패: %v\n", err)
	}
	migG := chronos.NewGroup()
	for _, tenant := range []string{"acme", "globex", "initech"} {
		migG.AddChain(chronos.NewChain().
			Then(MigDump{Tenant: tenant}, chronos.WithQueue("mig")).
			Then(MigLoad{Tenant: tenant}, chronos.WithQueue("mig")))
	}
	_, _ = migG.OnComplete(MigVerify{}, chronos.WithQueue("mig")).Enqueue(ctx, client)
	time.Sleep(3 * time.Second)
	shutMig, cancelMig := context.WithTimeout(context.Background(), 3*time.Second)
	_ = migSrv.Shutdown(shutMig)
	cancelMig()

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
