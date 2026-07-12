package chronos

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestChain_BuilderValidation(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// 빈 체인 → 에러.
	if _, err := NewChain().Enqueue(ctx, c); err == nil {
		t.Error("empty chain: want error")
	}
	// WithTaskID 병용 → 에러 (체인이 ID를 소유).
	if _, err := NewChain().Then(emailArgs{UserID: "a"}, WithTaskID("x")).Enqueue(ctx, c); err == nil {
		t.Error("WithTaskID in chain: want error")
	}
	// WithUnique 병용 → 에러 (미지원).
	if _, err := NewChain().Then(emailArgs{UserID: "a"}, WithUnique(time.Minute)).Enqueue(ctx, c); err == nil {
		t.Error("WithUnique in chain: want error")
	}
	// 꼬리 링크의 WithProcessAt → 에러 (의미가 달라짐; WithProcessIn을 쓸 것).
	if _, err := NewChain().
		Then(emailArgs{UserID: "a"}).
		Then(emailArgs{UserID: "b"}, WithProcessAt(time.Now().Add(time.Hour))).
		Enqueue(ctx, c); err == nil {
		t.Error("WithProcessAt on tail link: want error")
	}
	// 마지막이 아닌 링크의 WithDeadLetterDiscard → 에러 (꼬리째 소실 방지).
	if _, err := NewChain().
		Then(emailArgs{UserID: "a"}, WithDeadLetterDiscard()).
		Then(emailArgs{UserID: "b"}).
		Enqueue(ctx, c); err == nil {
		t.Error("WithDeadLetterDiscard on non-last link: want error")
	}
	// 링크 1개 → 정상.
	info, err := NewChain().Then(emailArgs{UserID: "solo"}).Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("single link: %v", err)
	}
	if info.ID == "" {
		t.Error("single link: empty task id")
	}
}

func TestChain_ChainPendingExposed(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := NewChain().
		Then(emailArgs{UserID: "s1"}, WithProcessIn(time.Hour)).
		Then(emailArgs{UserID: "s2"}).
		Then(emailArgs{UserID: "s3"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	got, err := insp.GetTask(ctx, "default", info.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ChainPending != 2 {
		t.Errorf("ChainPending = %d, want 2", got.ChainPending)
	}
}

// chainArgs is a dedicated kind so chain tests don't collide with other tests.
type chainArgs struct {
	Step int `json:"step"`
}

func (chainArgs) Kind() string { return "test:chainstep" }

func TestChain_SequentialExecution(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var mu sync.Mutex
	var order []int
	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[chainArgs]) error {
		mu.Lock()
		order = append(order, task.Args.Step)
		n := len(order)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
		return nil
	})
	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1, "chainq": 1},
		Concurrency: 2,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := NewChain().
		Then(chainArgs{Step: 1}).
		Then(chainArgs{Step: 2}, WithQueue("chainq")).
		Then(chainArgs{Step: 3}).
		Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("chain did not complete; order=%v", order)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Errorf("order = %v, want [1 2 3]", order)
	}
}

func TestChain_StopsOnDeadLetterAndResumesViaRunTask(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var failStep2 atomic.Bool
	failStep2.Store(true)
	var step3Runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[chainArgs]) error {
		switch task.Args.Step {
		case 2:
			if failStep2.Load() {
				return errors.New("step2 boom")
			}
		case 3:
			step3Runs.Add(1)
		}
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := NewChain().
		Then(chainArgs{Step: 1}).
		Then(chainArgs{Step: 2}, WithMaxRetry(0)). // 즉시 dead-letter
		Then(chainArgs{Step: 3}).
		Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	insp := NewInspector(client)
	var deadID string
	deadline := time.Now().Add(10 * time.Second)
	for deadID == "" {
		tasks, _ := insp.ListTasks(ctx, "default", "archived", 10)
		for _, ti := range tasks {
			if ti.ChainPending == 1 {
				deadID = ti.ID
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("step2 never dead-lettered")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // 잘못 이어졌다면 3단계가 돌 시간
	if n := step3Runs.Load(); n != 0 {
		t.Fatalf("step3 ran %d times despite chain break", n)
	}

	failStep2.Store(false)
	if err := insp.RunTask(ctx, "default", deadID); err != nil {
		t.Fatalf("run: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for step3Runs.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("chain did not resume after RunTask")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestChain_RetainedLinkCarriesNoStaleTail(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	done := make(chan struct{}, 3)
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[chainArgs]) error {
		done <- struct{}{}
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := NewChain().
		Then(chainArgs{Step: 1}, WithRetention(time.Hour)). // 보관되는 중간 링크
		Then(chainArgs{Step: 2}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// 두 링크 모두 실행 완료 대기.
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("chain did not complete")
		}
	}
	// 보관된 1단계는 꼬리를 갖지 않아야 한다 (이미 넘긴 링크를 광고/재생성 금지).
	insp := NewInspector(client)
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, gerr := insp.GetTask(ctx, "default", info.ID)
		if gerr == nil && got.State == "completed" {
			if got.ChainPending != 0 {
				t.Fatalf("retained link ChainPending = %d, want 0 (tail already handed off)", got.ChainPending)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("link1 not retained as completed (err=%v)", gerr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
