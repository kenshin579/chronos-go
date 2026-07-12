package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestGroup_BuilderValidation(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// 멤버 0개 → 에러.
	if _, err := NewGroup().OnComplete(emailArgs{UserID: "cb"}).Enqueue(ctx, c); err == nil {
		t.Error("no members: want error")
	}
	// OnComplete 누락 → 에러.
	if _, err := NewGroup().Add(emailArgs{UserID: "a"}).Enqueue(ctx, c); err == nil {
		t.Error("no callback: want error")
	}
	// 멤버의 WithTaskID/WithUnique → 에러.
	if _, err := NewGroup().Add(emailArgs{UserID: "a"}, WithTaskID("x")).
		OnComplete(emailArgs{UserID: "cb"}).Enqueue(ctx, c); err == nil {
		t.Error("WithTaskID member: want error")
	}
	if _, err := NewGroup().Add(emailArgs{UserID: "a"}, WithUnique(time.Minute)).
		OnComplete(emailArgs{UserID: "cb"}).Enqueue(ctx, c); err == nil {
		t.Error("WithUnique member: want error")
	}
	// 콜백의 WithProcessAt → 에러 (그룹 완료 기준 상대 지연만 허용).
	if _, err := NewGroup().Add(emailArgs{UserID: "a"}).
		OnComplete(emailArgs{UserID: "cb"}, WithProcessAt(time.Now().Add(time.Hour))).
		Enqueue(ctx, c); err == nil {
		t.Error("WithProcessAt callback: want error")
	}

	// 정상: GroupInfo 반환.
	info, err := NewGroup().
		Add(emailArgs{UserID: "m0"}, WithProcessIn(time.Hour)). // scheduled 멤버로 조회 가능하게
		Add(emailArgs{UserID: "m1"}, WithProcessIn(time.Hour)).
		OnComplete(emailArgs{UserID: "cb"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if info.GroupID == "" || len(info.MemberIDs) != 2 || info.CallbackID != info.GroupID+":cb" {
		t.Errorf("GroupInfo = %+v", info)
	}

	// Inspector 노출: 멤버의 GroupID + GroupPending(잔여 2).
	insp := NewInspector(client)
	got, err := insp.GetTask(ctx, "default", info.MemberIDs[0])
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GroupID != info.GroupID {
		t.Errorf("GroupID = %q, want %q", got.GroupID, info.GroupID)
	}
	if got.GroupPending != 2 {
		t.Errorf("GroupPending = %d, want 2", got.GroupPending)
	}
}

// groupArgs is a dedicated kind for group integration tests.
type groupArgs struct {
	N int `json:"n"`
}

func (groupArgs) Kind() string { return "test:groupmember" }

// groupCbArgs is the callback kind.
type groupCbArgs struct {
	Batch string `json:"batch"`
}

func (groupCbArgs) Kind() string { return "test:groupcb" }

func TestGroup_FanOutFiresCallbackOnce(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var members, callbacks atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[groupArgs]) error {
		members.Add(1)
		return nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[groupCbArgs]) error {
		callbacks.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1, "gq": 1},
		Concurrency: 8, // 동시 완료 경합 유도
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	g := NewGroup()
	for i := 0; i < 6; i++ {
		q := "default"
		if i%2 == 1 {
			q = "gq"
		}
		g.Add(groupArgs{N: i}, WithQueue(q))
	}
	if _, err := g.OnComplete(groupCbArgs{Batch: "b1"}).Enqueue(ctx, c); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for callbacks.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("callback never fired (members done=%d)", members.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // 중복 발화 시간 여유
	if n := callbacks.Load(); n != 1 {
		t.Errorf("callbacks = %d, want exactly 1", n)
	}
	if n := members.Load(); n != 6 {
		t.Errorf("members = %d, want 6", n)
	}
}

func TestGroup_StalledByDeadLetterResumesViaRunTask(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var failN2 atomic.Bool
	failN2.Store(true)
	var callbacks atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[groupArgs]) error {
		if task.Args.N == 2 && failN2.Load() {
			return errors.New("member 2 boom")
		}
		return nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[groupCbArgs]) error {
		callbacks.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 2})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := NewGroup().
		Add(groupArgs{N: 1}).
		Add(groupArgs{N: 2}, WithMaxRetry(0)). // 즉시 dead-letter
		OnComplete(groupCbArgs{Batch: "b2"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// 멤버 2가 archived로 갈 때까지 대기 → 그룹 대기(콜백 미발화).
	insp := NewInspector(client)
	deadID := info.MemberIDs[1]
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, gerr := insp.GetTask(ctx, "default", deadID)
		if gerr == nil && got.State == "archived" {
			if got.GroupID != info.GroupID || got.GroupPending != 1 {
				t.Fatalf("dead member group info wrong: %+v", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("member 2 never dead-lettered")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if callbacks.Load() != 0 {
		t.Fatal("callback fired despite stalled group")
	}

	// 원인 해소 → 재실행 → 그룹 재개, 콜백 발화.
	failN2.Store(false)
	if err := insp.RunTask(ctx, "default", deadID); err != nil {
		t.Fatalf("run: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for callbacks.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("group did not resume after RunTask")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
