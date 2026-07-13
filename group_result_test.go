package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type gmArgs struct {
	I int `json:"i"`
}

func (gmArgs) Kind() string { return "gr:member" }

type gmSilentArgs struct{}

func (gmSilentArgs) Kind() string { return "gr:silent" }

type gmCbArgs struct{}

func (gmCbArgs) Kind() string { return "gr:cb" }

type gmOut struct {
	Sq int `json:"sq"`
}

// 멤버 3(결과 2 + 무결과 1) → 콜백이 Add 순서로 결과 수신.
func TestGroup_CallbackReceivesOrderedResults(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	type recv struct {
		raw [][]byte
	}
	var got atomic.Pointer[recv]
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[gmArgs]) (gmOut, error) {
		return gmOut{Sq: task.Args.I * task.Args.I}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[gmSilentArgs]) error { return nil })
	AddHandler(mux, func(ctx context.Context, task *Task[gmCbArgs]) error {
		got.Store(&recv{raw: task.RawGroupResults()})
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"gr": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	c := NewClient(client)
	_, err := NewGroup().
		Add(gmArgs{I: 2}, WithQueue("gr")).   // idx 0 → {"sq":4}
		Add(gmSilentArgs{}, WithQueue("gr")). // idx 1 → nil
		Add(gmArgs{I: 3}, WithQueue("gr")).   // idx 2 → {"sq":9}
		OnComplete(gmCbArgs{}, WithQueue("gr")).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for got.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	r := got.Load()
	if r == nil {
		t.Fatal("callback never ran")
	}
	if len(r.raw) != 3 || string(r.raw[0]) != `{"sq":4}` || r.raw[1] != nil || string(r.raw[2]) != `{"sq":9}` {
		t.Fatalf("raw results = %v", r.raw)
	}
}

type gmFlakyArgs struct {
	I int `json:"i"`
}

func (gmFlakyArgs) Kind() string { return "gr:flaky" }

// dead-letter로 정지한 그룹을 RunTask로 재개하면, 재실행 멤버의 결과까지
// 포함해 콜백이 전체 결과를 받는다.
func TestGroup_ResumedMemberResultReachesCallback(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var fail atomic.Bool
	fail.Store(true)
	var got atomic.Pointer[[][]byte]
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[gmArgs]) (gmOut, error) {
		return gmOut{Sq: task.Args.I * task.Args.I}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[gmFlakyArgs]) (gmOut, error) {
		if fail.Load() {
			return gmOut{}, SkipRetry(errors.New("first pass fails"))
		}
		return gmOut{Sq: task.Args.I * 100}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[gmCbArgs]) error {
		raw := task.RawGroupResults()
		got.Store(&raw)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"gr2": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())
	c := NewClient(client)

	info, err := NewGroup().
		Add(gmArgs{I: 2}, WithQueue("gr2")).      // idx 0 → {"sq":4}
		Add(gmFlakyArgs{I: 3}, WithQueue("gr2")). // idx 1 → dead-letter 후 재개 시 {"sq":300}
		OnComplete(gmCbArgs{}, WithQueue("gr2")).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	// flaky 멤버가 dead-letter로 갈 때까지 대기.
	insp := NewInspector(client)
	flakyID := info.MemberIDs[1]
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ti, err := insp.GetTask(ctx, "gr2", flakyID); err == nil && ti.State == "archived" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	fail.Store(false)
	if err := insp.RunTask(ctx, "gr2", flakyID); err != nil {
		t.Fatalf("run task: %v", err)
	}

	// RunTask 재개 직후 데드라인을 재설정해, archived 대기 루프가 시간을 얼마나
	// 소진했든 콜백 대기가 온전한 예산(10초)을 갖도록 한다(진단 정확도).
	deadline = time.Now().Add(10 * time.Second)
	for got.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got.Load() == nil {
		t.Fatal("callback never ran after resume")
	}
	raw := *got.Load()
	if len(raw) != 2 || string(raw[0]) != `{"sq":4}` || string(raw[1]) != `{"sq":300}` {
		t.Fatalf("resumed results = %v", raw)
	}
}
