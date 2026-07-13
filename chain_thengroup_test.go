package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type wfOut struct {
	V string `json:"v"`
}

// 팬아웃→팬인→후속: prep 결과가 전 멤버에 복제되고, 콜백이 멤버 결과를 Add
// 순서로 받고, 콜백 결과가 마지막 스테이지로 릴레이된다.
func TestThenGroup_FanOutFanInContinue(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var memberPrev [2]atomic.Pointer[string] // 각 멤버가 받은 PrevResult.V
	var cbGot atomic.Pointer[[]wfOut]
	var finalGot atomic.Pointer[string]

	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "prepared"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		prev, err := PrevResult[wfOut](task)
		if err == nil {
			idx := 0
			if task.Args.Res == "4k" {
				idx = 1
			}
			v := prev.V
			memberPrev[idx].Store(&v)
		}
		return wfOut{V: task.Args.Res}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfMerge]) (wfOut, error) {
		rs, err := GroupResults[wfOut](task)
		if err != nil {
			return wfOut{}, err
		}
		cbGot.Store(&rs)
		return wfOut{V: rs[0].V + "+" + rs[1].V}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[wfDeploy]) error {
		prev, err := PrevResult[wfOut](task)
		if err != nil {
			return err
		}
		finalGot.Store(&prev.V)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1, "enc": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	info, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "720p"}, WithQueue("enc")).
			Add(wfEnc{Res: "4k"}, WithQueue("enc")).
			OnComplete(wfMerge{}, WithQueue("wf"))).
		Then(wfDeploy{}, WithQueue("wf")).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	if info.ID == "" {
		t.Fatal("first link TaskInfo missing")
	}

	deadline := time.Now().Add(15 * time.Second)
	for finalGot.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if finalGot.Load() == nil {
		t.Fatal("workflow never reached the final stage")
	}
	if got := *finalGot.Load(); got != "720p+4k" {
		t.Fatalf("final PrevResult = %q, want 720p+4k", got)
	}
	for i := 0; i < 2; i++ {
		if p := memberPrev[i].Load(); p == nil || *p != "prepared" {
			t.Errorf("member %d PrevResult = %v, want prepared", i, p)
		}
	}
	if rs := cbGot.Load(); rs == nil || (*rs)[0].V != "720p" || (*rs)[1].V != "4k" {
		t.Errorf("callback results = %v", cbGot.Load())
	}
}

// 그룹 스테이지가 마지막이어도 동작한다(콜백이 마지막 링크).
func TestThenGroup_AsLastStage(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var done atomic.Bool
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "p"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		return wfOut{V: task.Args.Res}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[wfMerge]) error {
		if raw := task.RawGroupResults(); len(raw) == 2 {
			done.Store(true)
		}
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	_, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "a"}, WithQueue("wf")).
			Add(wfEnc{Res: "b"}, WithQueue("wf")).
			OnComplete(wfMerge{}, WithQueue("wf"))).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for !done.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !done.Load() {
		t.Fatal("last-stage group callback never ran")
	}
}
