package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type mcOut struct {
	V string `json:"v"`
}

// 그룹-of-체인: 각 멤버가 dump→xform→load 3링크 체인, 마지막(load)만 부모에
// 보고. 콜백이 멤버별 최종 결과를 Add 순서로 수신.
func TestGroupMemberChain_FanOutOfChains(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var loadRuns, dumpRuns atomic.Int64
	var cbResults atomic.Pointer[[]string]

	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcDump]) (mcOut, error) {
		dumpRuns.Add(1)
		return mcOut{V: "d:" + task.Args.T}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcXform]) (mcOut, error) {
		prev, _ := PrevResult[mcOut](task) // 체인 내부 릴레이 확인
		return mcOut{V: "x(" + prev.V + ")"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcLoad]) (mcOut, error) {
		loadRuns.Add(1)
		prev, _ := PrevResult[mcOut](task)
		return mcOut{V: "l(" + prev.V + ")"}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[mcVerify]) error {
		rs, err := GroupResults[mcOut](task)
		if err != nil {
			return err
		}
		vs := make([]string, len(rs))
		for i, r := range rs {
			vs[i] = r.V
		}
		cbResults.Store(&vs)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"mc": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	g := NewGroup()
	for _, tenant := range []string{"a", "b"} {
		g.AddChain(NewChain().
			Then(mcDump{T: tenant}, WithQueue("mc")).
			Then(mcXform{T: tenant}, WithQueue("mc")).
			Then(mcLoad{T: tenant}, WithQueue("mc")))
	}
	if _, err := g.OnComplete(mcVerify{}, WithQueue("mc")).Enqueue(ctx, NewClient(client)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for cbResults.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	rs := cbResults.Load()
	if rs == nil {
		t.Fatal("callback never ran (member chains did not report)")
	}
	// Add 순서: 멤버0(a)·멤버1(b), 각자 full chain 결과.
	if len(*rs) != 2 || (*rs)[0] != "l(x(d:a))" || (*rs)[1] != "l(x(d:b))" {
		t.Fatalf("group results = %v", *rs)
	}
	// 각 체인의 dump/load는 정확히 1회(중간 링크가 조기 보고하지 않음).
	if dumpRuns.Load() != 2 || loadRuns.Load() != 2 {
		t.Errorf("runs: dump=%d load=%d (want 2/2)", dumpRuns.Load(), loadRuns.Load())
	}
}

// 멤버 체인 중간 링크 dead-letter → RunTask 재개 → 그룹 완주.
func TestGroupMemberChain_MidLinkDeadLetterResumes(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var fail atomic.Bool
	fail.Store(true)
	var done atomic.Bool

	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcDump]) (mcOut, error) {
		return mcOut{V: "d"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcXform]) (mcOut, error) {
		if fail.Load() {
			return mcOut{}, SkipRetry(errors.New("xform down"))
		}
		return mcOut{V: "x"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcLoad]) (mcOut, error) {
		return mcOut{V: "l"}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[mcVerify]) error {
		done.Store(true)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"mc": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	_, err := NewGroup().
		AddChain(NewChain().Then(mcDump{}, WithQueue("mc")).Then(mcXform{}, WithQueue("mc")).Then(mcLoad{}, WithQueue("mc"))).
		OnComplete(mcVerify{}, WithQueue("mc")).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}

	// xform 링크(멤버 슬롯 :m0의 링크 1)가 dead-letter될 때까지 대기 후 재개.
	insp := NewInspector(client)
	deadline := time.Now().Add(15 * time.Second)
	var xformID string
	for time.Now().Before(deadline) && xformID == "" {
		tasks, _ := insp.ListTasks(ctx, "mc", "archived", 10)
		for _, ti := range tasks {
			if ti.Kind == "mc:xform" {
				xformID = ti.ID
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if xformID == "" {
		t.Fatal("xform never dead-lettered")
	}
	fail.Store(false)
	if err := insp.RunTask(ctx, "mc", xformID); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for !done.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !done.Load() {
		t.Fatal("group did not resume to callback after RunTask")
	}
}

type mcFlat struct{}

func (mcFlat) Kind() string { return "mc:flat" }

// flat 멤버 + 체인 멤버 혼용: 콜백이 Add 순서로 두 결과를 받는다(멤버0=flat,
// 멤버1=2링크 체인). flat 멤버는 GroupMemberID 폴백(자기 ID), 체인 멤버는 슬롯.
func TestGroupMemberChain_MixedFlatAndChain(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var got atomic.Pointer[[]string]
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcFlat]) (mcOut, error) {
		return mcOut{V: "flat"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcDump]) (mcOut, error) {
		return mcOut{V: "d"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[mcLoad]) (mcOut, error) {
		prev, _ := PrevResult[mcOut](task)
		return mcOut{V: "l(" + prev.V + ")"}, nil
	})
	AddHandler(mux, func(ctx context.Context, task *Task[mcVerify]) error {
		rs, err := GroupResults[mcOut](task)
		if err != nil {
			return err
		}
		vs := make([]string, len(rs))
		for i, r := range rs {
			vs[i] = r.V
		}
		got.Store(&vs)
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"mc": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	_, err := NewGroup().
		Add(mcFlat{}, WithQueue("mc")). // 멤버0: flat 단일 태스크
		AddChain(NewChain().            // 멤버1: 2링크 체인
						Then(mcDump{}, WithQueue("mc")).
						Then(mcLoad{}, WithQueue("mc"))).
		OnComplete(mcVerify{}, WithQueue("mc")).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for got.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	rs := got.Load()
	if rs == nil {
		t.Fatal("callback never ran (mixed members)")
	}
	if len(*rs) != 2 || (*rs)[0] != "flat" || (*rs)[1] != "l(d)" {
		t.Fatalf("group results = %v, want [flat l(d)]", *rs)
	}
}
