package chronos

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
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

var errNoResultTestSentinel = errors.New("wf: deliberate failure")

// rdbCreateIfAbsentForTest replays the stage-creation call a redelivered
// predecessor would make (chain ID를 몰라도 되도록 archived/completed에서
// 콜백 ID를 찾는 대신, 콜백 retention 덕에 남아 있는 hash를 스캔해 group ID를
// 복원한다).
func rdbCreateIfAbsentForTest(ctx context.Context, client redis.UniversalClient, q string) (int, error) {
	r := rdb.NewRDB(client)
	var cbKey string
	iter := client.Scan(ctx, 0, "chronos:{"+q+"}:t:*:cb", 100).Iterator()
	for iter.Next(ctx) {
		cbKey = iter.Val()
	}
	if cbKey == "" {
		return -1, errors.New("callback hash not found (retention?)")
	}
	// "chronos:{q}:t:<chainID>:<i>:cb" → groupID "<chainID>:<i>", cbID 그대로
	id := strings.TrimPrefix(cbKey, "chronos:{"+q+"}:t:")
	groupID := strings.TrimSuffix(id, ":cb")
	st, err := r.CreateGroupIfAbsent(ctx, q, groupID, []string{groupID + ":m0"}, id)
	return int(st), err
}

type wfFlaky struct{}

func (wfFlaky) Kind() string { return "wf:flaky" }

// 멤버 dead-letter → 그룹 대기 → RunTask 재개 → 콜백·후속 스테이지까지 완주.
func TestThenGroup_MemberDeadLetterResumesChain(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var fail atomic.Bool
	fail.Store(true)
	var finalGot atomic.Pointer[string]

	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "p"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		return wfOut{V: task.Args.Res}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfFlaky]) (wfOut, error) {
		if fail.Load() {
			return wfOut{}, SkipRetry(errNoResultTestSentinel)
		}
		return wfOut{V: "recovered"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfMerge]) (wfOut, error) {
		rs, err := GroupResults[wfOut](task)
		if err != nil {
			return wfOut{}, err
		}
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

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	_, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "ok"}, WithQueue("wf")).
			Add(wfFlaky{}, WithQueue("wf")).
			OnComplete(wfMerge{}, WithQueue("wf"))).
		Then(wfDeploy{}, WithQueue("wf")).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}

	// flaky 멤버(스테이지 1, 멤버 1)가 dead-letter로 갈 때까지 대기 후 재개.
	insp := NewInspector(client)
	deadline := time.Now().Add(15 * time.Second)
	var flakyID string
	for time.Now().Before(deadline) && flakyID == "" {
		tasks, _ := insp.ListTasks(ctx, "wf", "archived", 10)
		for _, ti := range tasks {
			if ti.Kind == "wf:flaky" {
				flakyID = ti.ID
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if flakyID == "" {
		t.Fatal("flaky member never dead-lettered")
	}
	fail.Store(false)
	if err := insp.RunTask(ctx, "wf", flakyID); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(15 * time.Second)
	for finalGot.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if finalGot.Load() == nil {
		t.Fatal("chain did not resume to the final stage")
	}
	if got := *finalGot.Load(); got != "ok+recovered" {
		t.Fatalf("final = %q, want ok+recovered", got)
	}
}

// 선행 재전달 멱등성: 완료된 그룹 스테이지는 재생성되지 않는다(콜백 hash 펜스).
func TestThenGroup_RedeliveryDoesNotResurrectStage(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var encRuns atomic.Int64
	var cbRuns atomic.Int64
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "p"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		encRuns.Add(1)
		return wfOut{V: "e"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfMerge]) (wfOut, error) {
		cbRuns.Add(1)
		return wfOut{V: "m"}, nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	// 콜백에 retention을 줘 완료 후에도 hash가 남게 → 펜스가 살아 있는 창에서 검증.
	_, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "x"}, WithQueue("wf")).
			OnComplete(wfMerge{}, WithQueue("wf"), WithRetention(time.Minute))).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for cbRuns.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if cbRuns.Load() == 0 {
		t.Fatal("stage never completed")
	}

	// 선행(prep, 스테이지 0) 재전달을 시뮬레이션: 서버 내부 enqueueNext를 직접
	// 재호출하는 대신 rdb.CreateGroupIfAbsent가 Done을 반환하는지 확인한다
	// (server 경로는 이 상태에서 멤버를 만들지 않음 — Task 4의 state 분기).
	st, err := rdbCreateIfAbsentForTest(ctx, client, "wf")
	if err != nil {
		t.Fatal(err)
	}
	if st != 0 { // rdb.GroupStageDone
		t.Fatalf("completed stage must be fenced, state=%d", st)
	}
	time.Sleep(500 * time.Millisecond)
	if encRuns.Load() != 1 || cbRuns.Load() != 1 {
		t.Fatalf("stage re-ran: enc=%d cb=%d", encRuns.Load(), cbRuns.Load())
	}
}

// 펜스 소멸 후 재생성: 잔존 completed 멤버(재투입 no-op)를 드레인하면 SET이
// 비고 콜백이 재점화된다. (멤버 retention 1분, 콜백 무retention)
func TestThenGroup_DrainLingeringCompletedMember(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var cbRuns atomic.Int64
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfPrep]) (wfOut, error) {
		return wfOut{V: "p"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfEnc]) (wfOut, error) {
		return wfOut{V: "e"}, nil
	})
	AddHandlerR(mux, func(ctx context.Context, task *Task[wfMerge]) (wfOut, error) {
		cbRuns.Add(1)
		return wfOut{V: "m"}, nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	info, err := NewChain().
		Then(wfPrep{}, WithQueue("wf")).
		ThenGroup(NewGroup().
			Add(wfEnc{Res: "x"}, WithQueue("wf"), WithRetention(time.Minute)).
			OnComplete(wfMerge{}, WithQueue("wf"))).
		Enqueue(ctx, NewClient(client))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for cbRuns.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if cbRuns.Load() == 0 {
		t.Fatal("stage never completed")
	}
	time.Sleep(500 * time.Millisecond) // 콜백(무retention) hash 삭제 대기

	// 선행 재전달의 스테이지 재생성을 재현: 펜스·SET 모두 없음 → Created.
	chainID := strings.TrimSuffix(info.ID, ":0")
	groupID := chainID + ":1"
	memberID := groupID + ":m0"
	r := rdb.NewRDB(client)
	st, err := r.CreateGroupIfAbsent(ctx, "wf", groupID, []string{memberID}, groupID+":cb")
	if err != nil || st != rdb.GroupStageCreated {
		t.Fatalf("recreate: st=%v err=%v", st, err)
	}
	// 잔존 completed 멤버는 create-if-absent에 걸린다 — 드레인 경로 직접 호출.
	if err := srv.drainCompletedStageMember(ctx, "wf", memberID); err != nil {
		t.Fatalf("drain: %v", err)
	}
	// SET이 비어 삭제되고 콜백 hash가 재생성됐는지.
	if n, _ := client.Exists(ctx, base.GroupKey("wf", groupID)).Result(); n != 0 {
		t.Error("pending set must be drained away")
	}
	if n, _ := client.Exists(ctx, base.TaskKey("wf", groupID+":cb")).Result(); n != 1 {
		t.Error("callback must be re-fired by the drained report")
	}
}

// 부분 완료 후 스테이지 재-enqueue: 완료된 멤버(m0)는 스킵되고, 미완료 멤버(m1)도
// create-if-absent라 재실행되지 않는다(no-op). 완료 멤버가 pending SET을 떠난 뒤
// 남은 멤버만 pending으로 유지되고 콜백은 아직 점화되지 않음을 좁게 검증한다.
func TestThenGroup_PartialCompletionReenqueueSkips(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	// 워커는 띄우지 않는다(Start 없음) — 서버 비공개 경로만 결정적으로 구동.
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"wf": 1}, Concurrency: 1})
	r := rdb.NewRDB(client)

	link := base.ChainLink{
		Kind: "wf:merge", Queue: "wf",
		Group: []base.GroupMemberLink{
			{Kind: "wf:enc", Queue: "wf"},
			{Kind: "wf:enc", Queue: "wf"},
		},
	}
	msg := &base.TaskMessage{
		ChainID: "cP", ChainIndex: 0,
		Chain:  []base.ChainLink{link},
		Result: []byte(`{"v":"p"}`),
	}
	groupID := "cP:1"
	m0, m1 := groupID+":m0", groupID+":m1"

	// 최초 스테이지 생성 + 멤버 2건 create-if-absent enqueue.
	if err := srv.enqueueNextGroup(ctx, msg, link); err != nil {
		t.Fatal(err)
	}
	if n, _ := client.SCard(ctx, base.GroupKey("wf", groupID)).Result(); n != 2 {
		t.Fatalf("initial pending = %d, want 2", n)
	}

	// m0만 완료(SREM). 저장된 메시지에 결과를 실어 CompleteGroupMember 호출.
	stored, err := r.GetTask(ctx, "wf", m0)
	if err != nil {
		t.Fatal(err)
	}
	stored.Result = []byte(`{"v":"e0"}`)
	if fired, err := r.CompleteGroupMember(ctx, stored); err != nil || fired {
		t.Fatalf("complete m0: fired=%v err=%v (m1 남아 있으면 점화 금지)", fired, err)
	}
	// 이제 pending SET에는 m1만.
	if members, _ := r.GroupMembers(ctx, "wf", groupID); len(members) != 1 || members[0] != m1 {
		t.Fatalf("after m0 complete, pending = %v, want [%s]", members, m1)
	}

	// 선행 재전달로 스테이지 재-enqueue: 완료 멤버(m0) 스킵, 미완료 멤버(m1) no-op.
	if err := srv.enqueueNextGroup(ctx, msg, link); err != nil {
		t.Fatalf("re-enqueue: %v", err)
	}

	// m0는 SET에 되살아나지 않고, m1만 pending으로 유지된다.
	members, _ := r.GroupMembers(ctx, "wf", groupID)
	if len(members) != 1 || members[0] != m1 {
		t.Fatalf("re-enqueue가 pending set을 %v로 바꿈, want [%s] (m0 스킵·m1 no-op)", members, m1)
	}
	// 콜백은 아직 점화되지 않았다(m1 미완료).
	if n, _ := client.Exists(ctx, base.TaskKey("wf", groupID+":cb")).Result(); n != 0 {
		t.Error("callback must not fire while m1 is still pending")
	}
}
