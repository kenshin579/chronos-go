# 큐 일시정지/재개 + 스케줄 레지스트리 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 큐 소비를 일시정지/재개(Inspector/CLI/UI)하고, 등록된 전체 스케줄을 레지스트리로 노출한다 (webui Phase 2).

**Architecture:** pause = 전역 SET `chronos:paused` + fetchLoop의 1초 캐시(paused 큐를 라운드에서 제외). 레지스트리 = 전역 HASH `chronos:schedules`(멱등 HSET, 기존 renew 틱에서 last_seen 갱신, stale 판정 60s). 둘 다 단일 키 명령만 — cluster-safe, 신규 Lua 없음.

**Tech Stack:** Go, redis/go-redis, 실제 Redis(DB 15, `-p 1`), docker cluster(스모크 17번째).

---

## File Structure

- Modify `internal/base/keys.go` — `PausedKey`/`SchedulesKey`. Test: `internal/base/task_test.go`
- Create `internal/rdb/pause.go` — Pause/Resume/PausedQueues. `internal/rdb/registry.go` — RegisterSchedules/TouchSchedules/ListSchedules + `ScheduleMeta`. Tests: `internal/rdb/pause_test.go`, `registry_test.go`
- Modify `server.go` — fetchLoop paused 캐시·제외. Test: `server_pause_test.go`(신규)
- Modify `scheduler.go` — scheduleEntry에 `spec`, Start 시 등록, renew 틱에서 touch
- Modify `inspector.go` — PauseQueue/ResumeQueue/PausedQueues, QueueInfo.Paused, ScheduleInfo 확장+병합. `chronos.go` — 필드. Test: `inspector_test.go`, `scheduler_integration_test.go`
- Modify `cmd/chronos/main.go` — queue pause/resume + PAUSED 컬럼. Test: `cmd/chronos/main_test.go`
- Modify `contrib/webui` — 토글 라우트/버튼/배지, /api/stats paused, 스케줄러 페이지 컬럼. Test: `webui_test.go`
- Modify `cluster_test.go` — 17번째. `examples/tour/main.go` — 섹션 14. `README.md`.

**구현자 참고:**
- fetchLoop의 order 구성부: `server.go` `order = order[:0]` 블록 — paused 제외는 order 구성 직후 필터링이 최소 침습.
- scheduler의 renew 틱: `run()`의 `case <-renew.C: s.tryElect(ctx)` — 여기에 touch 추가. `scheduleEntry`(scheduler.go:33)에 `spec` 필드 추가 필요(현재 id에만 포함).
- `register(spec string, ...)`(scheduler.go:81)가 spec 원문을 받음.
- Inspector `SchedulerStatus`/`ScheduleInfo`는 inspector.go에 있음(v2에서 추가). `ScanSchedules`는 internal/rdb/inspect.go.
- webui: `action` 헬퍼(Origin 가드), `statsQueue`, 큐 상세 템플릿, `scheduler.html`.

---

## Task 1: base 키 + rdb pause/registry

**Files:**
- Modify: `internal/base/keys.go`
- Create: `internal/rdb/pause.go`, `internal/rdb/registry.go`
- Test: `internal/base/task_test.go`, `internal/rdb/pause_test.go`, `internal/rdb/registry_test.go`

- [ ] **Step 1: 실패 테스트**

`internal/base/task_test.go`에:
```go
func TestGlobalKeys(t *testing.T) {
	if PausedKey() != "chronos:paused" {
		t.Errorf("PausedKey = %q", PausedKey())
	}
	if SchedulesKey() != "chronos:schedules" {
		t.Errorf("SchedulesKey = %q", SchedulesKey())
	}
}
```

`internal/rdb/pause_test.go` 신규:
```go
package rdb

import (
	"context"
	"testing"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestPauseResumeQueues(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if paused, _ := r.PausedQueues(ctx); len(paused) != 0 {
		t.Fatalf("initial paused = %v, want empty", paused)
	}
	if err := r.PauseQueue(ctx, "critical"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if err := r.PauseQueue(ctx, "critical"); err != nil { // 멱등
		t.Fatalf("pause twice: %v", err)
	}
	paused, err := r.PausedQueues(ctx)
	if err != nil || len(paused) != 1 || paused[0] != "critical" {
		t.Fatalf("paused = %v err=%v", paused, err)
	}
	if err := r.ResumeQueue(ctx, "critical"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if paused, _ := r.PausedQueues(ctx); len(paused) != 0 {
		t.Fatalf("after resume = %v, want empty", paused)
	}
}
```

`internal/rdb/registry_test.go` 신규:
```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestScheduleRegistry(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	metas := []ScheduleMeta{
		{ID: "job-a#1", Kind: "report:daily", Queue: "default", Spec: "0 0 * * *"},
		{ID: "job-b#2", Kind: "health:ping", Queue: "ops", Spec: "@every 30s"},
	}
	if err := r.RegisterSchedules(ctx, metas); err != nil {
		t.Fatalf("register: %v", err)
	}
	// 멱등 재등록 + touch.
	if err := r.RegisterSchedules(ctx, metas[:1]); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	before := time.Now().Add(-time.Minute).Unix()
	if err := r.TouchSchedules(ctx, []string{"job-a#1"}); err != nil {
		t.Fatalf("touch: %v", err)
	}
	got, err := r.ListSchedules(ctx)
	if err != nil || len(got) != 2 {
		t.Fatalf("list = %v err=%v, want 2", got, err)
	}
	byID := map[string]ScheduleMeta{}
	for _, m := range got {
		byID[m.ID] = m
	}
	a := byID["job-a#1"]
	if a.Kind != "report:daily" || a.Spec != "0 0 * * *" || a.LastSeen < before {
		t.Errorf("job-a = %+v", a)
	}
	if byID["job-b#2"].Queue != "ops" {
		t.Errorf("job-b = %+v", byID["job-b#2"])
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/base/ -run TestGlobalKeys && go test ./internal/rdb/ -run 'TestPauseResume|TestScheduleRegistry' -p 1`
Expected: FAIL (undefined).

- [ ] **Step 3: 구현**

`internal/base/keys.go`에 (QueuesKey 근처):
```go
// PausedKey is the SET key listing paused queue names. Global (no hash tag):
// only single-key commands touch it, so it is cluster-safe.
func PausedKey() string { return "chronos:paused" }

// SchedulesKey is the HASH key holding the registry of known schedules
// (field = deterministic schedule ID, value = JSON metadata). Global single
// key, single-key commands only — cluster-safe.
func SchedulesKey() string { return "chronos:schedules" }
```

`internal/rdb/pause.go`:
```go
package rdb

import (
	"context"

	"github.com/kenshin579/chronos-go/internal/base"
)

// PauseQueue marks a queue paused: servers stop consuming it (enqueueing,
// forwarding and recovery continue, so work accumulates as pending). Idempotent.
func (r *RDB) PauseQueue(ctx context.Context, qname string) error {
	return r.client.SAdd(ctx, base.PausedKey(), qname).Err()
}

// ResumeQueue lifts a pause. Idempotent.
func (r *RDB) ResumeQueue(ctx context.Context, qname string) error {
	return r.client.SRem(ctx, base.PausedKey(), qname).Err()
}

// PausedQueues lists currently paused queue names.
func (r *RDB) PausedQueues(ctx context.Context) ([]string, error) {
	return r.client.SMembers(ctx, base.PausedKey()).Result()
}
```

`internal/rdb/registry.go`:
```go
package rdb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
)

// ScheduleMeta is one registered schedule's registry entry.
type ScheduleMeta struct {
	ID           string `json:"-"` // hash field, not part of the JSON value
	Kind         string `json:"kind"`
	Queue        string `json:"queue"`
	Spec         string `json:"spec"`
	RegisteredAt int64  `json:"registered_at"` // unix seconds (last registration)
	LastSeen     int64  `json:"last_seen"`     // unix seconds (scheduler heartbeat)
}

// RegisterSchedules upserts the given schedules into the registry. Schedule
// IDs are deterministic, so concurrent registration from multiple instances
// overwrites identical data — idempotent by construction.
func (r *RDB) RegisterSchedules(ctx context.Context, metas []ScheduleMeta) error {
	if len(metas) == 0 {
		return nil
	}
	now := time.Now().Unix()
	pairs := make([]interface{}, 0, len(metas)*2)
	for _, m := range metas {
		m.RegisteredAt, m.LastSeen = now, now
		v, err := json.Marshal(m)
		if err != nil {
			return err
		}
		pairs = append(pairs, m.ID, string(v))
	}
	return r.client.HSet(ctx, base.SchedulesKey(), pairs...).Err()
}

// TouchSchedules refreshes last_seen for the given schedule IDs (scheduler
// heartbeat). Unknown IDs are skipped.
func (r *RDB) TouchSchedules(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	vals, err := r.client.HMGet(ctx, base.SchedulesKey(), ids...).Result()
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	pairs := make([]interface{}, 0, len(ids)*2)
	for i, raw := range vals {
		s, ok := raw.(string)
		if !ok {
			continue
		}
		var m ScheduleMeta
		if json.Unmarshal([]byte(s), &m) != nil {
			continue
		}
		m.LastSeen = now
		v, _ := json.Marshal(m)
		pairs = append(pairs, ids[i], string(v))
	}
	if len(pairs) == 0 {
		return nil
	}
	return r.client.HSet(ctx, base.SchedulesKey(), pairs...).Err()
}

// ListSchedules returns every registry entry.
func (r *RDB) ListSchedules(ctx context.Context) ([]ScheduleMeta, error) {
	all, err := r.client.HGetAll(ctx, base.SchedulesKey()).Result()
	if err != nil {
		return nil, err
	}
	out := make([]ScheduleMeta, 0, len(all))
	for id, raw := range all {
		var m ScheduleMeta
		if json.Unmarshal([]byte(raw), &m) != nil {
			continue // foreign value shape; skip
		}
		m.ID = id
		out = append(out, m)
	}
	return out, nil
}
```

- [ ] **Step 4: 통과 + 커밋**

Run: `go test ./internal/base/ ./internal/rdb/ -p 1` → PASS.
```bash
git add internal/base/ internal/rdb/
git commit -m "feat: rdb pause(SET)·스케줄 레지스트리(HASH) — 전역 단일 키, cluster-safe"
```

---

## Task 2: 서버 — fetchLoop paused 제외 (1s 캐시)

**Files:**
- Modify: `server.go`
- Test: `server_pause_test.go` (신규)

- [ ] **Step 1: 실패 테스트**

`server_pause_test.go`:
```go
package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_PauseStopsConsumptionResumeRestarts(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	insp := NewInspector(client)

	var done atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		done.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 2})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	// pause 후 캐시 반영 여유(1s+)를 두고 enqueue → 소비되지 않아야 한다.
	if err := insp.PauseQueue(ctx, "default"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	time.Sleep(1500 * time.Millisecond) // pause cache refresh
	for i := 0; i < 3; i++ {
		if _, err := Enqueue(ctx, c, emailArgs{UserID: "p"}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	time.Sleep(2 * time.Second)
	if n := done.Load(); n != 0 {
		t.Fatalf("consumed %d tasks while paused, want 0", n)
	}

	// resume → 쌓인 3개 소비.
	if err := insp.ResumeQueue(ctx, "default"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for done.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("resume did not restart consumption (done=%d)", done.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestServer_PauseOneQueueOthersContinue(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	insp := NewInspector(client)

	var a, b atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[chainArgs]) error {
		if task.Args.Step == 1 {
			a.Add(1)
		} else {
			b.Add(1)
		}
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"qa": 1, "qb": 1}, Concurrency: 2})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if err := insp.PauseQueue(ctx, "qa"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, err := Enqueue(ctx, c, chainArgs{Step: 1}, WithQueue("qa")); err != nil {
		t.Fatalf("enqueue qa: %v", err)
	}
	if _, err := Enqueue(ctx, c, chainArgs{Step: 2}, WithQueue("qb")); err != nil {
		t.Fatalf("enqueue qb: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for b.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("unpaused queue qb was not consumed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if a.Load() != 0 {
		t.Errorf("paused queue qa consumed %d", a.Load())
	}
}
```
(NOTE: `Inspector.PauseQueue`는 Task 3에서 추가 — 이 테스트는 Task 3 후에 컴파일됨. **Task 2에서는 rdb를 직접 써서** `rdb.NewRDB(client).PauseQueue(...)`로 작성하고, Task 3에서 Inspector로 바꾸지 말고 그대로 둔다 — 간단히: 테스트에서 `internal/rdb` import해 `r := rdb.NewRDB(client)` 사용. 위 코드의 `insp.PauseQueue`를 `r.PauseQueue`로 대체해 작성하라.)

- [ ] **Step 2: 실패 확인**

Run: `go test . -run TestServer_Pause -p 1` → FAIL (pause가 소비를 안 멈춤 — done=3).

- [ ] **Step 3: 구현 (server.go)**

(a) 상수: `pollBlock` 근처에
```go
// pauseCheckInterval bounds how often fetchLoop refreshes the paused-queue
// set — pause/resume take effect within about this long.
const pauseCheckInterval = time.Second
```
(b) fetchLoop 로컬 상태 + 헬퍼: fetchLoop 시작부(byWeight/picker 선언 근처)에
```go
	var (
		paused        map[string]bool
		pausedChecked time.Time
	)
	refreshPaused := func() {
		if time.Since(pausedChecked) < pauseCheckInterval {
			return
		}
		pausedChecked = time.Now()
		names, err := s.rdb.PausedQueues(ctx)
		if err != nil {
			// A pause-lookup hiccup must not stop consumption; keep the last
			// known set and try again next interval.
			if ctx.Err() == nil {
				s.logger.Error("chronos: paused-queue lookup failed", "error", err)
			}
			return
		}
		set := make(map[string]bool, len(names))
		for _, q := range names {
			set[q] = true
		}
		paused = set
	}
```
(c) order 구성 직후(배치 계산 전) 필터:
```go
		refreshPaused()
		if len(paused) > 0 {
			kept := order[:0]
			for _, q := range order {
				if !paused[q] {
					kept = append(kept, q)
				}
			}
			order = kept
			if len(order) == 0 {
				// Every configured queue is paused — idle briefly, release the
				// held slot, and re-check.
				<-s.sem
				select {
				case <-time.After(pauseCheckInterval):
				case <-ctx.Done():
					return
				}
				continue
			}
		}
```
주의: 이 시점은 sem 슬롯을 이미 보유한 상태인지 확인(fetchLoop 구조상 sem 획득 → order 구성 순서) — 보유 중이면 위처럼 `<-s.sem` 해제 후 continue. 실제 코드 순서를 읽고 맞출 것.

- [ ] **Step 4: 통과 + 회귀 + 커밋**

Run: `go test . -run TestServer_Pause -p 1 -race` → PASS. `make check` → PASS (기존 우선순위/체인/그룹 테스트 무회귀 — paused가 비면 order 무변).
```bash
git add server.go server_pause_test.go
git commit -m "feat: fetchLoop paused 큐 제외 (1s 캐시 — 소비만 중단)"
```

---

## Task 3: Inspector + CLI

**Files:**
- Modify: `inspector.go`, `chronos.go`(QueueInfo.Paused), `internal/rdb/inspect.go`(불필요 — pause.go에 이미), `cmd/chronos/main.go`
- Test: `inspector_test.go`, `cmd/chronos/main_test.go`

- [ ] **Step 1: 실패 테스트**

`inspector_test.go`:
```go
func TestInspector_PauseResumeAndPausedFlag(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	insp := NewInspector(client)

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "x"}, WithProcessIn(time.Hour)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := insp.PauseQueue(ctx, "default"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	qs, err := insp.Queues(ctx)
	if err != nil || len(qs) == 0 {
		t.Fatalf("queues: %v", err)
	}
	if !qs[0].Paused {
		t.Error("QueueInfo.Paused = false, want true")
	}
	paused, err := insp.PausedQueues(ctx)
	if err != nil || len(paused) != 1 {
		t.Fatalf("paused = %v err=%v", paused, err)
	}
	if err := insp.ResumeQueue(ctx, "default"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	qs, _ = insp.Queues(ctx)
	if qs[0].Paused {
		t.Error("still paused after resume")
	}
}
```
`cmd/chronos/main_test.go`:
```go
func TestRun_QueuePauseResume(t *testing.T) {
	client := testutil.NewRedis(t)
	c := chronos.NewClient(client)
	defer c.Close()
	if _, err := chronos.Enqueue(context.Background(), c, greetArgs{Name: "x"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	var out bytes.Buffer
	if code := run([]string{"queue", "pause", "default"}, client, &out); code != 0 {
		t.Fatalf("pause exit = %d: %s", code, out.String())
	}
	out.Reset()
	if code := run([]string{"queue", "ls"}, client, &out); code != 0 {
		t.Fatalf("ls exit = %d", code)
	}
	if !strings.Contains(out.String(), "PAUSED") || !strings.Contains(out.String(), "yes") {
		t.Errorf("queue ls missing paused flag:\n%s", out.String())
	}
	out.Reset()
	if code := run([]string{"queue", "resume", "default"}, client, &out); code != 0 {
		t.Fatalf("resume exit = %d", code)
	}
}
```

- [ ] **Step 2: 실패 확인** → FAIL (undefined).

- [ ] **Step 3: 구현**

inspector.go:
```go
// PauseQueue stops servers from consuming the queue (within about one second).
// Enqueueing, forwarding and recovery continue — work accumulates as pending.
func (i *Inspector) PauseQueue(ctx context.Context, qname string) error {
	return i.rdb.PauseQueue(ctx, qname)
}

// ResumeQueue lifts a pause; consumption restarts within about one second.
func (i *Inspector) ResumeQueue(ctx context.Context, qname string) error {
	return i.rdb.ResumeQueue(ctx, qname)
}

// PausedQueues lists currently paused queue names.
func (i *Inspector) PausedQueues(ctx context.Context) ([]string, error) {
	return i.rdb.PausedQueues(ctx)
}
```
`Queues()`에서 SMEMBERS 1회로 `Paused` 채움 (기존 루프 전에 `paused, _ := i.rdb.PausedQueues(ctx)` → set → 매핑 시 `Paused: set[name]`). chronos.go `QueueInfo`에 `Paused bool` 추가.

cmd/chronos/main.go: doc comment/usage에 `queue pause|resume <queue>` 추가, `run`의 queue 분기:
```go
	case "queue":
		if len(args) >= 2 {
			switch args[1] {
			case "ls":
				return queueLs(ctx, insp, out)
			case "pause", "resume":
				if len(args) < 3 {
					fmt.Fprintf(out, "usage: chronos queue %s <queue>\n", args[1])
					return 2
				}
				var err error
				if args[1] == "pause" {
					err = insp.PauseQueue(ctx, args[2])
				} else {
					err = insp.ResumeQueue(ctx, args[2])
				}
				if err != nil {
					fmt.Fprintf(out, "error: %v\n", err)
					return 1
				}
				fmt.Fprintf(out, "%sd %s\n", args[1], args[2])
				return 0
			}
		}
```
(기존 구조에 맞춰 조정.) `queueLs` 헤더에 `\tPAUSED`, 행에 `yes`/`-`.

- [ ] **Step 4: 통과 + 커밋**

Run: `go test . -run TestInspector_PauseResume -p 1 && go test ./cmd/chronos/ -p 1` → PASS.
```bash
git add inspector.go chronos.go cmd/chronos/ inspector_test.go
git commit -m "feat: Inspector Pause/Resume/PausedQueues + CLI queue pause/resume + PAUSED 컬럼"
```

---

## Task 4: 스케줄러 레지스트리 통합

**Files:**
- Modify: `scheduler.go`, `inspector.go`(SchedulerStatus 병합), `chronos.go`(ScheduleInfo 확장 — 타입이 inspector.go에 있으면 거기)
- Test: `scheduler_integration_test.go`(또는 신규 `scheduler_registry_test.go`)

- [ ] **Step 1: 실패 테스트**

`scheduler_registry_test.go` 신규:
```go
package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestScheduler_RegistersSchedulesAndTouches(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	s := NewScheduler(client, SchedulerConfig{LeaderTTL: time.Second})
	if err := RegisterInterval(s, time.Hour, emailArgs{UserID: "reg"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Shutdown(context.Background())

	insp := NewInspector(client)
	// 등록 직후: 미발화여도 레지스트리로 노출.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := insp.SchedulerStatus(ctx)
		if err == nil && len(st.Schedules) == 1 {
			sc := st.Schedules[0]
			if sc.Kind != "email:send" || sc.Queue != "default" || sc.Spec != "@every 1h0m0s" {
				t.Fatalf("schedule meta wrong: %+v", sc)
			}
			if sc.Stale {
				t.Fatal("fresh schedule marked stale")
			}
			if sc.LastSeen.IsZero() {
				t.Fatal("LastSeen zero")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("schedule never appeared in registry")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSchedulerStatus_StaleDetection(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()
	// 레지스트리에 오래된 엔트리를 직접 심는다.
	old := time.Now().Add(-10 * time.Minute).Unix()
	client.HSet(ctx, "chronos:schedules", "ghost#1",
		`{"kind":"ghost:job","queue":"default","spec":"@every 1s","registered_at":`+
			// JSON에 숫자 삽입
			fmtInt(old)+`,"last_seen":`+fmtInt(old)+`}`)
	insp := NewInspector(client)
	st, err := insp.SchedulerStatus(ctx)
	if err != nil || len(st.Schedules) != 1 {
		t.Fatalf("status: %v %v", st, err)
	}
	if !st.Schedules[0].Stale {
		t.Error("old entry not marked stale")
	}
}
```
(`fmtInt`는 `strconv.FormatInt(old, 10)` 헬퍼 — 테스트 파일에 소함수 추가하거나 fmt.Sprintf 사용.)

주의: `Spec` 문자열 — RegisterInterval의 spec이 어떤 원문으로 저장되는지 확인(`register(spec, ...)`의 spec 인자; interval이면 `"@every "+d.String()` 형태일 것 — scheduler.go의 RegisterInterval 구현을 읽고 기대값을 실제 형식에 맞출 것).

- [ ] **Step 2: 실패 확인** → FAIL (레지스트리 미기록/필드 없음).

- [ ] **Step 3: 구현**

scheduler.go:
- `scheduleEntry`에 `spec string` 추가, `register()`에서 세팅.
- `Start`(또는 `run` 시작부)에서 등록:
```go
	metas := make([]rdb.ScheduleMeta, 0, len(s.entries))
	for _, e := range s.entries {
		metas = append(metas, rdb.ScheduleMeta{ID: e.id, Kind: e.kind, Queue: e.queue, Spec: e.spec})
	}
	if err := s.rdb.RegisterSchedules(ctx, metas); err != nil {
		s.logger.Error("chronos: schedule registry write failed", "error", err) // 비치명 — 스케줄링은 계속
	}
```
(s.entries 필드명·logger 존재를 실제 코드에서 확인해 맞출 것.)
- `run()`의 `case <-renew.C:`에 touch 추가:
```go
		case <-renew.C:
			s.tryElect(ctx)
			ids := make([]string, 0, len(s.entries))
			for _, e := range s.entries {
				ids = append(ids, e.id)
			}
			if err := s.rdb.TouchSchedules(ctx, ids); err != nil && ctx.Err() == nil {
				s.logger.Debug("chronos: schedule touch failed", "error", err)
			}
```

inspector.go — `ScheduleInfo` 확장 + `SchedulerStatus` 병합 재작성:
```go
// ScheduleInfo is one schedule's registry entry merged with its fire history.
type ScheduleInfo struct {
	ID        string
	Kind      string    // "" for pre-registry fire-history-only entries
	Queue     string
	Spec      string
	LastFired time.Time // zero if never fired
	LastSeen  time.Time // zero for fire-history-only entries
	Stale     bool      // registry entry not refreshed within staleAfter
}

// staleAfter marks a registry entry stale when no scheduler has refreshed it
// this long (schedulers touch every LeaderTTL/2).
const staleAfter = time.Minute
```
`SchedulerStatus()`: `rdb.ListSchedules` + `rdb.ScanSchedules`를 ID로 병합 —
레지스트리 엔트리에 LastFired 채우고, 레지스트리에 없는 발화 이력은 ID/LastFired만으로 추가. `Stale: time.Since(lastSeen) > staleAfter` (레지스트리 출신만).

- [ ] **Step 4: 통과 + 회귀 + 커밋**

Run: `go test . -run 'TestScheduler_Registers|TestSchedulerStatus_Stale|TestInspector_GroupMembersAndSchedulerStatus' -p 1 -race` → PASS (기존 SchedulerStatus 테스트가 새 필드로 깨지면 의미 유지 갱신). `make check` → PASS.
```bash
git add scheduler.go inspector.go scheduler_registry_test.go inspector_test.go
git commit -m "feat: 스케줄 레지스트리 — Start 등록·renew 틱 touch·SchedulerStatus 병합+stale"
```

---

## Task 5: webui — pause 토글 + 스케줄러 페이지 확장

**Files:**
- Modify: `contrib/webui/webui.go`(라우트), `handlers.go`, `templates/queue.html`·`dashboard.html`·`scheduler.html`, `static/style.css`·`app.js`
- Test: `contrib/webui/webui_test.go`

- [ ] **Step 1: 실패 테스트**

webui_test.go:
```go
func TestQueuePauseToggle(t *testing.T) {
	client := newTestRedis(t)
	seedScheduled(t, client)
	insp := chronos.NewInspector(client)
	srv := httptest.NewServer(Handler(insp))
	defer srv.Close()

	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Post(srv.URL+"/queues/default/pause", "", nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if paused, _ := insp.PausedQueues(context.Background()); len(paused) != 1 {
		t.Fatalf("not paused: %v", paused)
	}
	// 대시보드에 ⏸ 배지.
	body := readBody(t, mustGet(t, srv.URL+"/"))
	if !strings.Contains(body, "paused") {
		t.Errorf("dashboard missing paused badge")
	}
	// resume.
	resp2, err := noRedirect.Post(srv.URL+"/queues/default/resume", "", nil)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	resp2.Body.Close()
	if paused, _ := insp.PausedQueues(context.Background()); len(paused) != 0 {
		t.Fatalf("still paused: %v", paused)
	}
}

func TestSchedulerPage_RegistryColumns(t *testing.T) {
	client := newTestRedis(t)
	client.HSet(context.Background(), "chronos:schedules", "regjob#1",
		fmt.Sprintf(`{"kind":"report:daily","queue":"ops","spec":"0 0 * * *","registered_at":%d,"last_seen":%d}`,
			time.Now().Unix(), time.Now().Unix()))
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	body := readBody(t, mustGet(t, srv.URL+"/scheduler"))
	if !strings.Contains(body, "report:daily") || !strings.Contains(body, "0 0 * * *") {
		t.Errorf("scheduler page missing registry columns:\n%s", body)
	}
}
```
(`fmt` import 필요 시 추가.)

- [ ] **Step 2: 실패 확인** → FAIL (404/컬럼 없음).

- [ ] **Step 3: 구현**

- webui.go 라우트: `POST /queues/{queue}/pause` → `s.pauseQueue`, `/resume` → `s.resumeQueue`.
- handlers.go: 기존 `action` 헬퍼 재사용 불가(fn 시그니처가 (ctx,q,id)) — 유사한 2-인자 버전:
```go
func (s *server) pauseQueue(w http.ResponseWriter, r *http.Request) {
	s.queueAction(w, r, s.insp.PauseQueue, "paused (takes effect within ~1s)")
}

func (s *server) resumeQueue(w http.ResponseWriter, r *http.Request) {
	s.queueAction(w, r, s.insp.ResumeQueue, "resumed")
}

// queueAction runs a queue-level mutating call with the same Origin guard and
// PRG as task actions.
func (s *server) queueAction(w http.ResponseWriter, r *http.Request, fn func(ctx, string) error, okMsg string) {
	if o := r.Header.Get("Origin"); o != "" && o != "http://"+r.Host && o != "https://"+r.Host {
		http.Error(w, "cross-origin request rejected", http.StatusForbidden)
		return
	}
	queue := r.PathValue("queue")
	msg := okMsg
	if err := fn(r.Context(), queue); err != nil {
		msg = "error: " + err.Error()
	}
	http.Redirect(w, r, "/queues/"+queue+"?msg="+url.QueryEscape(msg), http.StatusSeeOther)
}
```
- dashboard.html 카드: `{{if .Paused}}<span class="badge b-paused">⏸ paused</span>{{end}}`(qname 옆). queue.html 상단에 토글 폼(Paused 여부에 따라 Pause/Resume 버튼 — queueDetail 데이터에 `Paused bool` 추가: `insp.PausedQueues` 조회 또는 Queues에서 — 간단히 `PausedQueues` 1회 조회 후 포함 여부).
- statsQueue에 `Paused bool` + apiStats에서 채움(PausedQueues 1회) + app.js render()가 paused 배지 토글(`card.querySelector('.b-paused')` 존재 토글은 복잡 — 간단히: `data-stat` 패턴처럼 배지를 항상 렌더하고 `hidden` 속성 토글).
- scheduler.html 표를 Kind/Queue/Spec/LastFired/LastSeen 컬럼으로 확장, `{{if .Stale}}class="stale"{{end}}` + `.stale { opacity:.5 }` + stale 배지.
- style.css: `.b-paused`(회색/노랑 계열) 추가.

- [ ] **Step 4: 통과 + 커밋**

Run: `cd contrib/webui && go test ./... -p 1 -race` → PASS.
```bash
git add contrib/webui
git commit -m "feat: webui 큐 pause/resume 토글·⏸ 배지 + 스케줄러 레지스트리 컬럼"
```

---

## Task 6: cluster 스모크 + tour + README + 리뷰 + PR

**Files:**
- Modify: `cluster_test.go`, `examples/tour/main.go`, `README.md`, `contrib/webui/README.md`

- [ ] **Step 1: cluster 스모크 17번째**

체크리스트 줄 추가 후:
```go
//  [x] pause/resume (global SET, consumption gate)          → TestCluster_PauseResume
```
```go
func TestCluster_PauseResume(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()
	insp := NewInspector(client)

	var done atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		done.Add(1)
		return nil
	})
	srv := NewServer(client, clusterServerConfig("alpha"))
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if err := insp.PauseQueue(ctx, "alpha"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, err := Enqueue(ctx, c, clArgs{N: 51}, WithQueue("alpha")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	time.Sleep(2 * time.Second)
	if done.Load() != 0 {
		t.Fatalf("consumed while paused")
	}
	if err := insp.ResumeQueue(ctx, "alpha"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	waitFor(t, 10*time.Second, "consumption after resume", func() bool { return done.Load() == 1 })
}
```
docker 클러스터 기동 후 17개 전체 실행(2회) — CROSSSLOT 발생 시 BLOCKED.

- [ ] **Step 2: tour 섹션 14**

섹션 13 종료 후 삽입 (insp/client/rdb 재사용):
```go
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
```
doc comment에 pause 추가. 실행 확인: `go run ./examples/tour 2>&1 | sed -n '/=== 14)/,$p'`.

- [ ] **Step 3: README**

- 루트: Highlights의 Observable 항목 뒤나 적절한 곳에 pause 한 줄, Web console 항목 갱신(pause 토글·스케줄 레지스트리), CLI 예시에 `queue pause`, Known limitations에서 Phase 2 문구 제거(chain×group·결과 전달만 남김), 스케줄러 섹션에 "등록 스케줄은 레지스트리로 노출(stale 표시)" 한 줄.
- webui README: 기능 목록에 pause 토글·스케줄 레지스트리 갱신.

- [ ] **Step 4: 최종 검증 + 리뷰 + PR**

`make check` + `make test-cluster`(17) + tour 14섹션 + webui 테스트.
k:code-reviewer 리뷰(집중: fetchLoop paused 필터의 sem 회계, 전부-paused 시 idle 경로, WRR 상호작용, TouchSchedules의 read-modify-write 경합(HMGET→HSET 사이 last_seen 유실 — 멱등이라 무해한지), 레지스트리 JSON 하위호환). 반영 후:

```bash
gh pr create --assignee kenshin579 --title "feat: 큐 일시정지/재개 + 스케줄 레지스트리 (webui Phase 2)" --body "$(cat <<'EOF'
## 배경
webui v2에서 연기한 Phase 2. pause는 운영 중 특정 큐의 소비를 잠시 멈추는 기능(배포·하류 장애 대응), 레지스트리는 "등록됐지만 미발화" 스케줄까지 노출.

## 변경
- **pause = 소비만 중단**(asynq 의미론): 전역 SET `chronos:paused` + fetchLoop 1s 캐시(반영 지연 ~1s 문서화). in-flight 완주, forwarder/enqueue 계속 → pending 축적이 대시보드에 보임. Inspector Pause/Resume/PausedQueues + QueueInfo.Paused, CLI `queue pause|resume` + PAUSED 컬럼, webui 토글+⏸ 배지+/api/stats.
- **스케줄 레지스트리**: 전역 HASH `chronos:schedules` — Scheduler.Start 멱등 등록 + 기존 renew 틱에서 last_seen touch(새 고루틴 없음). Shutdown 삭제 없음(다중 인스턴스 보호) → stale(60s) 판정 표시. SchedulerStatus가 레지스트리+발화 이력 병합 — 미발화 스케줄 노출(E-lite 한계 해소). webui 스케줄러 페이지 Kind/Queue/Spec/LastSeen 컬럼+stale 회색.
- 둘 다 단일 키 명령만 — cluster-safe(신규 Lua 없음), cluster 스모크 17개.

## 테스트 계획
- [x] make check 무회귀 (pause 통합·부분 pause·레지스트리·stale·CLI·webui 토글)
- [x] make test-cluster 17/17
- [x] go run ./examples/tour 섹션 14 눈 확인

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review (계획 작성자 확인 완료)

- **스펙 커버리지**: A pause(T1 rdb, T2 서버, T3 Inspector/CLI, T5 UI) / B 레지스트리(T1 rdb, T4 스케줄러·병합, T5 UI) / cluster·hearsay(T6) / tour·README(T6) — 전 항목 매핑.
- **placeholder**: 전 스텝 실제 코드. 실코드 확인 지시(스케줄러 entries 필드명, RegisterInterval spec 형식, fetchLoop sem 순서)는 명시적 검증 지점.
- **타입 일관성**: `rdb.PauseQueue/ResumeQueue/PausedQueues`(T1)를 T2(fetchLoop)·T3(Inspector)가 사용, `rdb.ScheduleMeta/RegisterSchedules/TouchSchedules/ListSchedules`(T1)를 T4가 사용, `QueueInfo.Paused`(T3)를 T5·T6 tour가 사용, `ScheduleInfo{Kind,Queue,Spec,LastSeen,Stale}`(T4)를 T5 템플릿이 사용.
- **주의**: T2 테스트는 Inspector 대신 rdb 직접 사용(순서 의존 제거). 기존 `TestSchedulerPage`(v2)는 레지스트리 없는 발화 이력만으로도 통과해야 함(병합이 이력-only 엔트리 유지 — T4 구현이 보장).
