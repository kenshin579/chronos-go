# chronos-go Inspector + CLI Implementation Plan (관찰 도구, M5 일부를 앞당김)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 헤드리스 라이브러리의 "지금 시스템에 뭐가 쌓여 있나"를 볼 수 있게 한다 — 큐/태스크 상태를 조회하고 재실행·삭제하는 **Inspector API**와 그 위에 얹는 **CLI(`chronos`)**. 더불어 M1부터 있던 **stream 무한 증가 결함**(XACK 후 XDEL 안 함)을 먼저 고친다.

**Architecture:** 태스크 상태는 이미 Redis(stream + scheduled/retry/archived ZSET + task HASH)에 다 있다. Inspector는 이를 읽어 큐별 상태 카운트와 태스크 목록을 제공하고, 재실행(해당 ZSET→stream 승격)·삭제(ZSET/HASH 제거 + unique 락 해제)를 수행한다. CLI는 Inspector를 감싼 얇은 표준 `flag` 기반 명령이다(외부 의존성 추가 없음).

**Tech Stack:** Go 1.26, `github.com/redis/go-redis/v9`, 표준 `flag`/`text/tabwriter`, 실제 Redis 테스트 하니스.

**설계 문서:** `docs/superpowers/specs/2026-07-05-multi-cluster-scheduler-design.md` (섹션 4 Inspector API, M5 운영성)

**왜 M4보다 먼저?** 스케줄러(M4)를 개발할 때 "리더가 큐에 뭘 넣었나"를 CLI로 들여다보며 진행할 수 있다. `examples/tour`가 "실행 중 동작"을 보여준다면 Inspector/CLI는 "현재 적재 상태"를 보여줘 상호보완적이다.

**범위 밖:** Prometheus 메트릭(별도), Web UI, 리더 선출/스케줄러(M4), completed 태스크 retention/janitor(별도 — 이 계획은 stream 트리밍만 다루고 completed ZSET은 도입하지 않는다).

**M1~M3에서 확정된 실제 시그니처:**
- `rdb.NewRDB(client)`, `rdb.RDB.Client()`, `rdb.ConsumerGroup`
- `rdb.RDB.Done(ctx, qname, streamID string, msg *base.TaskMessage) error` — 현재 `XAck + Del(taskKey)` (XDEL 없음)
- `retry.go`의 `moveToZSetCmd` Lua (Retry/Archive가 사용) — 현재 `XACK + HSET + ZADD` (XDEL 없음)
- `rdb.go`의 `Dequeue` orphan 경로 — `r.client.XAck(...)` (XDEL 없음)
- `base.StreamKey/TaskKey/TaskKeyPrefix/ScheduledKey/RetryKey/ArchivedKey/QueuesKey/UniqueKey`, 상태 상수
- `base.TaskMessage{ID, Kind, Payload, Queue, State, Retried, MaxRetry, NoArchive, UniqueKey}`, `base.DecodeMessage`
- `rdb.RDB.releaseUnique(ctx, msg)` (비공개; unique 락 해제)
- 루트 `chronos.TaskInfo{ID, Kind, Queue}`

---

## File Structure

| 파일 | 변경 |
|---|---|
| `internal/rdb/rdb.go` | `Done`에 XDEL 추가; Dequeue orphan 경로에 XDEL 추가 |
| `internal/rdb/retry.go` | `moveToZSetCmd` Lua에 XDEL 추가 |
| `internal/rdb/inspect.go` (신규) | `QueueStats`, `ListZSetTasks`, `GetTask`, `RunTask`, `DeleteTask` |
| `inspector.go` (신규, 루트) | `Inspector`, `QueueInfo`, `Queues`, `ListTasks`, `RunTask`, `DeleteTask` |
| `cmd/chronos/main.go` (신규) | CLI: `queue ls`, `task ls/run/rm` |
| 각 `*_test.go` | 신규 테스트 |

**의존 순서:** 트리밍 fix → rdb inspect → Inspector → CLI → 데모/문서.

**테스트:** 실제 Redis. `make test-race`.

---

## Task 1: stream 트리밍 결함 수정 (XACK 뒤 XDEL)

태스크가 stream을 떠날 때(완료/재시도/보관/orphan) 엔트리를 XDEL해 stream 무한 증가를 막는다. 상태는
task HASH와 ZSET이 갖고 있으므로 stream 엔트리는 "배달 수단"일 뿐, 소비 후 삭제해도 안전하다.

**Files:**
- Modify: `internal/rdb/rdb.go` (Done, Dequeue orphan), `internal/rdb/retry.go` (moveToZSetCmd)
- Test: `internal/rdb/rdb_test.go` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`internal/rdb/rdb_test.go`에 추가:
```go
func TestDone_TrimsStreamEntry(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, streamID, err := r.Dequeue(ctx, "c1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if err := r.Done(ctx, "default", streamID, got); err != nil {
		t.Fatalf("done: %v", err)
	}

	// After Done the stream must be empty (entry deleted, not just acked).
	if n, _ := client.XLen(ctx, base.StreamKey("default")).Result(); n != 0 {
		t.Errorf("stream len after Done = %d, want 0 (entry must be XDEL'd)", n)
	}
}

func TestRetry_TrimsStreamEntry(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	msg := &base.TaskMessage{ID: "t1", Kind: "k", Payload: []byte("{}"), Queue: "default", MaxRetry: 3}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, streamID, _ := r.Dequeue(ctx, "c1", 0, "default")
	if err := r.Retry(ctx, "default", streamID, got, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if n, _ := client.XLen(ctx, base.StreamKey("default")).Result(); n != 0 {
		t.Errorf("stream len after Retry = %d, want 0", n)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestDone_TrimsStreamEntry|TestRetry_TrimsStreamEntry' -v`
Expected: FAIL — stream len == 1 (엔트리가 XACK만 되고 남아있음).

- [ ] **Step 3: Done에 XDEL 추가**

`internal/rdb/rdb.go`의 `Done`의 파이프라인에 XDel을 추가:
```go
func (r *RDB) Done(ctx context.Context, qname, streamID string, msg *base.TaskMessage) error {
	pipe := r.client.TxPipeline()
	pipe.XAck(ctx, base.StreamKey(qname), ConsumerGroup, streamID)
	pipe.XDel(ctx, base.StreamKey(qname), streamID)
	pipe.Del(ctx, base.TaskKey(qname, msg.ID))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return r.releaseUnique(ctx, msg)
}
```

- [ ] **Step 4: moveToZSetCmd에 XDEL 추가**

`internal/rdb/retry.go`의 `moveToZSetCmd`를 교체(XACK 다음 줄에 XDEL):
```go
var moveToZSetCmd = redis.NewScript(`
redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
redis.call("XDEL", KEYS[1], ARGV[2])
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[6])
return 1
`)
```

- [ ] **Step 5: Dequeue orphan 경로에 XDEL 추가**

`internal/rdb/rdb.go`의 Dequeue에서 orphan(본문 없음) 처리를 XACK+XDEL로:
```go
	if err == redis.Nil {
		// Orphan stream entry (body already gone): ack, delete it, report no task.
		pipe := r.client.TxPipeline()
		pipe.XAck(ctx, streamKey, ConsumerGroup, entry.ID)
		pipe.XDel(ctx, streamKey, entry.ID)
		_, _ = pipe.Exec(ctx)
		return nil, "", ErrNoTask
	}
```
(기존 단일 `r.client.XAck(...)` 줄을 위 블록으로 대체.)

- [ ] **Step 6: 통과 확인 + 회귀**

Run: `go test ./internal/rdb/ -race` 그리고 `go test ./... -race -p 1`
Expected: 신규 2개 포함 전부 PASS. (기존 `TestDone_AcksAndDeletesTask`, Archive/Recover 테스트도 XDEL 추가와 무관하게 통과.)

- [ ] **Step 7: 커밋**

```bash
git add internal/rdb/rdb.go internal/rdb/retry.go internal/rdb/rdb_test.go
git commit -m "fix: stream 엔트리를 XACK 후 XDEL (stream 무한 증가 방지)"
```

---

## Task 2: rdb — QueueStats (큐별 상태 카운트)

**Files:**
- Create: `internal/rdb/inspect.go`
- Test: `internal/rdb/inspect_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/inspect_test.go`:
```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestQueueStats_CountsByState(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// 1 pending (enqueued, not dequeued)
	if err := r.Enqueue(ctx, &base.TaskMessage{ID: "p1", Kind: "k", Payload: []byte("{}"), Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// 1 active (dequeued, not acked)
	if err := r.Enqueue(ctx, &base.TaskMessage{ID: "a1", Kind: "k", Payload: []byte("{}"), Queue: "default"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, _, err := r.Dequeue(ctx, "c1", 0, "default"); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	// 1 scheduled
	if err := r.Schedule(ctx, &base.TaskMessage{ID: "s1", Kind: "k", Payload: []byte("{}"), Queue: "default"}, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	st, err := r.QueueStats(ctx, "default")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Pending != 1 || st.Active != 1 || st.Scheduled != 1 {
		t.Errorf("stats = %+v, want pending1/active1/scheduled1", st)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run TestQueueStats_CountsByState -v`
Expected: FAIL — `undefined: (*RDB).QueueStats`.

- [ ] **Step 3: QueueStats 구현**

Create `internal/rdb/inspect.go`:
```go
package rdb

import (
	"context"

	"github.com/kenshin579/chronos-go/internal/base"
)

// QueueStats holds per-state task counts for a queue.
type QueueStats struct {
	Queue     string
	Pending   int64 // in the stream, not yet delivered to a worker
	Active    int64 // delivered, not yet acked (consumer group PEL)
	Scheduled int64
	Retry     int64
	Archived  int64
}

// QueueStats returns the per-state task counts for a queue.
func (r *RDB) QueueStats(ctx context.Context, qname string) (*QueueStats, error) {
	streamKey := base.StreamKey(qname)

	xlen, err := r.client.XLen(ctx, streamKey).Result()
	if err != nil {
		return nil, err
	}
	pending, err := r.client.XPending(ctx, streamKey, ConsumerGroup).Result()
	if err != nil {
		return nil, err
	}
	active := pending.Count // entries currently in the PEL

	scheduled, err := r.client.ZCard(ctx, base.ScheduledKey(qname)).Result()
	if err != nil {
		return nil, err
	}
	retry, err := r.client.ZCard(ctx, base.RetryKey(qname)).Result()
	if err != nil {
		return nil, err
	}
	archived, err := r.client.ZCard(ctx, base.ArchivedKey(qname)).Result()
	if err != nil {
		return nil, err
	}

	streamPending := xlen - active // stream total minus in-flight = not-yet-delivered
	if streamPending < 0 {
		streamPending = 0
	}
	return &QueueStats{
		Queue:     qname,
		Pending:   streamPending,
		Active:    active,
		Scheduled: scheduled,
		Retry:     retry,
		Archived:  archived,
	}, nil
}

// Queues returns the names of all known queues.
func (r *RDB) Queues(ctx context.Context) ([]string, error) {
	return r.client.SMembers(ctx, base.QueuesKey()).Result()
}
```

- [ ] **Step 4: 통과 확인 + 커밋**

Run: `go test ./internal/rdb/ -run TestQueueStats_CountsByState -v`
Expected: PASS

```bash
git add internal/rdb/inspect.go internal/rdb/inspect_test.go
git commit -m "feat: rdb QueueStats + Queues (큐별 상태 카운트)"
```

---

## Task 3: rdb — 태스크 목록/재실행/삭제

**Files:**
- Modify: `internal/rdb/inspect.go`
- Test: `internal/rdb/inspect_test.go` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`internal/rdb/inspect_test.go`에 추가:
```go
func TestListZSetTasks_ReturnsMessages(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "s1", Kind: "k", Payload: []byte(`{"x":1}`), Queue: "default"}
	if err := r.Schedule(ctx, msg, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	tasks, err := r.ListZSetTasks(ctx, "default", base.ScheduledKey("default"), 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ID != "s1" {
		t.Fatalf("tasks = %+v, want 1 with id s1", tasks)
	}
}

func TestRunTask_MovesToStream(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// An archived task, run it now.
	msg := &base.TaskMessage{ID: "a1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, sid, _ := r.Dequeue(ctx, "c1", 0, "default")
	if err := r.Archive(ctx, "default", sid, msg, time.Now()); err != nil {
		t.Fatalf("archive: %v", err)
	}

	if err := r.RunTask(ctx, "default", "a1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	// Removed from archived, now in the stream.
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "a1").Result(); err == nil {
		t.Error("task should be removed from archived zset")
	}
	if n, _ := client.XLen(ctx, base.StreamKey("default")).Result(); n != 1 {
		t.Errorf("stream len = %d, want 1", n)
	}
}

func TestDeleteTask_RemovesEverywhere(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "s1", Kind: "k", Payload: []byte("{}"), Queue: "default"}
	if err := r.Schedule(ctx, msg, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if err := r.DeleteTask(ctx, "default", "s1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), "s1").Result(); err == nil {
		t.Error("task should be removed from scheduled zset")
	}
	if n, _ := client.Exists(ctx, base.TaskKey("default", "s1")).Result(); n != 0 {
		t.Error("task hash should be deleted")
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestListZSetTasks_ReturnsMessages|TestRunTask_MovesToStream|TestDeleteTask_RemovesEverywhere' -v`
Expected: FAIL — `undefined` on the three methods.

- [ ] **Step 3: 구현**

`internal/rdb/inspect.go`에 추가:
```go
// ListZSetTasks returns up to limit task messages referenced by a state ZSET
// (scheduled / retry / archived), ordered by score (soonest / oldest first).
func (r *RDB) ListZSetTasks(ctx context.Context, qname, zsetKey string, limit int) ([]*base.TaskMessage, error) {
	ids, err := r.client.ZRange(ctx, zsetKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	tasks := make([]*base.TaskMessage, 0, len(ids))
	for _, id := range ids {
		msg, err := r.GetTask(ctx, qname, id)
		if err == redis.Nil {
			continue // body gone; skip
		}
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, msg)
	}
	return tasks, nil
}

// GetTask reads a single task's message by ID. Returns redis.Nil if absent.
func (r *RDB) GetTask(ctx context.Context, qname, taskID string) (*base.TaskMessage, error) {
	raw, err := r.client.HGet(ctx, base.TaskKey(qname, taskID), "msg").Result()
	if err != nil {
		return nil, err
	}
	return base.DecodeMessage([]byte(raw))
}

// runTaskCmd moves a task from whichever state ZSET holds it into the stream for
// immediate processing.
// KEYS[1] scheduled, KEYS[2] retry, KEYS[3] archived, KEYS[4] stream, KEYS[5] task hash.
// ARGV[1] taskID, ARGV[2] pending state.
var runTaskCmd = redis.NewScript(`
local removed = redis.call("ZREM", KEYS[1], ARGV[1]) + redis.call("ZREM", KEYS[2], ARGV[1]) + redis.call("ZREM", KEYS[3], ARGV[1])
if redis.call("EXISTS", KEYS[5]) == 0 then
  return 0
end
redis.call("XADD", KEYS[4], "*", "task_id", ARGV[1])
redis.call("HSET", KEYS[5], "state", ARGV[2])
return 1
`)

// RunTask promotes a scheduled/retry/archived task to the stream so it runs now.
func (r *RDB) RunTask(ctx context.Context, qname, taskID string) error {
	keys := []string{
		base.ScheduledKey(qname), base.RetryKey(qname), base.ArchivedKey(qname),
		base.StreamKey(qname), base.TaskKey(qname, taskID),
	}
	return runTaskCmd.Run(ctx, r.client, keys, taskID, int(base.StatePending)).Err()
}

// DeleteTask removes a task from all state ZSETs and deletes its body, releasing
// any unique lock it holds. (It does not remove an in-flight stream/PEL entry;
// use it for scheduled/retry/archived tasks.)
func (r *RDB) DeleteTask(ctx context.Context, qname, taskID string) error {
	msg, err := r.GetTask(ctx, qname, taskID)
	if err != nil && err != redis.Nil {
		return err
	}
	pipe := r.client.TxPipeline()
	pipe.ZRem(ctx, base.ScheduledKey(qname), taskID)
	pipe.ZRem(ctx, base.RetryKey(qname), taskID)
	pipe.ZRem(ctx, base.ArchivedKey(qname), taskID)
	pipe.Del(ctx, base.TaskKey(qname, taskID))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	if msg != nil {
		return r.releaseUnique(ctx, msg)
	}
	return nil
}
```

`internal/rdb/inspect.go`의 import에 `"github.com/redis/go-redis/v9"`를 추가하라.

- [ ] **Step 4: 통과 확인 + 커밋**

Run: `go test ./internal/rdb/ -race`
Expected: PASS

```bash
git add internal/rdb/inspect.go internal/rdb/inspect_test.go
git commit -m "feat: rdb ListZSetTasks/GetTask/RunTask/DeleteTask (태스크 조회·재실행·삭제)"
```

---

## Task 4: 공개 Inspector API

**Files:**
- Create: `inspector.go`
- Test: `inspector_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `inspector_test.go`:
```go
package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestInspector_QueuesAndListAndRun(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	insp := NewInspector(client)
	ctx := context.Background()

	// One archived task via a failing server run would be complex; enqueue a
	// scheduled task and inspect it directly.
	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	queues, err := insp.Queues(ctx)
	if err != nil {
		t.Fatalf("queues: %v", err)
	}
	if len(queues) != 1 || queues[0].Queue != "default" || queues[0].Scheduled != 1 {
		t.Fatalf("queues = %+v, want 1 default with scheduled=1", queues)
	}

	tasks, err := insp.ListTasks(ctx, "default", "scheduled", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ID != info.ID {
		t.Fatalf("tasks = %+v, want the scheduled task", tasks)
	}

	// Run it now → moves to stream.
	if err := insp.RunTask(ctx, "default", info.ID); err != nil {
		t.Fatalf("run: %v", err)
	}
	if n, _ := client.XLen(ctx, "chronos:{default}:stream").Result(); n != 1 {
		t.Errorf("stream len = %d, want 1", n)
	}
}

func TestInspector_ListTasks_RejectsUnknownState(t *testing.T) {
	client := testutil.NewRedis(t)
	insp := NewInspector(client)
	if _, err := insp.ListTasks(context.Background(), "default", "bogus", 10); err == nil {
		t.Error("expected error for unknown state")
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run 'TestInspector_' -v`
Expected: FAIL — `undefined: NewInspector`.

- [ ] **Step 3: Inspector 구현**

Create `inspector.go`:
```go
package chronos

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// Inspector provides read and administrative access to queues and tasks. It is
// the foundation the CLI (and any future UI) is built on.
type Inspector struct {
	rdb *rdb.RDB
}

// NewInspector returns an Inspector backed by the given Redis client.
func NewInspector(r redis.UniversalClient) *Inspector {
	return &Inspector{rdb: rdb.NewRDB(r)}
}

// QueueInfo is a queue's per-state task counts.
type QueueInfo struct {
	Queue     string
	Pending   int64
	Active    int64
	Scheduled int64
	Retry     int64
	Archived  int64
}

// Queues lists all known queues with their per-state counts.
func (i *Inspector) Queues(ctx context.Context) ([]*QueueInfo, error) {
	names, err := i.rdb.Queues(ctx)
	if err != nil {
		return nil, err
	}
	infos := make([]*QueueInfo, 0, len(names))
	for _, name := range names {
		st, err := i.rdb.QueueStats(ctx, name)
		if err != nil {
			return nil, err
		}
		infos = append(infos, &QueueInfo{
			Queue: st.Queue, Pending: st.Pending, Active: st.Active,
			Scheduled: st.Scheduled, Retry: st.Retry, Archived: st.Archived,
		})
	}
	return infos, nil
}

// zsetKeyForState maps a user-facing state name to its ZSET key.
func zsetKeyForState(qname, state string) (string, error) {
	switch state {
	case "scheduled":
		return base.ScheduledKey(qname), nil
	case "retry":
		return base.RetryKey(qname), nil
	case "archived":
		return base.ArchivedKey(qname), nil
	default:
		return "", fmt.Errorf("chronos: unknown task state %q (want scheduled|retry|archived)", state)
	}
}

// ListTasks returns up to limit tasks in the given state (scheduled|retry|archived).
func (i *Inspector) ListTasks(ctx context.Context, qname, state string, limit int) ([]*TaskInfo, error) {
	zsetKey, err := zsetKeyForState(qname, state)
	if err != nil {
		return nil, err
	}
	msgs, err := i.rdb.ListZSetTasks(ctx, qname, zsetKey, limit)
	if err != nil {
		return nil, err
	}
	infos := make([]*TaskInfo, 0, len(msgs))
	for _, m := range msgs {
		infos = append(infos, &TaskInfo{ID: m.ID, Kind: m.Kind, Queue: m.Queue})
	}
	return infos, nil
}

// RunTask promotes a scheduled/retry/archived task so it runs immediately.
func (i *Inspector) RunTask(ctx context.Context, qname, taskID string) error {
	return i.rdb.RunTask(ctx, qname, taskID)
}

// DeleteTask removes a scheduled/retry/archived task and releases its unique lock.
func (i *Inspector) DeleteTask(ctx context.Context, qname, taskID string) error {
	return i.rdb.DeleteTask(ctx, qname, taskID)
}
```

- [ ] **Step 4: 통과 확인 + 커밋**

Run: `go test . -run 'TestInspector_' -race -v`
Expected: PASS

```bash
git add inspector.go inspector_test.go
git commit -m "feat: 공개 Inspector API (Queues/ListTasks/RunTask/DeleteTask)"
```

---

## Task 5: CLI (`cmd/chronos`)

표준 `flag`만 사용(외부 의존성 없음). 명령: `queue ls`, `task ls`, `task run`, `task rm`.

**Files:**
- Create: `cmd/chronos/main.go`
- Test: `cmd/chronos/main_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `cmd/chronos/main_test.go`:
```go
package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/kenshin579/chronos-go"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestRun_QueueLs(t *testing.T) {
	client := testutil.NewRedis(t)
	c := chronos.NewClient(client)
	defer c.Close()
	if _, err := chronos.Enqueue(context.Background(), c, greetArgs{Name: "x"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var out bytes.Buffer
	code := run([]string{"queue", "ls"}, client, &out)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; out=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "default") {
		t.Errorf("output missing queue name; got:\n%s", out.String())
	}
}

func TestRun_UnknownCommand(t *testing.T) {
	client := testutil.NewRedis(t)
	var out bytes.Buffer
	if code := run([]string{"bogus"}, client, &out); code == 0 {
		t.Error("unknown command should return non-zero")
	}
}

type greetArgs struct {
	Name string `json:"name"`
}

func (greetArgs) Kind() string { return "cli:greet" }
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./cmd/chronos/ -run 'TestRun_' -v`
Expected: FAIL — `undefined: run`.

- [ ] **Step 3: CLI 구현**

`run`을 테스트 가능한 순수 함수로 분리(Redis client + 출력 Writer 주입)하고, `main`은 flag 파싱 + Redis 연결 후 `run` 호출.

Create `cmd/chronos/main.go`:
```go
// Command chronos is a CLI for inspecting and administering chronos-go queues.
//
//	chronos [--redis addr] [--db n] queue ls
//	chronos [--redis addr] [--db n] task ls   <queue> <scheduled|retry|archived>
//	chronos [--redis addr] [--db n] task run  <queue> <task-id>
//	chronos [--redis addr] [--db n] task rm   <queue> <task-id>
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
)

func main() {
	addr := flag.String("redis", envOr("REDIS_ADDR", "127.0.0.1:6379"), "Redis address")
	db := flag.Int("db", 0, "Redis database number")
	flag.Parse()

	client := redis.NewClient(&redis.Options{Addr: *addr, DB: *db})
	defer client.Close()

	os.Exit(run(flag.Args(), client, os.Stdout))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// run executes a CLI command against the given client, writing to out. It
// returns a process exit code. Split out from main for testability.
func run(args []string, client redis.UniversalClient, out io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(out, "usage: chronos <queue|task> ...")
		return 2
	}
	insp := chronos.NewInspector(client)
	ctx := context.Background()

	switch args[0] {
	case "queue":
		if len(args) >= 2 && args[1] == "ls" {
			return queueLs(ctx, insp, out)
		}
	case "task":
		if len(args) >= 2 {
			switch args[1] {
			case "ls":
				if len(args) == 4 {
					return taskLs(ctx, insp, out, args[2], args[3])
				}
			case "run":
				if len(args) == 4 {
					return taskAction(ctx, out, "run", func() error { return insp.RunTask(ctx, args[2], args[3]) })
				}
			case "rm":
				if len(args) == 4 {
					return taskAction(ctx, out, "rm", func() error { return insp.DeleteTask(ctx, args[2], args[3]) })
				}
			}
		}
	}
	fmt.Fprintf(out, "unknown or malformed command: %v\n", args)
	return 2
}

func queueLs(ctx context.Context, insp *chronos.Inspector, out io.Writer) int {
	queues, err := insp.Queues(ctx)
	if err != nil {
		fmt.Fprintf(out, "error: %v\n", err)
		return 1
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "QUEUE\tPENDING\tACTIVE\tSCHEDULED\tRETRY\tARCHIVED")
	for _, q := range queues {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\n", q.Queue, q.Pending, q.Active, q.Scheduled, q.Retry, q.Archived)
	}
	tw.Flush()
	return 0
}

func taskLs(ctx context.Context, insp *chronos.Inspector, out io.Writer, queue, state string) int {
	tasks, err := insp.ListTasks(ctx, queue, state, 100)
	if err != nil {
		fmt.Fprintf(out, "error: %v\n", err)
		return 1
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tKIND\tQUEUE")
	for _, t := range tasks {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", t.ID, t.Kind, t.Queue)
	}
	tw.Flush()
	return 0
}

func taskAction(ctx context.Context, out io.Writer, verb string, fn func() error) int {
	if err := fn(); err != nil {
		fmt.Fprintf(out, "error: %v\n", err)
		return 1
	}
	fmt.Fprintf(out, "%s: ok\n", verb)
	return 0
}
```

- [ ] **Step 4: 통과 확인 + 빌드 + 커밋**

Run: `go test ./cmd/chronos/ -race -v && go build ./cmd/chronos`
Expected: PASS, 바이너리 빌드 성공.

```bash
git add cmd/chronos/
git commit -m "feat: chronos CLI (queue ls, task ls/run/rm)"
```

---

## Task 6: 투어/문서에 Inspector·CLI 반영 + 전체 검증

**Files:**
- Modify: `examples/tour/main.go` (Inspector 섹션 추가), `docs/OBSERVING.md` (CLI 사용법)

- [ ] **Step 1: 투어에 Inspector 섹션 추가**

`examples/tour/main.go`의 마지막 섹션(중복 억제) 다음, "투어 완료" 출력 전에 추가:
```go
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
```

- [ ] **Step 2: 투어 실행 확인**

Run: `go run ./examples/tour`
Expected: 6개 섹션 정상 출력, 마지막에 큐 카운트(scheduled>=1) 표시.

- [ ] **Step 3: CLI 수동 확인**

Run (투어가 DB 15에 데이터를 남긴 직후):
```bash
go run ./cmd/chronos --db 15 queue ls
go run ./cmd/chronos --db 15 task ls default scheduled
```
Expected: 큐 카운트 테이블, scheduled 태스크 목록 출력.

- [ ] **Step 4: OBSERVING.md에 CLI 섹션 추가**

`docs/OBSERVING.md`의 "3. 자동화된 검증" 앞에 추가:
```markdown
## 2b. CLI로 조회·관리

redis-cli로 원시 키를 보는 대신, `chronos` CLI로 상태를 조회하고 태스크를 재실행/삭제할 수 있다.

​```bash
go run ./cmd/chronos --db 15 queue ls                      # 큐별 상태 카운트
go run ./cmd/chronos --db 15 task ls default archived      # dead-letter 목록
go run ./cmd/chronos --db 15 task run default <task-id>    # 지금 재실행
go run ./cmd/chronos --db 15 task rm  default <task-id>    # 삭제
​```

`--redis <addr>`(기본 `$REDIS_ADDR` 또는 `127.0.0.1:6379`), `--db <n>`(기본 0)로 대상 지정.
실사용에서는 앱이 쓰는 DB에 맞춰 `--db`를 지정한다(테스트/데모는 15).
```
(위 코드블록의 ​백틱은 실제로는 일반 백틱 3개다 — zero-width 문자 없이 작성하라.)

- [ ] **Step 5: 전체 검증 + 커밋**

Run: `make check`
Expected: gofmt/vet 클린, `go test ./... -race -p 1` 전 패키지 PASS.

```bash
git add examples/tour/main.go docs/OBSERVING.md
git commit -m "docs: 투어/관찰 가이드에 Inspector·CLI 반영"
```

---

## 완료 기준

- [ ] `make check` 통과
- [ ] stream이 XACK+XDEL로 트리밍됨(처리된 태스크가 stream에 누적되지 않음)
- [ ] `Inspector.Queues`가 큐별 pending/active/scheduled/retry/archived 카운트 반환
- [ ] `Inspector.ListTasks/RunTask/DeleteTask` 동작
- [ ] `chronos queue ls` / `task ls|run|rm` CLI 동작
- [ ] 투어에 Inspector 섹션 추가, OBSERVING.md에 CLI 사용법

**다음 단계:** 이 관찰 도구를 손에 쥔 상태로 M4(리더 선출 + cron/interval 스케줄러 + 결정적 TaskID + misfire)를 진행한다. 스케줄러가 큐에 넣는 태스크를 `chronos queue ls`로 실시간 확인하며 개발할 수 있다.
