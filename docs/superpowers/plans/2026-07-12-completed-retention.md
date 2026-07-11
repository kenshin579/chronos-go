# Completed Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 성공한 태스크를 `WithRetention(d)` 기간 동안 `completed` ZSET에 보관해 Inspector/CLI로 조회·재실행·삭제 가능하게 한다 (기본 0 = 즉시 삭제, 기존 무변화).

**Architecture:** `TaskMessage`에 Retention(초)·CompletedAt(unix) 추가. `rdb.Done`이 Retention>0이면 삭제 대신 신규 `completeCmd` Lua로 completed ZSET(score=만료시각)에 이동. janitor는 기존 `trimArchivedCmd` Lua를 completed 키+cutoff=now로 재사용(`TrimCompleted`). Inspector/CLI는 `zsetKeyForState`에 "completed" 한 줄로 run/delete/ls/get 전부 획득.

**Tech Stack:** Go, redis/go-redis v9, Lua(기존 패턴), 실제 Redis(DB 15, `-p 1`), docker cluster(스모크 확장).

---

## File Structure

- Modify `internal/base/task.go` — Retention/CompletedAt 필드. Test: `internal/base/task_test.go`
- Modify `internal/base/keys.go` — `CompletedKey`. Test: 기존 keys 테스트 파일 패턴
- Modify `chronos.go` — `WithRetention` 옵션 + Enqueue가 msg.Retention 세팅
- Modify `internal/rdb/rdb.go` — `Done` retention 분기 + `completeCmd` Lua. Test: `internal/rdb/rdb_test.go`(기존 파일)
- Modify `internal/rdb/janitor.go` — `TrimCompleted`. Test: `internal/rdb/janitor_test.go`(기존 파일 확인)
- Modify `server.go` — `ServerConfig.MaxCompleted` + janitorLoop에 TrimCompleted. Test: `server_completed_test.go` (신규)
- Modify `inspector.go` — zsetKeyForState "completed", `TaskInfo.CompletedAt`. Modify `chronos.go`(TaskInfo)
- Modify `internal/rdb/inspect.go` — `QueueStats.Completed`
- Modify `cmd/chronos/main.go` — COMPLETED 컬럼 + usage 문구
- Modify `cluster_test.go` — `TestCluster_CompletedRetention` + 체크리스트 갱신
- Modify `examples/tour/main.go` — 섹션 11
- Modify `README.md`

**구현자 참고:** 기존 유사 코드 — 옵션 패턴 `chronos.go:112 WithMaxRetry`, Done `internal/rdb/rdb.go:164`, moveToZSetCmd Lua `internal/rdb/retry.go`, TrimArchived `internal/rdb/janitor.go`, janitorLoop `server.go:554`, QueueStats `internal/rdb/inspect.go:23`. 테스트 헬퍼 `testutil.NewRedis(t)`.

---

## Task 1: base — Retention/CompletedAt 필드 + CompletedKey

**Files:**
- Modify: `internal/base/task.go`, `internal/base/keys.go`
- Test: `internal/base/task_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`internal/base/task_test.go`에 추가:

```go
func TestTaskMessage_RetentionRoundTrips(t *testing.T) {
	msg := &TaskMessage{ID: "t2", Kind: "k", Queue: "default", Retention: 3600, CompletedAt: 1700000000}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Retention != 3600 || got.CompletedAt != 1700000000 {
		t.Errorf("Retention=%d CompletedAt=%d, want 3600/1700000000", got.Retention, got.CompletedAt)
	}
}

func TestCompletedKey(t *testing.T) {
	if got, want := CompletedKey("q1"), "chronos:{q1}:completed"; got != want {
		t.Errorf("CompletedKey = %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/base/ -run 'TestTaskMessage_RetentionRoundTrips|TestCompletedKey'`
Expected: FAIL — `unknown field Retention` / `undefined: CompletedKey`

- [ ] **Step 3: 구현**

`internal/base/task.go`의 `TaskMessage`에서 `LastErr` 필드 아래에 추가:

```go
	// Retention is how long (in seconds) a successfully completed task is kept
	// in the completed ZSET for inspection. 0 (default) deletes it immediately.
	Retention int64 `json:"retention,omitempty"`
	// CompletedAt is the unix time the task finished successfully. Set only
	// when the task is kept (Retention > 0).
	CompletedAt int64 `json:"completed_at,omitempty"`
```

`internal/base/keys.go`의 `ArchivedKey` 아래에 추가:

```go
// CompletedKey returns the ZSET key holding successfully completed tasks that
// are retained for inspection (score = expire-at, i.e. completed-at + retention).
func CompletedKey(qname string) string {
	return QueueKeyPrefix(qname) + "completed"
}
```

- [ ] **Step 4: 통과 확인**

Run: `go test ./internal/base/ -p 1` → PASS (전체)

- [ ] **Step 5: 커밋**

```bash
git add internal/base/task.go internal/base/keys.go internal/base/task_test.go
git commit -m "feat: TaskMessage Retention/CompletedAt + CompletedKey"
```

---

## Task 2: rdb — Done의 retention 분기 (completeCmd Lua)

**Files:**
- Modify: `internal/rdb/rdb.go`
- Test: `internal/rdb/rdb_test.go` (기존 파일에 추가; 없으면 inspect_test.go 패턴으로 신규)

- [ ] **Step 1: 실패 테스트 작성**

기존 rdb 테스트 파일의 헬퍼 패턴(testutil.NewRedis + NewRDB)을 따라 추가:

```go
func TestDone_RetentionMovesToCompleted(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "c1", Kind: "k", Queue: "default", Retention: 3600}
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("group: %v", err)
	}
	got, streamID, err := r.Dequeue(ctx, "w1", -1, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	before := time.Now().Unix()
	if err := r.Done(ctx, "default", streamID, got); err != nil {
		t.Fatalf("done: %v", err)
	}

	// completed ZSET에 score = 완료시각+retention 으로 존재해야 한다.
	score, err := client.ZScore(ctx, base.CompletedKey("default"), "c1").Result()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	wantMin := float64(before + 3600)
	if score < wantMin || score > wantMin+5 {
		t.Errorf("score = %v, want ~%v (completedAt+retention)", score, wantMin)
	}
	// 태스크 hash가 남아 있고 state/CompletedAt이 기록돼야 한다.
	stored, err := r.GetTask(ctx, "default", "c1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.State != base.StateCompleted || stored.CompletedAt < before {
		t.Errorf("state=%v completedAt=%d, want completed/>=%d", stored.State, stored.CompletedAt, before)
	}
	// 스트림은 비워져야 한다 (XACK+XDEL).
	if xlen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); xlen != 0 {
		t.Errorf("stream len = %d, want 0", xlen)
	}
}

func TestDone_NoRetentionDeletesImmediately(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "c2", Kind: "k", Queue: "default"} // Retention 0
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("group: %v", err)
	}
	got, streamID, err := r.Dequeue(ctx, "w1", -1, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if err := r.Done(ctx, "default", streamID, got); err != nil {
		t.Fatalf("done: %v", err)
	}
	if _, err := r.GetTask(ctx, "default", "c2"); err != redis.Nil {
		t.Errorf("task hash should be deleted, got err=%v", err)
	}
	if n, _ := client.ZCard(ctx, base.CompletedKey("default")).Result(); n != 0 {
		t.Errorf("completed zset should be empty, got %d", n)
	}
}
```

(필요 import: `time`, `redis`. 기존 파일에 이미 있으면 생략.)

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestDone_Retention|TestDone_NoRetention' -p 1`
Expected: `TestDone_RetentionMovesToCompleted` FAIL (completed ZSET 비어 있음 — 현재 Done은 무조건 삭제). `TestDone_NoRetentionDeletesImmediately`는 이미 PASS일 수 있음(현행 동작).

- [ ] **Step 3: 구현**

`internal/rdb/rdb.go`의 `Done` 위에 Lua 추가, `Done`을 분기 버전으로 교체:

```go
// completeCmd acknowledges a task and, instead of deleting it, retains it in
// the completed ZSET for later inspection: it acks+deletes the stream entry,
// overwrites the task hash with the completed-stamped message, and registers
// the task in the completed ZSET with score = expire-at. All keys share the
// queue hash tag (cluster-safe).
// KEYS[1] stream, KEYS[2] task hash, KEYS[3] completed zset.
// ARGV[1] group, ARGV[2] streamID, ARGV[3] encoded msg, ARGV[4] state,
// ARGV[5] expire-at (score), ARGV[6] task id.
var completeCmd = redis.NewScript(`
redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
redis.call("XDEL", KEYS[1], ARGV[2])
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[6])
return 1
`)

// Done acknowledges a successfully processed task. With no retention it acks
// the stream entry, deletes the task body, and releases the unique lock (if
// any). With msg.Retention > 0 it keeps the task in the completed ZSET until
// completed-at + retention (the janitor removes it later); the unique lock is
// still released immediately — a retained completed task must not block a new
// identical enqueue.
func (r *RDB) Done(ctx context.Context, qname, streamID string, msg *base.TaskMessage) error {
	if msg.Retention > 0 {
		now := time.Now()
		msg.State = base.StateCompleted
		msg.CompletedAt = now.Unix()
		encoded, err := base.EncodeMessage(msg)
		if err != nil {
			return err
		}
		keys := []string{
			base.StreamKey(qname),
			base.TaskKey(qname, msg.ID),
			base.CompletedKey(qname),
		}
		argv := []interface{}{
			ConsumerGroup, streamID, encoded, int(base.StateCompleted),
			now.Unix() + msg.Retention, msg.ID,
		}
		if err := completeCmd.Run(ctx, r.client, keys, argv...).Err(); err != nil {
			return err
		}
		return r.releaseUnique(ctx, msg)
	}

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

(`time` import 확인. 기존 Done의 doc comment는 위 버전으로 대체.)

- [ ] **Step 4: 통과 확인**

Run: `go test ./internal/rdb/ -p 1` → 전체 PASS (기존 Done 테스트 포함 — Retention 0 경로는 바이트 단위 동일 동작)

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/rdb.go internal/rdb/rdb_test.go
git commit -m "feat: rdb.Done retention 분기 — completed ZSET 보관 (completeCmd Lua)"
```

---

## Task 3: rdb — TrimCompleted (기존 Lua 재사용)

**Files:**
- Modify: `internal/rdb/janitor.go`
- Test: `internal/rdb/janitor_test.go` (기존 파일에 추가)

- [ ] **Step 1: 실패 테스트 작성**

기존 janitor 테스트 패턴을 따라 추가:

```go
func TestTrimCompleted_RemovesExpiredAndOverCap(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	now := time.Now()

	// 만료 2개(score 과거), 유효 3개(score 미래).
	seed := func(id string, expireAt time.Time) {
		msg := &base.TaskMessage{ID: id, Kind: "k", Queue: "default", State: base.StateCompleted}
		encoded, _ := base.EncodeMessage(msg)
		client.HSet(ctx, base.TaskKey("default", id), "msg", encoded, "state", int(base.StateCompleted))
		client.ZAdd(ctx, base.CompletedKey("default"), redis.Z{Score: float64(expireAt.Unix()), Member: id})
	}
	seed("e1", now.Add(-time.Hour))
	seed("e2", now.Add(-time.Minute))
	seed("v1", now.Add(time.Hour))
	seed("v2", now.Add(2*time.Hour))
	seed("v3", now.Add(3*time.Hour))

	// (1) 만료분 정리: cutoff = now → e1, e2 삭제.
	n, err := r.TrimCompleted(ctx, "default", now, -1, 100)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if n != 2 {
		t.Errorf("removed = %d, want 2", n)
	}
	// (2) 크기 상한: maxSize 1 → 오래된 v1, v2 삭제, v3만 잔존.
	n, err = r.TrimCompleted(ctx, "default", now, 1, 100)
	if err != nil {
		t.Fatalf("trim cap: %v", err)
	}
	if n != 2 {
		t.Errorf("cap removed = %d, want 2", n)
	}
	if card, _ := client.ZCard(ctx, base.CompletedKey("default")).Result(); card != 1 {
		t.Errorf("remaining = %d, want 1", card)
	}
	if _, err := r.GetTask(ctx, "default", "v1"); err != redis.Nil {
		t.Errorf("v1 hash should be gone, err=%v", err)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/rdb/ -run TestTrimCompleted -p 1`
Expected: FAIL — `undefined: r.TrimCompleted`

- [ ] **Step 3: 구현**

`internal/rdb/janitor.go`의 `TrimArchived` 아래에 추가 (Lua는 zset 키만 다르므로 `trimArchivedCmd` 재사용):

```go
// TrimCompleted removes retained completed tasks whose expiry (the ZSET score,
// completed-at + retention) has passed, and any that still exceed maxSize
// (oldest first). It reuses trimArchivedCmd: the script is generic over the
// ZSET — only the key and the cutoff semantics differ (here the score already
// IS the expiry, so the cutoff is simply "now"). Batch/negative-maxSize rules
// match TrimArchived. Completed tasks hold no unique lock (released in Done).
func (r *RDB) TrimCompleted(ctx context.Context, qname string, now time.Time, maxSize, batch int) (int, error) {
	keys := []string{base.CompletedKey(qname)}
	argv := []interface{}{now.Unix(), batch, base.TaskKeyPrefix(qname), maxSize}
	n, err := trimArchivedCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}
```

- [ ] **Step 4: 통과 확인**

Run: `go test ./internal/rdb/ -p 1` → 전체 PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/janitor.go internal/rdb/janitor_test.go
git commit -m "feat: rdb.TrimCompleted (만료+크기 상한, trimArchivedCmd 재사용)"
```

---

## Task 4: 공개 API — WithRetention 옵션 + 서버 통합 (MaxCompleted, janitorLoop)

**Files:**
- Modify: `chronos.go` (옵션 + Enqueue), `server.go` (ServerConfig + janitorLoop)
- Test: `server_completed_test.go` (신규)

- [ ] **Step 1: 실패 테스트 작성**

`server_completed_test.go` 신규 (`package chronos`):

```go
package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

// startCompletedServer runs a server with fast janitor settings on "default".
func startCompletedServer(t *testing.T, client interface {
	// redis.UniversalClient without importing redis here
}, _ ...struct{}) {
}

func TestServer_WithRetentionKeepsCompletedTask(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(done)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "keep"}, WithRetention(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-done

	insp := NewInspector(client)
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, gerr := insp.GetTask(ctx, "default", info.ID)
		if gerr == nil && got.State == "completed" {
			if got.CompletedAt.IsZero() {
				t.Error("CompletedAt is zero, want completion time")
			}
			if got.NextProcessAt.IsZero() {
				t.Error("NextProcessAt (expiry) is zero")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task not in completed state in time (last err=%v)", gerr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestServer_UniqueLockReleasedDespiteRetention(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	done := make(chan struct{}, 2)
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		done <- struct{}{}
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "uq"}, WithUnique(time.Hour), WithRetention(time.Hour)); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("first task not processed")
	}
	// 완료 직후: 보관 중이어도 unique 락은 해제 → 동일 태스크 enqueue 가능해야 한다.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := Enqueue(ctx, c, emailArgs{UserID: "uq"}, WithUnique(time.Hour), WithRetention(time.Hour))
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("re-enqueue still blocked: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestServer_JanitorTrimsExpiredCompleted(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(done)
		return nil
	})
	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     1,
		JanitorInterval: 200 * time.Millisecond,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "short"}, WithRetention(time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-done

	insp := NewInspector(client)
	// retention(1s) + janitor(200ms) 경과 후 사라져야 한다.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, gerr := insp.GetTask(ctx, "default", info.ID); gerr != nil {
			return // trimmed
		}
		if time.Now().After(deadline) {
			t.Fatal("completed task not trimmed by janitor in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
```

> 주의: 파일 상단의 `startCompletedServer` 스텁은 넣지 말 것 — 위 코드블록에 실수로 포함된 초안 흔적이며 실제 테스트에 불필요하다. 세 테스트 함수와 import만 작성하라. `emailArgs`는 chronos_test.go에 이미 존재.

- [ ] **Step 2: 실패 확인**

Run: `go test . -run 'TestServer_WithRetention|TestServer_UniqueLockReleased|TestServer_JanitorTrimsExpiredCompleted' -p 1`
Expected: FAIL — `undefined: WithRetention` (컴파일 에러)

- [ ] **Step 3: WithRetention 옵션 구현**

`chronos.go`의 `enqueueOptions`에 필드 추가:

```go
	retention time.Duration // > 0 keeps the completed task for inspection
```

`WithUnique` 아래에 옵션 추가:

```go
// WithRetention keeps the task in the "completed" set for d after it succeeds,
// so it can be inspected (Inspector/CLI state "completed") before the janitor
// removes it. The default (no option) deletes the task immediately on success.
// Durations under one second are rounded up to one second; d <= 0 is ignored.
// Note: on high-throughput queues a long retention grows Redis memory — the
// per-queue MaxCompleted cap (ServerConfig) bounds the worst case.
func WithRetention(d time.Duration) Option {
	return optionFunc(func(o *enqueueOptions) {
		if d <= 0 {
			o.retention = 0
			return
		}
		if d < time.Second {
			d = time.Second
		}
		o.retention = d
	})
}
```

`Enqueue`의 `msg := &base.TaskMessage{...}` 리터럴에 필드 추가:

```go
		Retention: int64(options.retention / time.Second),
```

- [ ] **Step 4: 서버 통합**

`server.go`:
(a) `ServerConfig`의 `MaxArchived` 필드 아래에 추가:

```go
	// MaxCompleted caps the number of retained completed tasks per queue; the
	// janitor deletes the oldest beyond this even before their retention
	// expires. Defaults to 10000. Set negative to disable the size cap.
	MaxCompleted int
```

(b) `NewServer`의 `if cfg.MaxArchived == 0 { ... }` 아래에 추가:

```go
	if cfg.MaxCompleted == 0 {
		cfg.MaxCompleted = 10000
	}
```

(c) `janitorLoop`의 TrimArchived 호출 블록 아래(같은 `for _, q := range queues` 안)에 추가:

```go
				nc, err := s.rdb.TrimCompleted(ctx, q, time.Now(), s.cfg.MaxCompleted, janitorBatchSize)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: janitor trim completed failed", "queue", q, "error", err)
					continue
				}
				if nc > 0 {
					s.logger.Debug("chronos: janitor trimmed completed", "queue", q, "removed", nc)
				}
```

(d) `janitorLoop`의 doc comment를 "archived and retained-completed tasks"로 갱신.

- [ ] **Step 5: Inspector 최소 연결 (테스트가 GetTask로 검증하므로 이번 태스크에 포함)**

`inspector.go`:
(a) `zsetKeyForState`에 케이스 추가 + 에러 메시지 갱신:

```go
	case "completed":
		return base.CompletedKey(qname), nil
	default:
		return "", fmt.Errorf("%w %q (want scheduled|retry|archived|completed)", ErrInvalidState, state)
```

(b) `chronos.go`의 `TaskInfo`에 필드 추가 (`NextProcessAt` 아래):

```go
	CompletedAt time.Time // when the task finished successfully (zero unless retained)
```

(c) `inspector.go`의 `taskInfoFromMsg`에 매핑 추가:

```go
	ti := &TaskInfo{
		...기존 필드 유지...
	}
	if m.CompletedAt > 0 {
		ti.CompletedAt = time.Unix(m.CompletedAt, 0)
	}
	return ti
```
(기존 리터럴 반환을 변수로 바꿔 조건 세팅 후 반환하는 형태로 조정.)

`GetTask`의 doc comment을 `(scheduled/retry/archived/completed)`로 갱신.

- [ ] **Step 6: 통과 확인**

Run: `go test . -run 'TestServer_WithRetention|TestServer_UniqueLockReleased|TestServer_JanitorTrimsExpiredCompleted' -p 1 -race`
Expected: 3개 PASS. 이어서 `go test . -p 1` 전체 회귀 → PASS.

- [ ] **Step 7: 커밋**

```bash
git add chronos.go server.go inspector.go server_completed_test.go
git commit -m "feat: WithRetention 옵션 + completed 보관·janitor 통합 + Inspector completed 상태"
```

---

## Task 5: Inspector 카운트 + run/delete + CLI

**Files:**
- Modify: `internal/rdb/inspect.go` (QueueStats), `inspector.go` (QueueInfo 매핑), `cmd/chronos/main.go`
- Test: `inspector_test.go`, `cmd/chronos/main_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`inspector_test.go`에 추가:

```go
func TestInspector_CompletedCountAndActions(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		runs.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "cc"}, WithRetention(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	waitCompleted := func(id string) {
		deadline := time.Now().Add(5 * time.Second)
		for {
			got, gerr := insp.GetTask(ctx, "default", id)
			if gerr == nil && got.State == "completed" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("task %s not completed in time", id)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitCompleted(info.ID)

	// QueueInfo.Completed 카운트.
	qs, err := insp.Queues(ctx)
	if err != nil {
		t.Fatalf("queues: %v", err)
	}
	var completed int64 = -1
	for _, q := range qs {
		if q.Queue == "default" {
			completed = q.Completed
		}
	}
	if completed != 1 {
		t.Errorf("Completed count = %d, want 1", completed)
	}

	// ListTasks("completed") 필드.
	tasks, err := insp.ListTasks(ctx, "default", "completed", 10)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("list completed: n=%d err=%v", len(tasks), err)
	}
	if tasks[0].CompletedAt.IsZero() {
		t.Error("ListTasks completed: CompletedAt zero")
	}

	// RunTask: completed 태스크 재실행 → 핸들러 2회.
	if err := insp.RunTask(ctx, "default", info.ID); err != nil {
		t.Fatalf("run: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for runs.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("re-run did not execute (runs=%d)", runs.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	waitCompleted(info.ID) // 재실행 후 다시 completed로

	// DeleteTask: 보관분 조기 삭제.
	if err := insp.DeleteTask(ctx, "default", info.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := insp.GetTask(ctx, "default", info.ID); err == nil {
		t.Error("task still present after delete")
	}
}
```

(`sync/atomic` import 확인.)

`cmd/chronos/main_test.go`에 추가:

```go
func TestRun_QueueLsIncludesCompleted(t *testing.T) {
	client := testutil.NewRedis(t)
	var out bytes.Buffer
	code := run([]string{"queue", "ls"}, client, &out)
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if !strings.Contains(out.String(), "COMPLETED") {
		t.Errorf("queue ls header missing COMPLETED:\n%s", out.String())
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test . -run TestInspector_CompletedCountAndActions -p 1 && go test ./cmd/chronos/ -run TestRun_QueueLsIncludesCompleted -p 1`
Expected: FAIL — `q.Completed undefined` / 헤더에 COMPLETED 없음.

- [ ] **Step 3: 구현**

(a) `internal/rdb/inspect.go`의 `QueueStats` 구조체에 `Completed int64` 추가, `QueueStats` 함수에서 archived ZCard 아래에:

```go
	completed, err := r.client.ZCard(ctx, base.CompletedKey(qname)).Result()
	if err != nil {
		return nil, err
	}
```
반환 리터럴에 `Completed: completed,` 추가.

(b) `inspector.go`의 `QueueInfo`에 `Completed int64` 추가, `Queues`의 매핑에 `Completed: st.Completed,` 추가.

(c) `cmd/chronos/main.go`의 `queueLs`: 헤더에 `\tCOMPLETED`, 행 포맷에 `\t%d` + `q.Completed` 추가. 파일 상단 doc comment와 usage 문자열의 `<scheduled|retry|archived>`를 `<scheduled|retry|archived|completed>`로 갱신 (`run` 함수 안 usage 출력도 grep해서 동일 갱신).

(d) **`internal/rdb/inspect.go`의 RunTask/DeleteTask에 completed ZSET 추가** (확인 완료 — 둘 다 3개 ZSET 하드코딩):

`runTaskCmd` Lua를 4-ZSET 버전으로 교체 (KEYS 인덱스가 한 칸씩 밀림에 주의):

```go
// runTaskCmd moves a task from whichever state ZSET holds it into the stream for
// immediate processing. It only promotes a task that was actually removed from a
// state ZSET (scheduled/retry/archived/completed): if the task is not in any of
// them (e.g. it is already pending or in-flight/active), it does nothing. This
// prevents a duplicate stream entry (double execution) and makes concurrent
// RunTask calls safe — only the one that wins the ZREM promotes.
// KEYS[1] scheduled, KEYS[2] retry, KEYS[3] archived, KEYS[4] completed,
// KEYS[5] stream, KEYS[6] task hash.
// ARGV[1] taskID, ARGV[2] pending state.
var runTaskCmd = redis.NewScript(`
local removed = redis.call("ZREM", KEYS[1], ARGV[1]) + redis.call("ZREM", KEYS[2], ARGV[1]) + redis.call("ZREM", KEYS[3], ARGV[1]) + redis.call("ZREM", KEYS[4], ARGV[1])
if removed == 0 then
  return 0
end
if redis.call("EXISTS", KEYS[6]) == 0 then
  return 0
end
redis.call("XADD", KEYS[5], "*", "task_id", ARGV[1])
redis.call("HSET", KEYS[6], "state", ARGV[2])
return 1
`)
```

`RunTask`의 keys를 갱신 (doc comment도 completed 포함으로):

```go
	keys := []string{
		base.ScheduledKey(qname), base.RetryKey(qname), base.ArchivedKey(qname),
		base.CompletedKey(qname),
		base.StreamKey(qname), base.TaskKey(qname, taskID),
	}
```

`DeleteTask`의 TxPipeline에 `pipe.ZRem(ctx, base.CompletedKey(qname), taskID)`를 archived ZRem 아래에 추가하고, doc comment의 상태 나열도 completed 포함으로 갱신.

- [ ] **Step 4: 통과 확인**

Run: `go test . -run TestInspector_CompletedCountAndActions -p 1 -race && go test ./cmd/chronos/ -p 1`
Expected: PASS. 전체 회귀: `make check` → PASS.

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/inspect.go inspector.go cmd/chronos/main.go inspector_test.go cmd/chronos/main_test.go
git commit -m "feat: Inspector/CLI completed 카운트·조회·run/delete"
```

---

## Task 6: cluster 스모크 확장 (신규 Lua 검증)

**Files:**
- Modify: `cluster_test.go`

**전제:** docker 클러스터 필요. `cd deploy/redis-cluster && docker compose up -d && sleep 12` 후 `redis-cli -p 7000 cluster info | head -1`이 `cluster_state:ok`인지 확인. Docker 데몬이 내려가 있으면 사용자에게 요청(BLOCKED 보고).

- [ ] **Step 1: 테스트 추가 + 체크리스트 갱신**

`cluster_test.go` 체크리스트에 줄 추가:

```go
//  [x] completeCmd + TrimCompleted (retention)              → TestCluster_CompletedRetention
```

파일 끝에 테스트 추가:

```go
func TestCluster_CompletedRetention(t *testing.T) {
	client := testutil.NewClusterRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	var done atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[clArgs]) error {
		done.Add(1)
		return nil
	})
	cfg := clusterServerConfig("retq")
	cfg.JanitorInterval = 300 * time.Millisecond
	srv := NewServer(client, cfg)
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	// completeCmd: 성공 후 completed에 보관된다.
	info, err := Enqueue(ctx, c, clArgs{N: 13}, WithQueue("retq"), WithRetention(time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	waitFor(t, 5*time.Second, "task retained as completed", func() bool {
		got, gerr := insp.GetTask(ctx, "retq", info.ID)
		return gerr == nil && got.State == "completed"
	})
	// TrimCompleted: retention(1s) 경과 후 janitor가 정리한다.
	waitFor(t, 10*time.Second, "completed task trimmed", func() bool {
		_, gerr := insp.GetTask(ctx, "retq", info.ID)
		return gerr != nil
	})
}
```

- [ ] **Step 2: 클러스터에서 실행 (13개 전체)**

Run: `REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 go test -run 'TestCluster_' -p 1 -race -count=1 . 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: 13/13 PASS. 2회 실행해 안정성 확인. CROSSSLOT 에러 발생 시 BLOCKED 보고(라이브러리 버그).

- [ ] **Step 3: 커밋**

```bash
git add cluster_test.go
git commit -m "test: cluster 스모크에 completed retention 추가 (completeCmd/TrimCompleted)"
```

---

## Task 7: tour 섹션 11 + README + 최종 검증 + 리뷰 + PR

**Files:**
- Modify: `examples/tour/main.go`, `README.md`

- [ ] **Step 1: tour 섹션 11 추가**

`examples/tour/main.go`에서 섹션 10 종료(`shutPrioCtx` 블록) 뒤, 마지막 `fmt.Println("\n─...")` 앞에 추가:

```go
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
```

파일 상단 doc comment의 기능 나열에 `, and completed-task retention`을 추가. `insp`는 섹션 6에서 이미 선언돼 있어 재사용.

Run: `gofmt -w examples/tour/main.go && go vet ./examples/tour/ && go run ./examples/tour 2>&1 | sed -n '/=== 11)/,$p'`
Expected: `처리 직후 → completed=1`, `janitor 실행 후 → completed=0`.

- [ ] **Step 2: README 갱신**

(a) "Enqueue options" 코드블록에 줄 추가:

```go
	chronos.WithRetention(24*time.Hour),     // keep the completed task for inspection
```

(b) Highlights의 "Self-cleaning" 항목을 다음으로 교체:

```markdown
- **Self-cleaning** — a janitor trims dead-lettered and retained-completed
  tasks by age and count, so Redis memory stays bounded.
```

(c) "Known limitations / roadmap"의 `- Not yet built: completed-task retention, a web UI, workflows (chains/groups).`에서 completed-task retention 제거:

```markdown
- Not yet built: a web UI, workflows (chains/groups).
```

(d) "Handler outcomes"의 success 항목을 갱신:

```markdown
- return `nil` → success (acked and removed — or kept for `WithRetention` for
  later inspection).
```

(e) Observability의 CLI 예시에 한 줄 추가:

```bash
  go run ./cmd/chronos task ls default completed  # inspect retained successes
```

- [ ] **Step 3: 최종 검증**

```bash
make check          # 전체 무회귀
make test-cluster   # 13개 (클러스터 떠 있는 상태)
go run ./examples/tour   # 11개 섹션 눈 확인
```

- [ ] **Step 4: 커밋**

```bash
git add examples/tour/main.go README.md
git commit -m "docs: tour 섹션 11(completed retention) + README 갱신"
```

- [ ] **Step 5: 코드 리뷰 + PR**

k:code-reviewer로 브랜치 전체(`git diff main...HEAD`) 리뷰 — 특히 completeCmd 원자성, Done 분기 후 unique 해제 순서, TrimCompleted 재사용의 정합, RunTask의 completed 경로. 반영 후:

```bash
gh pr create --assignee kenshin579 --title "feat: completed retention (WithRetention — 성공 태스크 보관·조회)" --body "$(cat <<'EOF'
## 배경
성공한 태스크는 즉시 삭제돼 "어젯밤 그 태스크 돌았어?"에 답할 수 없었다(실패만 archived로 관찰 가능한 비대칭). asynq 패리티 항목 중 남은 Retention을 추가한다.

## 변경
- `WithRetention(d)` 태스크별 옵션(기본 0=즉시 삭제, 기존 무변화). TaskMessage에 Retention/CompletedAt.
- `rdb.Done` 분기: retention>0이면 completeCmd Lua로 completed ZSET(score=만료시각) 보관. unique 락은 즉시 해제(보관이 dedup을 막지 않음 — 테스트로 검증).
- janitor `TrimCompleted`(trimArchivedCmd 재사용) + `ServerConfig.MaxCompleted`(기본 10000).
- Inspector/CLI: completed 상태 조회/run(재실행)/delete, QueueInfo.Completed, TaskInfo.CompletedAt.
- cluster 스모크 13개로 확장(신규 Lua 검증), tour 섹션 11, README.

## 테스트 계획
- [x] make check 무회귀
- [x] make test-cluster 13/13
- [x] go run ./examples/tour 섹션 11 눈 확인

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review (계획 작성자 확인 완료)

- **스펙 커버리지**: A 옵션(T4) / B 데이터모델(T1) / C Done 분기(T2) / D janitor(T3+T4) / E Inspector·CLI(T4+T5) / F tour·README(T7) / 테스트 1-6(T2,T4,T5,T6) — 전 항목 매핑.
- **placeholder**: 실제 코드·명령·기대출력 포함. Task 4 Step 1의 초안 스텁 제거 지시 명시. Task 5 Step 3에 RunTask 구현 확인 지시(하드코딩 여부에 따른 분기) 포함 — 구현자가 실제 코드를 읽고 판단할 지점을 명시했으므로 placeholder 아님.
- **타입 일관성**: `Retention int64`(초, T1)를 T2가 `now.Unix()+msg.Retention`으로, T4가 `int64(options.retention / time.Second)`로 사용 — 일치. `TrimCompleted(ctx, qname, now, maxSize, batch)`(T3)를 T4 janitorLoop가 동일 시그니처로 호출. `CompletedAt time.Time`(TaskInfo, T4)를 T5·T6 테스트가 사용.
- **주의**: retention < 1s 클램프(T4 WithRetention)로 "0이 아닌데 0초 저장" 함정 차단.
