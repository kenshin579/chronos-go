# chronos-go retention/janitor (archived 자동 정리) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** dead-letter(archived) 태스크가 무한 증가하는 문제를 해결한다 — janitor 고루틴이 주기적으로 (1) retention TTL이 지난 archived 태스크와 (2) MaxArchived를 초과한 오래된 archived 태스크를 자동 삭제한다. 관찰 스택에서 확인한 "archived 단조 증가"를 톱니 모양으로 안정화한다.

**Architecture:** archived ZSET의 score는 `diedAt.Unix()`(dead-letter된 시각)이다. janitor는 각 큐에 대해 `TrimArchived`를 주기 호출: score < (now - retention)인 항목을 배치로 삭제(나이 기반), 그리고 남은 개수가 MaxArchived를 넘으면 가장 오래된 초과분을 삭제(크기 상한). 삭제 = task HASH DEL + archived ZSET ZREM. **archived 태스크는 Archive 시점에 이미 unique 락이 해제되므로 janitor는 unique를 신경 쓸 필요가 없다.** 삭제 Lua는 원자적이라 여러 인스턴스가 동시에 돌려도 안전(멱등).

**Tech Stack:** Go 1.26, `github.com/redis/go-redis/v9`, 실제 Redis 하니스.

**설계 문서:** `docs/superpowers/specs/2026-07-05-multi-cluster-scheduler-design.md` (M5 운영성; 이 계획은 그 중 retention/janitor).

**범위 밖:** completed 태스크 retention(현재 `Done`이 completed를 즉시 삭제하므로 누수 없음 — completed ZSET 자체가 없다). unique 락 정리(TTL로 만료). heartbeat 마일스톤.

**M1~M4에서 확정된 사실:**
- `base.ArchivedKey(qname)` ZSET, score = `diedAt.Unix()`. `base.TaskKeyPrefix(qname)` = `chronos:{qname}:t:`.
- `rdb.RDB.Archive`가 이미 `releaseUnique` 호출(archived는 unique 락 없음).
- `forward.go`의 `forwardCmd` 패턴(ZRANGEBYSCORE + 키 조립 + ZREM)이 좋은 참고.
- `server.go`: `ServerConfig`(Queues/Concurrency/Logger/RetryDelayFunc/OnDeadLetter/Metrics/ForwardInterval/RecoverInterval/RecoverMinIdle), `Start`가 fetchLoop/forwarderLoop/recovererLoop를 `s.wg.Add(1)`+go로 기동, `Shutdown`이 ctx 취소 + wg.Wait. 상수 `forwardBatchSize=100`.
- `chronos.Inspector.QueueStats`/`Queues`로 archived 카운트 관측 가능.

---

## File Structure

| 파일 | 변경 |
|---|---|
| `internal/rdb/janitor.go` (신규) | `TrimArchived` (나이 cutoff + 크기 상한, Lua) |
| `server.go` (수정) | `ServerConfig`에 `ArchivedRetention`/`MaxArchived`/`JanitorInterval` + `NewServer` 기본값 + `janitorLoop` 기동 |
| `contrib/prometheus/cmd/loadgen/main.go` (수정) | 데모에 짧은 retention 설정(Grafana에서 archived 톱니 관찰) |
| `docs/OBSERVING.md` (수정) | janitor/retention 관찰 노트 |
| 각 `*_test.go` | 신규 테스트 |

**의존 순서:** rdb TrimArchived → server janitorLoop → 데모/문서/통합. 청크: A(rdb+server), B(데모+문서+통합).

**테스트:** 실제 Redis. `make test-race`.

---

## Task 1: rdb — TrimArchived (나이 + 크기 상한)

**Files:**
- Create: `internal/rdb/janitor.go`
- Test: `internal/rdb/janitor_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/janitor_test.go`:
```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

// archiveDirect places a task hash + archived ZSET entry with a given died-at,
// bypassing the normal flow, so retention can be tested deterministically.
func archiveDirect(t *testing.T, client redis.UniversalClient, qname, id string, diedAt time.Time) {
	t.Helper()
	ctx := context.Background()
	msg := &base.TaskMessage{ID: id, Kind: "k", Payload: []byte("{}"), Queue: qname, State: base.StateArchived}
	enc, _ := base.EncodeMessage(msg)
	if err := client.HSet(ctx, base.TaskKey(qname, id), "msg", enc, "state", int(base.StateArchived)).Err(); err != nil {
		t.Fatalf("hset: %v", err)
	}
	if err := client.ZAdd(ctx, base.ArchivedKey(qname), redis.Z{Score: float64(diedAt.Unix()), Member: id}).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}
}

func TestTrimArchived_DeletesExpiredByAge(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	now := time.Now()
	archiveDirect(t, client, "default", "old", now.Add(-2*time.Hour))   // expired
	archiveDirect(t, client, "default", "fresh", now.Add(-1*time.Minute)) // within retention

	cutoff := now.Add(-1 * time.Hour) // older than 1h → delete
	n, err := r.TrimArchived(ctx, "default", cutoff, 10000, 100)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if n != 1 {
		t.Errorf("trimmed = %d, want 1", n)
	}
	// old gone (ZSET + hash), fresh kept.
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "old").Result(); err == nil {
		t.Error("expired 'old' should be removed from archived")
	}
	if ex, _ := client.Exists(ctx, base.TaskKey("default", "old")).Result(); ex != 0 {
		t.Error("expired 'old' task hash should be deleted")
	}
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "fresh").Result(); err != nil {
		t.Error("fresh task should be kept")
	}
}

func TestTrimArchived_EnforcesMaxSize(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	now := time.Now()
	// 5 fresh tasks (none age-expired), staggered died-at so "oldest" is well-defined.
	for i := 0; i < 5; i++ {
		archiveDirect(t, client, "default", string(rune('a'+i)), now.Add(time.Duration(i)*time.Second))
	}
	// cutoff far in the past (nothing age-expired); maxSize=2 → keep 2 newest, delete 3 oldest.
	n, err := r.TrimArchived(ctx, "default", now.Add(-24*time.Hour), 2, 100)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if n != 3 {
		t.Errorf("trimmed = %d, want 3 (over max)", n)
	}
	card, _ := client.ZCard(ctx, base.ArchivedKey("default")).Result()
	if card != 2 {
		t.Errorf("archived size = %d, want 2", card)
	}
	// The 2 newest (d, e) must remain.
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "e").Result(); err != nil {
		t.Error("newest 'e' should be kept")
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestTrimArchived_' -v`
Expected: FAIL — `undefined: (*RDB).TrimArchived`.

- [ ] **Step 3: TrimArchived 구현**

Create `internal/rdb/janitor.go`:
```go
package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// trimArchivedCmd deletes archived tasks in two passes: (1) by age — entries
// with score (died-at) <= cutoff, and (2) by size — if the ZSET still exceeds
// maxSize, the oldest excess entries. For each removed ID it deletes the task
// hash and removes the ZSET member. All keys share the queue hash tag, so the
// multi-key script is cluster-safe.
// KEYS[1] archived zset.
// ARGV[1] cutoff (unix), ARGV[2] max age-batch, ARGV[3] task-key prefix, ARGV[4] maxSize.
var trimArchivedCmd = redis.NewScript(`
local removed = 0

-- (1) age-based: score <= cutoff
local expired = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
for _, id in ipairs(expired) do
  redis.call("DEL", ARGV[3] .. id)
  redis.call("ZREM", KEYS[1], id)
  removed = removed + 1
end

-- (2) size cap: delete oldest entries beyond maxSize
local over = redis.call("ZCARD", KEYS[1]) - tonumber(ARGV[4])
if over > 0 then
  local excess = redis.call("ZRANGE", KEYS[1], 0, over - 1)
  for _, id in ipairs(excess) do
    redis.call("DEL", ARGV[3] .. id)
    redis.call("ZREM", KEYS[1], id)
    removed = removed + 1
  end
end

return removed
`)

// TrimArchived removes dead-lettered tasks that are older than the cutoff, and
// (after that) any that still exceed maxSize (oldest first). ageBatch bounds how
// many age-expired entries a single call removes (keeping each script short);
// the size cap always fully enforces maxSize. Returns the number removed.
// It is safe to call concurrently from multiple instances (removals are atomic
// and idempotent). Archived tasks hold no unique lock (released at archive time),
// so no lock cleanup is needed here.
func (r *RDB) TrimArchived(ctx context.Context, qname string, cutoff time.Time, maxSize, ageBatch int) (int, error) {
	keys := []string{base.ArchivedKey(qname)}
	argv := []interface{}{cutoff.Unix(), ageBatch, base.TaskKeyPrefix(qname), maxSize}
	n, err := trimArchivedCmd.Run(ctx, r.client, keys, argv...).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}
```

- [ ] **Step 4: 통과 확인 + 커밋**

Run: `go test ./internal/rdb/ -run 'TestTrimArchived_' -race -v`
Expected: PASS

```bash
git add internal/rdb/janitor.go internal/rdb/janitor_test.go
git commit -m "feat: rdb TrimArchived (archived 나이 기반 + 크기 상한 정리, Lua)"
```

---

## Task 2: server — janitorLoop

**Files:**
- Modify: `server.go`
- Test: `server_janitor_test.go` (신규)

- [ ] **Step 1: 실패하는 테스트 작성**

Create `server_janitor_test.go`:
```go
package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_JanitorCleansExpiredArchived(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	// Seed an archived task that died 1h ago.
	msg := &base.TaskMessage{ID: "old", Kind: "k", Payload: []byte("{}"), Queue: "default", State: base.StateArchived}
	enc, _ := base.EncodeMessage(msg)
	client.HSet(ctx, base.TaskKey("default", "old"), "msg", enc, "state", int(base.StateArchived))
	client.ZAdd(ctx, base.ArchivedKey("default"), redis.Z{Score: float64(time.Now().Add(-time.Hour).Unix()), Member: "old"})

	srv := NewServer(client, ServerConfig{
		Queues:            map[string]int{"default": 1},
		Concurrency:       2,
		ArchivedRetention: 1 * time.Minute,        // died >1m ago → expired
		JanitorInterval:   100 * time.Millisecond, // clean fast for the test
	})
	if err := srv.Start(ctx, NewMux()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	eventually(t, 5*time.Second, func() bool {
		card, _ := client.ZCard(ctx, base.ArchivedKey("default")).Result()
		return card == 0
	}, "janitor should delete the expired archived task")
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run TestServer_JanitorCleansExpiredArchived -v`
Expected: FAIL — `unknown field ArchivedRetention/JanitorInterval`.

- [ ] **Step 3: ServerConfig 확장 + 기본값 + janitorLoop 기동**

`server.go`의 `ServerConfig`에 필드 추가(`RecoverMinIdle` 아래):
```go
	// ArchivedRetention is how long a dead-lettered (archived) task is kept
	// before the janitor deletes it. Defaults to 7 days (168h).
	ArchivedRetention time.Duration
	// MaxArchived caps the number of archived tasks per queue; the janitor
	// deletes the oldest beyond this even within the retention window. Defaults
	// to 10000. Set negative to disable the size cap.
	MaxArchived int
	// JanitorInterval is how often the janitor runs. Defaults to 1 minute.
	JanitorInterval time.Duration
```

`NewServer`의 기본값 채우는 블록에 추가(다른 기본값들 옆):
```go
	if cfg.ArchivedRetention <= 0 {
		cfg.ArchivedRetention = 7 * 24 * time.Hour
	}
	if cfg.MaxArchived == 0 {
		cfg.MaxArchived = 10000
	}
	if cfg.JanitorInterval <= 0 {
		cfg.JanitorInterval = 1 * time.Minute
	}
```
(주의: `MaxArchived`는 `== 0`일 때만 기본값 10000을 넣는다 — 음수는 "상한 비활성"으로 사용자가 명시한 값이므로 존중.)

`server.go`의 배치 상수 근처에 janitor 배치 상수 추가:
```go
// janitorBatchSize bounds how many age-expired archived tasks one janitor tick
// removes per queue.
const janitorBatchSize = 100
```

`Start`에서 recovererLoop 기동 다음에 janitor 기동 추가:
```go
	s.wg.Add(1)
	go s.janitorLoop(runCtx)
```

`server.go`에 janitorLoop 구현(recovererLoop 근처):
```go
// janitorLoop periodically deletes expired / over-capacity archived tasks from
// each queue. Removals are atomic and idempotent, so running it on every server
// instance is safe. maxSize < 0 disables the size cap.
func (s *Server) janitorLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.JanitorInterval)
	defer ticker.Stop()
	queues := s.queueNames()
	maxSize := s.cfg.MaxArchived
	if maxSize < 0 {
		maxSize = 1<<62 - 1 // effectively unbounded
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-s.cfg.ArchivedRetention)
			for _, q := range queues {
				if _, err := s.rdb.TrimArchived(ctx, q, cutoff, maxSize, janitorBatchSize); err != nil {
					if ctx.Err() != nil {
						return
					}
					s.logger.Error("chronos: janitor trim failed", "queue", q, "error", err)
				}
			}
		}
	}
}
```

- [ ] **Step 4: 통과 확인 + 회귀 + 커밋**

Run: `go test . -run TestServer_JanitorCleansExpiredArchived -race -v && go test ./... -race -p 1`
Expected: PASS (전 패키지).

```bash
git add server.go server_janitor_test.go
git commit -m "feat: server janitorLoop — archived retention/size 자동 정리"
```

---

## Task 3: 데모 짧은 retention + 문서 + 통합

**Files:**
- Modify: `contrib/prometheus/cmd/loadgen/main.go`, `docs/OBSERVING.md`
- Test: `integration_test.go` (테스트 추가; 기존 파일)

- [ ] **Step 1: 통합 테스트 — 부하 중 archived가 안정화되는지**

`integration_test.go`에 추가(같은 패키지 `chronos`):
```go
func TestIntegration_ArchivedStabilizesUnderRetention(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()

	// Handler always fails → every task dead-letters (fast archived growth).
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[poisonArgs]) error {
		return errors.New("always fails")
	})
	srv := NewServer(client, ServerConfig{
		Queues:            map[string]int{"default": 1},
		Concurrency:       4,
		ForwardInterval:   50 * time.Millisecond,
		RetryDelayFunc:    func(retried int, err error) time.Duration { return 20 * time.Millisecond },
		ArchivedRetention: 1 * time.Second,        // very short so archived drains quickly
		JanitorInterval:   100 * time.Millisecond, // clean often
	})
	ctx := context.Background()
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	insp := NewInspector(client)
	// Continuously enqueue failing tasks for ~2s.
	stop := time.Now().Add(2 * time.Second)
	for i := 0; time.Now().Before(stop); i++ {
		_, _ = Enqueue(ctx, c, poisonArgs{ID: i}, WithMaxRetry(0)) // MaxRetry 0 → immediate dead-letter
		time.Sleep(20 * time.Millisecond)
	}

	// With a 1s retention + frequent janitor, archived must not grow unbounded:
	// after letting the janitor run past the retention window, it should be small.
	eventually(t, 6*time.Second, func() bool {
		qs, err := insp.Queues(ctx)
		if err != nil || len(qs) == 0 {
			return false
		}
		return qs[0].Archived <= 5 // drained close to zero (retention 1s ≫ nothing lingers)
	}, "archived should stabilize (not grow unbounded) under short retention")
}
```
(주의: `poisonArgs`는 `reliability_integration_test.go`에 이미 정의돼 있다. import에 `errors`가 없으면 추가.)

- [ ] **Step 2: 통합 테스트 통과 확인**

Run: `go test . -run TestIntegration_ArchivedStabilizesUnderRetention -race -v`
Expected: PASS — archived가 무한 증가하지 않고 5 이하로 안정.

- [ ] **Step 3: loadgen 데모에 짧은 retention 설정**

`contrib/prometheus/cmd/loadgen/main.go`의 `chronos.NewServer(...)` `ServerConfig`에 필드 추가(Grafana에서 archived 톱니 관찰용):
```go
		Metrics:           metrics,
		ArchivedRetention: 30 * time.Second, // demo: archived drains so the graph sawtooths instead of climbing
		JanitorInterval:   5 * time.Second,
```
(기존 `Metrics: metrics,` 줄 뒤에 두 줄 추가. `ServerConfig` 리터럴 안.)

- [ ] **Step 4: 데모 빌드 확인**

Run: `cd contrib/prometheus && go build ./... && cd ../..`
Expected: 빌드 성공.

- [ ] **Step 5: OBSERVING.md에 janitor 노트 추가**

`docs/OBSERVING.md`의 "2c. 스케줄러 리더 관찰" 다음에 추가:
```markdown
## 2d. Retention / janitor (archived 자동 정리)

dead-letter(archived) 태스크는 `ServerConfig.ArchivedRetention`(기본 7일) 경과 후,
또는 `MaxArchived`(기본 10000) 초과 시 오래된 것부터 janitor가 자동 삭제한다. 그래서
`chronos_queue_tasks{state="archived"}`는 무한 증가하지 않고 톱니 모양으로 안정화된다.

- 짧게 설정해 관찰: `ArchivedRetention: 30*time.Second, JanitorInterval: 5*time.Second`
- 수동 정리는 여전히 가능: `chronos task rm <queue> <id>` / `task run`으로 재실행
- janitor는 원자적·멱등이라 모든 서버 인스턴스에서 돌아도 안전하다(리더 불필요).

관찰:
```bash
redis-cli -n 0 ZCARD 'chronos:{default}:archived'   # 시간이 지나면 오르내림(톱니)
```
```
(코드블록 백틱은 일반 백틱 3개)

- [ ] **Step 6: 전체 검증 + 커밋**

Run: `make check`
Expected: gofmt/vet 클린, `go test ./... -race -p 1` 전 패키지 PASS, contrib 빌드/테스트 PASS.

```bash
git add integration_test.go contrib/prometheus/cmd/loadgen/main.go docs/OBSERVING.md
git commit -m "test/docs: archived retention 통합 테스트 + 데모 짧은 retention + 관찰 가이드"
```

---

## 완료 기준

- [ ] `make check`(메인 + contrib) 통과
- [ ] janitor가 retention 경과 archived 태스크를 자동 삭제(task hash + ZSET)
- [ ] MaxArchived 초과 시 오래된 것부터 삭제(크기 상한)
- [ ] 부하 중에도 archived가 무한 증가하지 않고 안정화(통합 테스트)
- [ ] 데모/문서에 retention 반영 → Grafana archived 패널이 톱니 모양

**다음 단계:** 남은 큰 조각은 heartbeat 마일스톤(lease/unique TTL 연장 — recoverer의 in-flight 오회수와 unique 락 TTL 상한 해결). 그 후 operator-review 실제 교체 검토.
