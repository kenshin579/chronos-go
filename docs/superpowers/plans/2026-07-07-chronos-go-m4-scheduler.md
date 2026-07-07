# chronos-go M4 (리더 선출 + 스케줄러 + 결정적 TaskID + misfire) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** asynq의 최대 결함(다중 인스턴스에서 스케줄러가 태스크를 중복 enqueue)을 해결한다 — Redis 리더 선출로 **리더 인스턴스만** cron/interval 잡을 enqueue하고, **결정적 TaskID**로 리더 교체 경계의 중복까지 이중 차단하며, 리더 공백 후 놓친 실행은 misfire 정책(Skip/FireOnce)으로 처리한다.

**Architecture:** 모든 인스턴스가 `Scheduler.Start`를 호출해도 안전하다. 각 인스턴스는 Redis 락(`SET NX PX`)으로 리더 선출에 참여하고, 리더만 tick 루프에서 due 잡을 enqueue한다. enqueue는 결정적 dedup 키(`<scheduleID>:<trigger_unix>`)로 `SET NX`하므로, split-brain으로 두 인스턴스가 잠깐 동시에 리더라 믿어도 같은 트리거는 한 번만 enqueue된다(fencing). 리더가 graceful shutdown하면 락을 반납하고 pub/sub로 즉시 재선출을 알린다. 놓친 트리거는 `computeFires`(순수 함수)가 lastFired 기준으로 계산하고 정책에 따라 skip/catch-up한다.

**Tech Stack:** Go 1.26, `github.com/redis/go-redis/v9`, `github.com/robfig/cron/v3`(cron/interval 파싱), 실제 Redis 테스트 하니스.

**설계 문서:** `docs/superpowers/specs/2026-07-05-multi-cluster-scheduler-design.md` (섹션 4 스케줄 API, 섹션 5 misfire·리더 페일오버)

**M4 범위 밖:** Prometheus 메트릭(별도), heartbeat 기반 lease/unique TTL 연장(별도 마일스톤). `MisfireRunAll`(놓친 틱 전부 재실행)은 폭주 위험으로 미제공(설계 결정).

**M1~M3/Inspector에서 확정된 실제 시그니처:**
- `rdb.NewRDB(client)`, `rdb.RDB.Client()`, `base.QueueKeyPrefix/StreamKey/TaskKey/QueuesKey`
- `rdb.RDB.Enqueue(ctx, msg)` — HSET + XADD (dedup 가드 없음)
- unique 패턴: `enqueueUniqueCmd`가 `SET NX PX` 후 HSET+XADD, `uniqueTTLMillis(ttl)` 헬퍼 존재
- `base.TaskMessage{ID, Kind, Payload, Queue, State, Retried, MaxRetry, NoArchive, UniqueKey}`
- `chronos.go`: `enqueueOptions{queue, taskID, maxRetry, noArchive, processAt, uniqueTTL}`, `Option`/`optionFunc`, `WithQueue/WithMaxRetry/WithDeadLetterDiscard`, `TaskArgs`, `encodeArgs`, `DefaultQueue`, `DefaultMaxRetry`
- `server.go` 패턴 참고: goroutine + `sync.WaitGroup` + `context.WithCancel` + Shutdown이 wg.Wait를 ctx로 bound
- go.mod에 robfig/cron 없음 → 추가 필요

---

## File Structure

| 파일 | 변경 |
|---|---|
| `internal/base/keys.go` | `LeaderKey`, `LeaderResignChannel`, `PeriodicDedupKey`, `ScheduleLastFiredKey` 추가 |
| `internal/rdb/leader.go` (신규) | `AcquireOrRenewLeadership`, `ResignLeadership` (Lua) |
| `internal/rdb/periodic.go` (신규) | `EnqueuePeriodic` (결정적 dedup), `SetLastFired`/`GetLastFired` |
| `chronos.go` | `enqueueOptions.misfire` 필드 + `WithMisfirePolicy` 옵션 |
| `schedule.go` (신규, 루트) | `MisfirePolicy`, `computeFires` 순수 함수 |
| `scheduler.go` (신규, 루트) | `Scheduler`, `SchedulerConfig`, `RegisterInterval`/`RegisterCron`, 리더 elector + tick 루프, Start/Shutdown |
| 각 `*_test.go` | 신규 테스트 |

**의존 순서:** base 키 → rdb(leader, periodic) → chronos 옵션 + computeFires → Scheduler 조립 → 통합. 청크: A(리더+periodic rdb), B(misfire 순수함수 + Scheduler), C(통합+문서).

**테스트:** 실제 Redis. `make test-race`. computeFires는 Redis 불필요(순수 함수).

---

## Task 1: base 키 + rdb 리더 선출

**Files:**
- Modify: `internal/base/keys.go`
- Create: `internal/rdb/leader.go`
- Test: `internal/base/keys_test.go`, `internal/rdb/leader_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

`internal/base/keys_test.go`에 추가:
```go
func TestLeaderAndPeriodicKeys(t *testing.T) {
	if LeaderKey() != "chronos:leader" {
		t.Errorf("LeaderKey = %q", LeaderKey())
	}
	if LeaderResignChannel() != "chronos:leader:resign" {
		t.Errorf("LeaderResignChannel = %q", LeaderResignChannel())
	}
	if got := PeriodicDedupKey("default", "job:1:1700000000"); got != "chronos:{default}:pdedup:job:1:1700000000" {
		t.Errorf("PeriodicDedupKey = %q", got)
	}
	if got := ScheduleLastFiredKey("job:1"); got != "chronos:sched:job:1:last" {
		t.Errorf("ScheduleLastFiredKey = %q", got)
	}
}
```

Create `internal/rdb/leader_test.go`:
```go
package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestLeadership_SingleWinnerThenRenew(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// A acquires; B loses while A holds.
	okA, err := r.AcquireOrRenewLeadership(ctx, "A", time.Second)
	if err != nil || !okA {
		t.Fatalf("A acquire: ok=%v err=%v", okA, err)
	}
	okB, err := r.AcquireOrRenewLeadership(ctx, "B", time.Second)
	if err != nil {
		t.Fatalf("B: %v", err)
	}
	if okB {
		t.Error("B should not become leader while A holds")
	}
	// A renews (still leader).
	okA2, _ := r.AcquireOrRenewLeadership(ctx, "A", time.Second)
	if !okA2 {
		t.Error("A should renew and stay leader")
	}
}

func TestLeadership_FailoverAfterExpiry(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if ok, _ := r.AcquireOrRenewLeadership(ctx, "A", 200*time.Millisecond); !ok {
		t.Fatal("A acquire")
	}
	time.Sleep(300 * time.Millisecond) // let A's lock expire (A "died")
	if ok, _ := r.AcquireOrRenewLeadership(ctx, "B", time.Second); !ok {
		t.Error("B should take over after A's lock expires")
	}
}

func TestResignLeadership_ReleasesForOthers(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	r.AcquireOrRenewLeadership(ctx, "A", time.Minute)
	if err := r.ResignLeadership(ctx, "A"); err != nil {
		t.Fatalf("resign: %v", err)
	}
	// B can immediately acquire after A resigns.
	if ok, _ := r.AcquireOrRenewLeadership(ctx, "B", time.Minute); !ok {
		t.Error("B should acquire immediately after A resigns")
	}
	// A resigning again (not owner) is a no-op, not an error.
	if err := r.ResignLeadership(ctx, "A"); err != nil {
		t.Fatalf("resign non-owner: %v", err)
	}
	if ok, _ := r.AcquireOrRenewLeadership(ctx, "B", time.Minute); ok {
		t.Error("A's second resign must not have released B's lock")
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/base/ -run TestLeaderAndPeriodicKeys -v && go test ./internal/rdb/ -run 'TestLeadership_|TestResignLeadership_' -v`
Expected: FAIL — `undefined: LeaderKey` 등, `AcquireOrRenewLeadership`/`ResignLeadership`.

- [ ] **Step 3: base 키 추가**

`internal/base/keys.go` 끝에 추가:
```go
// LeaderKey is the STRING key holding the current scheduler leader's instance ID.
func LeaderKey() string { return "chronos:leader" }

// LeaderResignChannel is the pub/sub channel a leader publishes to on graceful
// resignation so followers can re-elect immediately instead of waiting for TTL.
func LeaderResignChannel() string { return "chronos:leader:resign" }

// PeriodicDedupKey is the STRING key used to deduplicate a single scheduled
// trigger. id is "<scheduleID>:<trigger_unix>". Wrapped in the queue hash tag so
// it shares the queue's slot.
func PeriodicDedupKey(qname, id string) string {
	return QueueKeyPrefix(qname) + "pdedup:" + id
}

// ScheduleLastFiredKey is the STRING key holding the unix time a schedule last
// fired, used to compute missed triggers across leader changes. It is global
// (no queue hash tag) because a schedule is not tied to one queue's slot.
func ScheduleLastFiredKey(scheduleID string) string {
	return "chronos:sched:" + scheduleID + ":last"
}
```

- [ ] **Step 4: rdb 리더 선출 구현**

Create `internal/rdb/leader.go`:
```go
package rdb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// acquireOrRenewCmd acquires leadership if vacant, or renews it if already held
// by this instance. Returns 1 if the caller is the leader after the call, 0 if
// another instance holds it.
// KEYS[1] leader key. ARGV[1] instanceID, ARGV[2] ttl millis.
var acquireOrRenewCmd = redis.NewScript(`
local cur = redis.call("GET", KEYS[1])
if cur == false then
  redis.call("SET", KEYS[1], ARGV[1], "PX", tonumber(ARGV[2]))
  return 1
elseif cur == ARGV[1] then
  redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))
  return 1
else
  return 0
end
`)

// resignCmd releases leadership only if this instance still holds it.
// KEYS[1] leader key. ARGV[1] instanceID.
var resignCmd = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("DEL", KEYS[1])
  return 1
end
return 0
`)

// AcquireOrRenewLeadership makes instanceID the leader (or renews its term) with
// the given TTL. Returns true if instanceID is the leader after the call.
func (r *RDB) AcquireOrRenewLeadership(ctx context.Context, instanceID string, ttl time.Duration) (bool, error) {
	ms := ttl.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	res, err := acquireOrRenewCmd.Run(ctx, r.client, []string{base.LeaderKey()}, instanceID, ms).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// ResignLeadership releases leadership if instanceID holds it, then notifies
// followers via pub/sub so they can re-elect immediately. Releasing when not the
// owner is a no-op.
func (r *RDB) ResignLeadership(ctx context.Context, instanceID string) error {
	if err := resignCmd.Run(ctx, r.client, []string{base.LeaderKey()}, instanceID).Err(); err != nil {
		return err
	}
	return r.client.Publish(ctx, base.LeaderResignChannel(), instanceID).Err()
}

// SubscribeResign returns a pub/sub subscription to the leader-resign channel.
func (r *RDB) SubscribeResign(ctx context.Context) *redis.PubSub {
	return r.client.Subscribe(ctx, base.LeaderResignChannel())
}
```

- [ ] **Step 5: 통과 확인 + 커밋**

Run: `go test ./internal/base/ ./internal/rdb/ -race`
Expected: PASS

```bash
git add internal/base/ internal/rdb/leader.go internal/rdb/leader_test.go
git commit -m "feat: rdb 리더 선출 (AcquireOrRenew/Resign) + leader/periodic 키"
```

---

## Task 2: rdb — EnqueuePeriodic (결정적 dedup) + lastFired

**Files:**
- Create: `internal/rdb/periodic.go`
- Test: `internal/rdb/periodic_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `internal/rdb/periodic_test.go`:
```go
package rdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestEnqueuePeriodic_DedupsSameTrigger(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "job:1:1700000000", Kind: "job", Payload: []byte("{}"), Queue: "default"}
	dedupKey := base.PeriodicDedupKey("default", "job:1:1700000000")

	// First enqueue for this trigger succeeds.
	if err := r.EnqueuePeriodic(ctx, msg, dedupKey, time.Minute); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second enqueue for the SAME trigger (e.g. a split-brain second leader) is rejected.
	if err := r.EnqueuePeriodic(ctx, msg, dedupKey, time.Minute); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second: err=%v, want ErrDuplicateTask", err)
	}
	// Exactly one stream entry.
	if n, _ := client.XLen(ctx, base.StreamKey("default")).Result(); n != 1 {
		t.Errorf("stream len = %d, want 1", n)
	}
	// The dedup key is NOT this task's UniqueKey, so Done must not release it
	// (it expires by TTL only).
	if msg.UniqueKey != "" {
		t.Errorf("periodic dedup must not set UniqueKey, got %q", msg.UniqueKey)
	}
}

func TestLastFired_RoundTrip(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// Absent → zero time, ok=false.
	_, ok, err := r.GetLastFired(ctx, "job:1")
	if err != nil {
		t.Fatalf("get absent: %v", err)
	}
	if ok {
		t.Error("absent lastFired should report ok=false")
	}

	when := time.Unix(1700000000, 0)
	if err := r.SetLastFired(ctx, "job:1", when); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, ok, err := r.GetLastFired(ctx, "job:1")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if !got.Equal(when) {
		t.Errorf("lastFired = %v, want %v", got, when)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test ./internal/rdb/ -run 'TestEnqueuePeriodic_DedupsSameTrigger|TestLastFired_RoundTrip' -v`
Expected: FAIL — `undefined: (*RDB).EnqueuePeriodic/SetLastFired/GetLastFired`.

- [ ] **Step 3: 구현**

Create `internal/rdb/periodic.go`:
```go
package rdb

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
)

// enqueuePeriodicCmd acquires a per-trigger dedup key (SET NX PX) and, only on
// success, stores the task and appends it to the stream. Returns -1 if the
// trigger was already enqueued. Unlike the unique lock, the dedup key is NOT
// recorded on the task, so it is never released early — it expires by TTL only,
// which is what fences a split-brain / late leader from re-enqueueing the same
// trigger.
// KEYS[1] dedup key, KEYS[2] task hash, KEYS[3] stream.
// ARGV[1] taskID, ARGV[2] dedup ttl millis, ARGV[3] encoded msg, ARGV[4] state.
var enqueuePeriodicCmd = redis.NewScript(`
if redis.call("SET", KEYS[1], "1", "NX", "PX", tonumber(ARGV[2])) == false then
  return -1
end
redis.call("HSET", KEYS[2], "msg", ARGV[3], "state", ARGV[4])
redis.call("XADD", KEYS[3], "*", "task_id", ARGV[1])
return 1
`)

// EnqueuePeriodic enqueues a scheduled trigger's task exactly once per dedupKey.
// Returns ErrDuplicateTask if the trigger was already enqueued (by this or
// another instance).
func (r *RDB) EnqueuePeriodic(ctx context.Context, msg *base.TaskMessage, dedupKey string, dedupTTL time.Duration) error {
	msg.State = base.StatePending
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if err := r.client.SAdd(ctx, base.QueuesKey(), msg.Queue).Err(); err != nil {
		return err
	}
	ms := dedupTTL.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	keys := []string{dedupKey, base.TaskKey(msg.Queue, msg.ID), base.StreamKey(msg.Queue)}
	res, err := enqueuePeriodicCmd.Run(ctx, r.client, keys, msg.ID, ms, encoded, int(base.StatePending)).Int()
	if err != nil {
		return err
	}
	if res == -1 {
		return ErrDuplicateTask
	}
	return nil
}

// SetLastFired records the unix time a schedule last fired.
func (r *RDB) SetLastFired(ctx context.Context, scheduleID string, when time.Time) error {
	return r.client.Set(ctx, base.ScheduleLastFiredKey(scheduleID), when.Unix(), 0).Err()
}

// GetLastFired returns the time a schedule last fired. ok is false if unset.
func (r *RDB) GetLastFired(ctx context.Context, scheduleID string) (time.Time, bool, error) {
	raw, err := r.client.Get(ctx, base.ScheduleLastFiredKey(scheduleID)).Result()
	if err == redis.Nil {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	sec, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, false, err
	}
	return time.Unix(sec, 0), true, nil
}
```

- [ ] **Step 4: 통과 확인 + 커밋**

Run: `go test ./internal/rdb/ -run 'TestEnqueuePeriodic_DedupsSameTrigger|TestLastFired_RoundTrip' -race -v`
Expected: PASS

```bash
git add internal/rdb/periodic.go internal/rdb/periodic_test.go
git commit -m "feat: rdb EnqueuePeriodic(결정적 dedup) + lastFired 추적"
```

---

## Task 3: misfire 순수 함수 + WithMisfirePolicy

`computeFires`는 Redis 없이 "lastFired와 now, 정책이 주어졌을 때 어떤 트리거 시각들을 enqueue하고 새 lastFired는 무엇인가"를 계산하는 순수 함수다. 스케줄러 정확성의 핵심이라 단위 테스트로 두껍게 검증한다.

**Files:**
- Create: `schedule.go`
- Modify: `chronos.go` (enqueueOptions.misfire + WithMisfirePolicy)
- Test: `schedule_test.go`

- [ ] **Step 1: 실패하는 테스트 작성**

Create `schedule_test.go`:
```go
package chronos

import (
	"testing"
	"time"
)

// everySecond is a trivial schedule: next trigger is the next whole second after t.
type everySecond struct{}

func (everySecond) Next(t time.Time) time.Time {
	return t.Truncate(time.Second).Add(time.Second)
}

func TestComputeFires_NormalTick_FiresOnce(t *testing.T) {
	sched := everySecond{}
	last := time.Unix(100, 0)
	now := time.Unix(101, 0) // exactly one trigger (101) is due
	fires, newLast := computeFires(sched, last, now, MisfireSkip)
	if len(fires) != 1 || !fires[0].Equal(time.Unix(101, 0)) {
		t.Fatalf("fires = %v, want [101]", fires)
	}
	if !newLast.Equal(time.Unix(101, 0)) {
		t.Errorf("newLast = %v, want 101", newLast)
	}
}

func TestComputeFires_NotDue_NoFire(t *testing.T) {
	fires, newLast := computeFires(everySecond{}, time.Unix(101, 0), time.Unix(101, 0), MisfireSkip)
	if len(fires) != 0 {
		t.Errorf("fires = %v, want none", fires)
	}
	if !newLast.Equal(time.Unix(101, 0)) {
		t.Errorf("newLast = %v, want unchanged 101", newLast)
	}
}

func TestComputeFires_Gap_Skip_FiresLatestOnly(t *testing.T) {
	// last=100, now=105 → triggers 101,102,103,104,105 missed. Skip fires none of
	// the missed ones and fast-forwards lastFired to the latest trigger (105).
	fires, newLast := computeFires(everySecond{}, time.Unix(100, 0), time.Unix(105, 0), MisfireSkip)
	if len(fires) != 0 {
		t.Errorf("Skip should not fire missed triggers, got %v", fires)
	}
	if !newLast.Equal(time.Unix(105, 0)) {
		t.Errorf("newLast = %v, want 105 (fast-forwarded)", newLast)
	}
}

func TestComputeFires_Gap_FireOnce_FiresLatestOnce(t *testing.T) {
	// Same gap; FireOnce catches up with exactly one fire (for the latest trigger).
	fires, newLast := computeFires(everySecond{}, time.Unix(100, 0), time.Unix(105, 0), MisfireFireOnce)
	if len(fires) != 1 || !fires[0].Equal(time.Unix(105, 0)) {
		t.Fatalf("FireOnce fires = %v, want [105]", fires)
	}
	if !newLast.Equal(time.Unix(105, 0)) {
		t.Errorf("newLast = %v, want 105", newLast)
	}
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `go test . -run 'TestComputeFires_' -v`
Expected: FAIL — `undefined: computeFires`, `MisfireSkip`.

- [ ] **Step 3: schedule.go 구현**

Create `schedule.go`:
```go
package chronos

import "time"

// MisfirePolicy decides what happens when a schedule's triggers were missed
// (e.g. during a leader-election gap or downtime).
type MisfirePolicy int

const (
	// MisfireSkip (default) discards missed triggers and resumes on schedule.
	MisfireSkip MisfirePolicy = iota
	// MisfireFireOnce fires exactly one catch-up trigger if any were missed.
	MisfireFireOnce
)

// cronSchedule is the minimal schedule interface computeFires needs. It matches
// robfig/cron's Schedule (Next returns the first activation time after t).
type cronSchedule interface {
	Next(time.Time) time.Time
}

// computeFires determines which trigger times to enqueue given the last fired
// time, the current time, and the misfire policy. It returns the trigger times
// to enqueue (0, or 1) and the new lastFired to persist.
//
//   - Nothing due (next trigger after now): no fire, lastFired unchanged.
//   - Exactly one trigger due (normal tick): fire it.
//   - Multiple triggers elapsed (a gap): Skip fires none and fast-forwards to the
//     latest elapsed trigger; FireOnce fires once (for the latest) then resumes.
//
// It never returns more than one trigger — chronos does not replay every missed
// tick (that risks a flood); MisfireRunAll is intentionally unsupported.
func computeFires(s cronSchedule, lastFired, now time.Time, policy MisfirePolicy) (fires []time.Time, newLastFired time.Time) {
	next := s.Next(lastFired)
	if next.After(now) {
		return nil, lastFired // nothing due yet
	}

	// Find the latest trigger that is <= now.
	latest := next
	for {
		n := s.Next(latest)
		if n.After(now) {
			break
		}
		latest = n
	}

	gap := latest.After(next) // more than one trigger elapsed
	switch {
	case !gap:
		return []time.Time{next}, next // normal single tick
	case policy == MisfireFireOnce:
		return []time.Time{latest}, latest // one catch-up
	default: // MisfireSkip
		return nil, latest // discard missed, fast-forward
	}
}
```

- [ ] **Step 4: WithMisfirePolicy 옵션 추가**

`chronos.go`의 `enqueueOptions`에 필드 추가:
```go
	misfire MisfirePolicy // used by scheduler registrations only
```

`WithUnique` 근처에 추가:
```go
// WithMisfirePolicy sets how a scheduled job handles missed triggers (after a
// leader-election gap or downtime). Only meaningful for RegisterInterval /
// RegisterCron; ignored by a plain Enqueue. Defaults to MisfireSkip.
func WithMisfirePolicy(p MisfirePolicy) Option {
	return optionFunc(func(o *enqueueOptions) { o.misfire = p })
}
```

- [ ] **Step 5: 통과 확인 + 커밋**

Run: `go test . -run 'TestComputeFires_' -v && go build ./...`
Expected: PASS, 빌드 성공.

```bash
git add schedule.go chronos.go schedule_test.go
git commit -m "feat: misfire 정책(computeFires 순수 함수) + WithMisfirePolicy"
```

---

## Task 4: Scheduler 조립 (리더 elector + tick 루프)

**Files:**
- Create: `scheduler.go`
- Test: `scheduler_test.go`
- Modify: `go.mod`/`go.sum` (robfig/cron/v3 추가)

- [ ] **Step 1: robfig/cron/v3 의존성 추가**

Run: `go get github.com/robfig/cron/v3@latest`
Expected: go.mod require에 `github.com/robfig/cron/v3` 추가.

- [ ] **Step 2: 실패하는 테스트 작성**

Create `scheduler_test.go`:
```go
package chronos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type tickArgs struct {
	N int `json:"n"`
}

func (tickArgs) Kind() string { return "sched:tick" }

func TestRegisterInterval_RejectsSubSecond(t *testing.T) {
	client := testutil.NewRedis(t)
	s := NewScheduler(client, SchedulerConfig{})
	if err := RegisterInterval(s, 500*time.Millisecond, tickArgs{}); err == nil {
		t.Error("interval < 1s must be rejected")
	}
	if err := RegisterInterval(s, 1*time.Second, tickArgs{}); err != nil {
		t.Errorf("1s interval should be accepted: %v", err)
	}
}

func TestRegisterCron_RejectsBadSpec(t *testing.T) {
	client := testutil.NewRedis(t)
	s := NewScheduler(client, SchedulerConfig{})
	if err := RegisterCron(s, "not a cron", tickArgs{}); err == nil {
		t.Error("bad cron spec must be rejected")
	}
}

// Two schedulers on the same Redis, both registering the same 1s interval job,
// must together enqueue each trigger only once (leader-only + deterministic dedup).
func TestScheduler_SingleExecutionAcrossInstances(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	// A consuming client + server counts how many tasks actually run.
	c := NewClient(client)
	defer c.Close()
	var runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[tickArgs]) error {
		runs.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	newSched := func() *Scheduler {
		s := NewScheduler(client, SchedulerConfig{LeaderTTL: 500 * time.Millisecond})
		if err := RegisterInterval(s, 1*time.Second, tickArgs{N: 1}); err != nil {
			t.Fatalf("register: %v", err)
		}
		return s
	}
	s1, s2 := newSched(), newSched()
	if err := s1.Start(ctx); err != nil {
		t.Fatalf("s1 start: %v", err)
	}
	if err := s2.Start(ctx); err != nil {
		t.Fatalf("s2 start: %v", err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { s1.Shutdown(context.Background()); s2.Shutdown(context.Background()) }) }
	defer stop()

	// Over ~3.5s a 1s job should fire ~3 times — and crucially the same number
	// whether one or two schedulers are running (no duplication).
	time.Sleep(3500 * time.Millisecond)
	stop()
	time.Sleep(300 * time.Millisecond) // let in-flight finish

	got := runs.Load()
	if got < 2 || got > 5 {
		t.Errorf("runs = %d, want ~3 (2 schedulers must not double-enqueue)", got)
	}
}
```

- [ ] **Step 3: Scheduler 구현**

Create `scheduler.go`:
```go
package chronos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// SchedulerConfig configures a Scheduler.
type SchedulerConfig struct {
	// Location is the timezone for cron schedules. Defaults to time.Local.
	Location *time.Location
	// Logger receives operational logs. Defaults to slog.Default().
	Logger *slog.Logger
	// LeaderTTL is how long a leadership term lasts before it must be renewed.
	// Defaults to 5s. Failover happens within ~LeaderTTL of a leader dying.
	LeaderTTL time.Duration
}

// scheduleEntry is one registered cron/interval job.
type scheduleEntry struct {
	id       string // stable ID: "<kind>:<spec>"
	kind     string
	payload  []byte
	queue    string
	maxRetry int
	noArch   bool
	misfire  MisfirePolicy
	schedule cronSchedule
	next     time.Time // in-memory next trigger (leader only)
}

// Scheduler registers periodic jobs and, on whichever instance is elected
// leader, enqueues their due triggers. Every instance may call Start; only the
// leader enqueues, and deterministic dedup keys prevent double-enqueue during
// leader handover.
type Scheduler struct {
	rdb      *rdb.RDB
	cfg      SchedulerConfig
	instance string
	logger   *slog.Logger

	entries  []*scheduleEntry
	isLeader atomic.Bool
	wg       sync.WaitGroup
	cancel   context.CancelFunc
}

// NewScheduler returns a Scheduler backed by the given Redis client.
func NewScheduler(r redis.UniversalClient, cfg SchedulerConfig) *Scheduler {
	if cfg.Location == nil {
		cfg.Location = time.Local
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.LeaderTTL <= 0 {
		cfg.LeaderTTL = 5 * time.Second
	}
	return &Scheduler{
		rdb:      rdb.NewRDB(r),
		cfg:      cfg,
		instance: newInstanceID(),
		logger:   cfg.Logger,
	}
}

// register adds an entry after resolving options.
func (s *Scheduler) register(spec string, sched cronSchedule, kind string, payload []byte, opts []Option) {
	o := enqueueOptions{queue: DefaultQueue, maxRetry: DefaultMaxRetry}
	for _, opt := range opts {
		opt.apply(&o)
	}
	s.entries = append(s.entries, &scheduleEntry{
		id: kind + ":" + spec, kind: kind, payload: payload, queue: o.queue,
		maxRetry: o.maxRetry, noArch: o.noArchive, misfire: o.misfire, schedule: sched,
	})
}

// RegisterInterval registers args to be enqueued every interval (>= 1s).
func RegisterInterval[T TaskArgs](s *Scheduler, interval time.Duration, args T, opts ...Option) error {
	if interval < time.Second {
		return errors.New("chronos: interval must be >= 1s (sub-second schedules cannot survive leader failover)")
	}
	payload, err := encodeArgs(args)
	if err != nil {
		return err
	}
	var zero T
	s.register("@every "+interval.String(), cron.Every(interval), zero.Kind(), payload, opts)
	return nil
}

// RegisterCron registers args to be enqueued on a standard 5-field cron spec.
func RegisterCron[T TaskArgs](s *Scheduler, spec string, args T, opts ...Option) error {
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return fmt.Errorf("chronos: invalid cron spec %q: %w", spec, err)
	}
	payload, err := encodeArgs(args)
	if err != nil {
		return err
	}
	var zero T
	s.register(spec, sched, zero.Kind(), payload, opts)
	return nil
}

// Start launches leader election and the tick loop. Safe to call on every
// instance; only the leader enqueues.
func (s *Scheduler) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go s.run(runCtx)
	return nil
}

// run drives leadership renewal and, while leader, the tick loop.
func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()

	renew := time.NewTicker(s.cfg.LeaderTTL / 2)
	defer renew.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	sub := s.rdb.SubscribeResign(ctx)
	defer sub.Close()
	resign := sub.Channel()

	s.tryElect(ctx) // attempt leadership immediately

	for {
		select {
		case <-ctx.Done():
			return
		case <-renew.C:
			s.tryElect(ctx)
		case <-resign:
			// A leader resigned; try to take over right away.
			s.tryElect(ctx)
		case <-tick.C:
			if s.isLeader.Load() {
				s.fireDue(ctx)
			}
		}
	}
}

// tryElect acquires or renews leadership and, on a transition to leader,
// initializes each entry's next-trigger baseline from persisted lastFired.
func (s *Scheduler) tryElect(ctx context.Context) {
	ok, err := s.rdb.AcquireOrRenewLeadership(ctx, s.instance, s.cfg.LeaderTTL)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Error("chronos: leadership renew failed", "error", err)
		}
		return
	}
	if ok && !s.isLeader.Swap(true) {
		s.logger.Info("chronos: became scheduler leader", "instance", s.instance)
	} else if !ok {
		s.isLeader.Store(false)
	}
}

// fireDue enqueues all due triggers for every entry, applying the misfire policy.
func (s *Scheduler) fireDue(ctx context.Context) {
	now := time.Now().In(s.cfg.Location)
	for _, e := range s.entries {
		last := s.lastFiredOrInit(ctx, e, now)
		fires, newLast := computeFires(e.schedule, last, now, e.misfire)
		for _, trigger := range fires {
			if err := s.enqueueTrigger(ctx, e, trigger); err != nil {
				if !errors.Is(err, rdb.ErrDuplicateTask) && ctx.Err() == nil {
					s.logger.Error("chronos: schedule enqueue failed", "schedule", e.id, "error", err)
				}
			}
		}
		if !newLast.Equal(last) {
			if err := s.rdb.SetLastFired(ctx, e.id, newLast); err != nil && ctx.Err() == nil {
				s.logger.Error("chronos: set lastFired failed", "schedule", e.id, "error", err)
			}
		}
	}
}

// lastFiredOrInit returns the persisted lastFired for an entry. The first time a
// schedule is ever seen it baselines at now (so there is no immediate catch-up
// fire) AND persists that baseline — otherwise every tick would re-initialize to
// now, making the next trigger perpetually in the future so the job never fires.
func (s *Scheduler) lastFiredOrInit(ctx context.Context, e *scheduleEntry, now time.Time) time.Time {
	last, ok, err := s.rdb.GetLastFired(ctx, e.id)
	if err == nil && ok {
		return last
	}
	if err := s.rdb.SetLastFired(ctx, e.id, now); err != nil && ctx.Err() == nil {
		s.logger.Error("chronos: init lastFired failed", "schedule", e.id, "error", err)
	}
	return now
}

// enqueueTrigger enqueues one trigger with a deterministic dedup key so the same
// (schedule, trigger-time) is enqueued at most once cluster-wide.
func (s *Scheduler) enqueueTrigger(ctx context.Context, e *scheduleEntry, trigger time.Time) error {
	triggerID := fmt.Sprintf("%s:%d", e.id, trigger.Unix())
	msg := &base.TaskMessage{
		ID: triggerID, Kind: e.kind, Payload: e.payload, Queue: e.queue,
		MaxRetry: e.maxRetry, NoArchive: e.noArch,
	}
	dedupKey := base.PeriodicDedupKey(e.queue, triggerID)
	// Dedup key lives well beyond a leader-handover window but not forever.
	return s.rdb.EnqueuePeriodic(ctx, msg, dedupKey, 10*s.cfg.LeaderTTL)
}

// Shutdown stops the scheduler; if this instance is the leader it resigns so a
// follower can take over immediately.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.isLeader.Load() {
		_ = s.rdb.ResignLeadership(context.WithoutCancel(ctx), s.instance)
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

- [ ] **Step 4: newInstanceID 헬퍼 확인**

`scheduler.go`가 `newInstanceID()`를 쓴다. 이미 `server.go`가 `uuid.NewString()`으로 consumer ID를 만든다. 루트 패키지에 헬퍼가 없으면 `scheduler.go`에 추가:
```go
func newInstanceID() string { return uuid.NewString() }
```
그리고 import에 `"github.com/google/uuid"` 추가. (이미 다른 파일에서 uuid를 쓰지만 import는 파일 단위다.)

- [ ] **Step 5: 통과 확인**

Run: `go test . -run 'TestRegisterInterval_RejectsSubSecond|TestRegisterCron_RejectsBadSpec|TestScheduler_SingleExecutionAcrossInstances' -race -v`
Expected: PASS. 특히 마지막은 스케줄러 2개를 띄워도 1s 잡이 중복 실행되지 않음을 검증(~3회).

- [ ] **Step 6: 회귀 + 커밋**

Run: `go test ./... -race -p 1`
Expected: 전 패키지 PASS.

```bash
git add scheduler.go scheduler_test.go go.mod go.sum
git commit -m "feat: 분산 Scheduler (리더 선출 + tick 루프 + 결정적 dedup enqueue)"
```

---

## Task 5: 통합(페일오버) + 투어/문서 + 검증

**Files:**
- Create: `scheduler_integration_test.go`
- Modify: `examples/tour/main.go`, `docs/OBSERVING.md`

- [ ] **Step 1: 페일오버 통합 테스트 작성**

Create `scheduler_integration_test.go`:
```go
package chronos

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

// When the current leader shuts down (resigns), a second scheduler must take
// over and keep firing — with no duplication during the handover.
func TestIntegration_SchedulerFailover(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	c := NewClient(client)
	defer c.Close()
	var runs atomic.Int32
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[tickArgs]) error {
		runs.Add(1)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 4})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	mk := func() *Scheduler {
		s := NewScheduler(client, SchedulerConfig{LeaderTTL: 500 * time.Millisecond})
		if err := RegisterInterval(s, 1*time.Second, tickArgs{N: 1}); err != nil {
			t.Fatalf("register: %v", err)
		}
		if err := s.Start(ctx); err != nil {
			t.Fatalf("start: %v", err)
		}
		return s
	}
	s1 := mk()
	s2 := mk()
	defer s2.Shutdown(context.Background())

	time.Sleep(1500 * time.Millisecond)
	before := runs.Load()

	// Leader resigns (graceful). s2 should take over via the resign notification.
	s1.Shutdown(context.Background())

	time.Sleep(2500 * time.Millisecond)
	after := runs.Load()

	if after <= before {
		t.Errorf("scheduling stalled after failover: before=%d after=%d", before, after)
	}
}
```

- [ ] **Step 2: 통합 테스트 통과 확인**

Run: `go test . -run TestIntegration_SchedulerFailover -race -count=2 -v`
Expected: PASS (페일오버 후 스케줄링 지속). 플레이키하지 않은지 -count=2로 확인.

- [ ] **Step 3: 투어에 스케줄러 섹션 추가**

`examples/tour/main.go`의 Inspector 섹션(6번) 다음, "투어 완료" 출력 전에 추가:
```go
	section("7) 분산 스케줄러 (M4): 리더만 enqueue — 여러 인스턴스에서 안전")
	sched := chronos.NewScheduler(rdb, chronos.SchedulerConfig{LeaderTTL: time.Second})
	if err := chronos.RegisterInterval(sched, 1*time.Second, GreetArgs{Name: "매초"}); err != nil {
		fmt.Printf("register 실패: %v\n", err)
	}
	if err := sched.Start(ctx); err != nil {
		fmt.Printf("scheduler start 실패: %v\n", err)
	}
	fmt.Println("   1초 간격 잡 등록 — 리더로 선출되어 몇 번 실행되는 것을 관찰:")
	time.Sleep(3200 * time.Millisecond)
	shutSchedCtx, cancelSched := context.WithTimeout(context.Background(), 3*time.Second)
	_ = sched.Shutdown(shutSchedCtx)
	cancelSched()
```

- [ ] **Step 4: 투어 실행 확인**

Run: `go run ./examples/tour`
Expected: 7개 섹션 정상 출력, 7번에서 "매초" greet가 여러 번(약 3회) 실행되는 로그.

- [ ] **Step 5: OBSERVING.md에 스케줄러/리더 관찰 추가**

`docs/OBSERVING.md`의 "2b. CLI로 조회·관리" 다음에 추가:
```markdown
## 2c. 스케줄러 리더 관찰

분산 스케줄러는 리더로 선출된 인스턴스만 잡을 enqueue한다. 현재 리더와 스케줄 상태:

​```bash
redis-cli -n 15 GET chronos:leader              # 현재 리더 인스턴스 ID
redis-cli -n 15 KEYS 'chronos:sched:*:last'     # 스케줄별 마지막 실행 시각
redis-cli -n 15 KEYS 'chronos:{default}:pdedup:*'  # 트리거별 결정적 dedup 키
​```

리더가 graceful shutdown하면 `chronos:leader`가 사라지고 `chronos:leader:resign`
채널로 통지되어, 다른 인스턴스가 즉시 재선출된다.
```
(코드블록 백틱은 일반 백틱 3개로 작성)

- [ ] **Step 6: 전체 검증 + 커밋**

Run: `make check`
Expected: gofmt/vet 클린, `go test ./... -race -p 1` 전 패키지 PASS.

```bash
git add scheduler_integration_test.go examples/tour/main.go docs/OBSERVING.md
git commit -m "test/docs: 스케줄러 페일오버 통합 + 투어/관찰 가이드 반영"
```

---

## M4 완료 기준

- [ ] `make check` 통과
- [ ] 리더 선출: 여러 인스턴스 중 하나만 리더, 만료/사임 시 페일오버
- [ ] 스케줄러 2개를 띄워도 1s interval 잡이 중복 실행되지 않음(결정적 dedup)
- [ ] cron/interval 등록, interval < 1s는 에러
- [ ] misfire: Skip(기본, 놓친 건 버림)/FireOnce(1회 만회), computeFires 단위 테스트 통과
- [ ] graceful shutdown 시 리더 사임 → 팔로워 즉시 재선출
- [ ] 투어에 스케줄러 섹션, OBSERVING.md에 리더 관찰법

**다음 단계:** M4까지면 operator-review의 `JobScheduler`(RegisterScheduledJob/RegisterCronJob/Enqueue/RegisterTaskHandler)를 chronos-go로 재구현할 수 있다 — v1 성공 기준 검증. 이후 M5 나머지(Prometheus 메트릭), heartbeat 마일스톤(lease/unique TTL 연장).
