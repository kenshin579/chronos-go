# 스케줄러 알려진 이슈 (개선 백로그)

> 작성일: 2026-11-05
> 기준 커밋: `27726ef` (main)
> 배경: operator-review 의 주기 잡 가드 변경(주기 잡을 클러스터에서 한 번에 하나만, 주기마다 한 번만 실행)을
> 리뷰하면서, 같은 문제를 chronos-go 스케줄러 코드에 대어 보았다.
> 조사 방법: 코드 읽기만 했다. 테스트·재현은 하지 않았다. 각 이슈의 "재현 방법"은 개선에 착수할 때
> 먼저 작성할 테스트다.

---

## 1. 요약

| # | 이슈 | 영향 | 우선순위 | 근거 |
|---|---|---|---|---|
| S1 | 앞 회차가 도는 동안 다음 회차가 겹쳐 실행된다 | operator-review 를 chronos-go 로 옮기면 "한 번에 하나" 보장이 사라진다 | 높음 | 코드로 확인 |
| S2 | 리더 1초 폴링의 위상 때문에 1초 잡이 두 회차 연속 빠질 수 있다 | 1초 잡 누락 | 중간 | 계산상 가능, 재현 필요 |
| S3 | 스케줄 등록에서 `WithUnique`·`WithTaskID`·`WithProcessIn/At` 이 조용히 무시된다 | 겹침 방지를 기대한 설정이 효과 없음 | 중간 | 코드로 확인 |
| S4 | 스케줄 잡의 기본 재시도가 25회라 재시도가 이후 회차와 겹친다 | S1 을 키운다 | 낮음 | 코드로 확인 |
| S5 | 테스트 이름이 단언과 반대로 읽힌다 | 가독성 | 낮음 | 코드로 확인 |
| S6 | 초 단위가 아닌 interval 이 조용히 잘린다 | 설정 실수를 숨긴다 | 낮음 | godoc 에 적혀 있음 |

이미 README "Known limitations" 에 적힌 한계(리더가 dedup TTL 보다 오래 멈췄다 깨어나면 같은 트리거를 한 번 더 낼 수
있음)는 [6. 참고](#6-참고-이미-문서화된-한계)에 따로 둔다.

### 잘 되어 있는 것 (회귀 방지용 메모)

주기마다 인스턴스 수만큼 실행되는 문제는 구조적으로 생기지 않는다. 개선 작업 중에 이 성질을 깨지 않는다.

- 리더 선출로 한 인스턴스만 틱을 낸다 (`scheduler.go:229`, `internal/rdb/leader.go`).
- 다음 트리거 시각을 Redis 에 저장한 `lastFired` 에서 계산하므로 인스턴스마다 다른 위상이 생기지 않는다
  (`scheduler.go:315`, `schedule.go:33`).
- 트리거 ID `<scheduleID>:<trigger unix>` 를 발행하는 쪽이 확정하고 `SET NX` dedup 키로 막으므로, 리더 교대 중
  옛 리더와 새 리더가 같은 트리거를 내도 한 번만 들어간다 (`scheduler.go:328`, `internal/rdb/periodic.go:25`).
- 검증: `TestScheduler_SingleExecutionAcrossInstances`.

---

## 2. S1. 앞 회차가 도는 동안 다음 회차가 겹쳐 실행된다

### 현상

interval 보다 오래 걸리는 주기 잡은 다음 회차와 동시에 실행된다. 1분 잡이 매번 70초 걸리면 매 회차가 앞 회차와
10초씩 겹친다. 외부 API 응답이 늦어지는 잡에서 흔하다. operator-review 에서는 같은 원인으로 아래 일이 있었다.

- 읽은 값 + 1 로 쓰는 시도 횟수를 두 실행이 서로 덮어썼다.
- 겹친 실행이 외부 벤더 API 를 두 배로 불러 `429` 로 거절당했다.

### 원인

- `enqueueTrigger` 는 트리거마다 다른 ID 를 만든다 (`scheduler.go:328-337`). dedup 키
  (`internal/rdb/periodic.go:25-34`)는 **같은 회차**의 중복만 막고, 다른 회차는 서로 다른 태스크다.
- `fireDue` 는 앞 회차가 pending 이나 active 인지 확인하지 않는다 (`scheduler.go:279-309`).
- heartbeat 는 실행 중인 태스크의 lease 와 unique 락을 연장해 회수를 막을 뿐, 다음 회차를 막지 않는다
  (`server.go:765-785`).
- 호환 어댑터 `examples/jobscheduler/jobscheduler.go:115-130` 의 `RegisterScheduledJob` 도 `RegisterInterval` 로
  바로 이어져 같은 동작을 한다.

### 영향

`docs/research/2026-07-05-go-scheduler-library-comparison.md` 의 성공 기준은 "operator-review 의 `JobScheduler` 를
asynq 에서 chronos-go 구현체로 교체해도 동일 동작"이다. operator-review 는 주기 잡 가드(실행 락 + 구간 선점)를
넣은 뒤로 "한 번에 하나"를 보장한다. 지금 어댑터로 교체하면 이 보장이 사라진다.

### 재현 방법

```go
// 1초 interval, handler 는 2.5초 걸린다. 동시에 실행 중인 handler 수의 최댓값을 센다.
var cur, peak atomic.Int32
AddHandler(mux, func(ctx context.Context, t *Task[tickArgs]) error {
	n := cur.Add(1)
	defer cur.Add(-1)
	for {
		p := peak.Load()
		if n <= p || peak.CompareAndSwap(p, n) {
			break
		}
	}
	time.Sleep(2500 * time.Millisecond)
	return nil
})
// ServerConfig{Concurrency: 4}, RegisterInterval(s, time.Second, tickArgs{}), 6초 관찰
// 현재: peak >= 2. 개선 후(옵션 사용): peak == 1
```

### 개선안: 스케줄 단위 "실행 중이면 건너뛰기" 옵션

옵션 이름 예: `WithSkipIfRunning()` (또는 `WithNoOverlap()`).

1. `enqueueTrigger` 에서 periodic dedup 키와 함께 **스케줄 단위** unique 키
   `UniqueKey(queue, "sched:"+scheduleID)` 를 같은 Lua 스크립트에서 `SET NX` 한다(값은 triggerID).
   두 키 모두 `{queue}` 해시태그 아래라 한 스크립트에 넣어도 Redis Cluster 에서 안전하다
   (`internal/base/keys.go:111`, `:125`).
2. `msg.UniqueKey` 에 이 키를 넣으면 기존 경로를 그대로 쓴다.
   - 실행 중에는 heartbeat 가 TTL 을 연장한다 (`server.go:768-784`).
   - 성공(`Done`)과 dead-letter(`Archive`)에서 해제된다 (`internal/rdb/rdb.go:285-311`, `internal/rdb/retry.go:35-41`).
   - 재시도 대기(`Retry`) 중에는 유지된다. 재시도가 끝날 때까지 다음 회차를 막는다.
3. 잠금에 실패하면 새 에러(예: `ErrScheduleBusy`)를 돌려준다. `fireDue` 는 이를 `ErrDuplicateTask` 와 같이
   "처리됨"으로 보고 `lastFired` 를 넘긴다 (`scheduler.go:292`). 그러면 "실행 중이면 이번 회차는 건너뛴다"가 된다.
   건너뛴 회차는 로그·메트릭에서 중복 트리거와 구분한다.
4. 호환 어댑터의 `RegisterScheduledJob` 은 이 옵션을 기본으로 켠다.

### 확인할 점

- **TTL:** pending 상태에서는 heartbeat 가 없어 TTL 로만 유지된다. TTL 은 interval 과 예상 대기 시간보다 길어야 한다.
  README 의 unique 락 한계와 같은 제약이다.
- **Lua 안의 검사 순서:** 스케줄 단위 키를 먼저 검사하고, 실패할 때 periodic dedup 키를 세울지 정한다.
  리더 교대 중 같은 회차를 다른 리더가 다시 내는 경우를 함께 따진다.
- **크래시 회수:** 워커가 죽어 recoverer 가 `XAUTOCLAIM` 으로 다시 큐에 넣을 때, 스케줄 단위 락이 유지되는지
  `internal/rdb/recover.go` 에서 확인한다.
- **semver:** 새 옵션 추가라 minor 변경이다.

### 수용 기준

- 재현 테스트에서 옵션을 쓰면 `peak == 1`, 안 쓰면 현재 동작 그대로.
- 앞 회차가 끝난 뒤의 회차는 정상 실행된다.
- 건너뛴 회차 수가 메트릭에 남는다.
- `make test-cluster` 통과.

---

## 3. S2. 리더 1초 폴링의 위상 때문에 1초 잡이 두 회차 연속 빠질 수 있다

### 현상

1초 interval 잡에서, 리더의 1초 틱이 정각 직전(예: 매초 x.999)에 놓여 있으면 틱 전달이 몇 ms 만 늦어도 두 회차가
연속으로 enqueue 되지 않는다.

### 원인

- 리더 루프는 시작 시각 기준 `time.NewTicker(time.Second)` 로 돈다. 정각 정렬이 없다 (`scheduler.go:222`).
- `cron.Every(1s).Next` 는 정각 초를 돌려주므로 트리거는 정각에 놓인다 (robfig/cron v3.0.1 `constantdelay.go`).
- `computeFires` 는 트리거가 둘 이상 지났으면 갭으로 본다. 기본값 `MisfireSkip` 이면 **하나도 내지 않고**
  `lastFired` 를 최신 트리거로 넘긴다 (`schedule.go:53-61`).

| 리더 틱 | now | lastFired | 계산 | 결과 |
|---|---|---|---|---|
| k | S+0.999 | S−1 | S 가 지났고 S+1 은 아직 | S 를 냄 |
| k+1 (2ms 늦게 전달) | S+2.001 | S | S+1, S+2 가 모두 지남 → 갭 | **아무것도 안 냄**, lastFired = S+2 |
| k+2 | S+2.999 | S+2 | S+3 은 아직 | 없음 |
| k+3 | S+3.999 | S+2 | S+3 이 지남 | S+3 을 냄 |

S+1 과 S+2 가 빠진다. 연속한 두 `fireDue` 사이에 정각 경계가 두 번 들어가면 생긴다. 리더 틱의 위상을 p, 틱 전달
지연을 δ 라 하면 `δ(k+1) − δ(k) ≥ 1 − p` 일 때다. 위상이 정각 직전이면 몇 ms 로 충분하다.

지연을 만드는 것:

- 같은 select 루프가 2.5초마다 `tryElect`·`TouchSchedules`·`pruneRegistry` 의 Redis 호출을 처리한다
  (`scheduler.go:235-245`). 그동안 도착한 틱은 기다린다.
- `fireDue` 는 스케줄마다 Redis 를 차례로 부른다. 스케줄이 많아 1초를 넘기면 `time.Ticker` 가 밀린 틱을 버리고,
  다음 틱에서는 모든 1초 잡이 갭으로 판정된다.
- GC 정지.

1초보다 긴 interval 도 원리는 같지만, 갭이 되려면 지연이 interval 만큼 커야 하므로 실질적으로는 1초 잡의 문제다.

operator-review 의 가드에도 같은 종류의 문제가 있었다. 거기서는 소비 쪽의 처리 시각이 정각 트리거와 어긋났고,
경계를 정각보다 200ms 앞에 두는 것으로 다뤘다. chronos-go 에서는 생산 쪽의 리더 폴링 시각이 어긋난다.

### 재현 방법

1. **계산 확인 (단위):**
   `computeFires(cron.Every(time.Second), S, S.Add(2001*time.Millisecond), MisfireSkip)` 이 아무것도 내지 않는다.
   갭에서 아무것도 내지 않는 동작 자체는 `TestComputeFires_Gap_Skip_FiresLatestOnly` 가 이미 고정한다.
2. **시퀀스 재현 (통합):** `Scheduler` 에 시계 주입 지점(`now func() time.Time`)을 만든다. now 를
   `[S+0.999, S+2.001, S+2.999, S+3.999]` 로 주며 `fireDue` 를 부르고, enqueue 된 트리거가 S 와 S+3 뿐임을 확인한다.
3. **실측:** 리더 시작 시각을 x.995 로 맞추고 `fireDue` 앞에 인위적인 지연을 섞어, 60초 동안 1초 잡의 실행 횟수를 센다.

### 개선안

- **A. 리더 루프를 정각 + 고정 오프셋에 맞춘다 (근본 대책).** 다음 "정각 + 100ms" 까지 기다린 뒤 ticker 를 시작하거나,
  매 반복마다 다음 "정각 + 오프셋" 까지 `time.Timer` 를 건다. 위상이 경계에서 떨어져 약 900ms 지연까지 버틴다.
- **B. 작은 지연은 misfire 로 보지 않는다.** Quartz 의 `misfireThreshold` 처럼, 마지막으로 놓친 트리거가 기준
  시간보다 덜 늦었으면 정상 발사로 처리한다. "한 번에 여러 개를 내지 않는다"(`MisfireRunAll` 미지원) 원칙과 맞추려면
  최신 트리거 하나를 낸다. GC 처럼 큰 지연에 대한 안전망이다.
- **C. 임시 방편:** 1초 잡에는 `MisfireFireOnce` 를 권장한다고 문서화한다. 갭에서 최신 하나는 내므로 손실이 두 회차에서
  한 회차로 준다.

권장은 A + B 다.

### 수용 기준

- 시퀀스 재현 테스트에서 S, S+2(또는 S+1, S+2), S+3 이 발사된다. 최소한 연속 두 회차가 빠지지 않는다.
- 리더 시작 위상과 무관하게, 부하가 없을 때 1초 잡의 60초 실행 횟수가 60 ± 1 이다.

---

## 4. S3. 스케줄 등록에서 일부 옵션이 조용히 무시된다

### 현상

`RegisterInterval(s, time.Minute, args, chronos.WithUnique(time.Minute))` 처럼 겹침을 막으려고 `WithUnique` 를 줘도
효과가 없고 에러도 나지 않는다. `WithTaskID`, `WithProcessIn`, `WithProcessAt` 도 같다.

### 원인

`register` 가 옵션을 `enqueueOptions` 로 풀지만, `scheduleEntry` 에는 `queue`, `maxRetry`, `noArchive`, `misfire`,
`retention` 만 옮긴다 (`scheduler.go:92-107`). `uniqueTTL`, `taskID`, `processAt` 은 버려진다.

### 개선안

- 체인에는 이미 선례가 있다. `WithTaskID` 와 `WithUnique` 는 체인 안에서 거절된다 (`chain.go:43`, README "Chains").
  스케줄도 의미 없는 옵션은 `RegisterInterval`/`RegisterCron` 이 에러를 돌려주게 한다.
- `WithUnique` 는 S1 의 스케줄 단위 잠금으로 의미를 연결하거나 거절한다. 둘 중 하나로 정해 문서에 적는다.
- **semver:** 지금 받아들이던 옵션을 거절하면 동작 변경이다. 다음 minor 에서는 경고 로그와 `// Deprecated` 수준의
  문서 안내를 하고, 거절은 major 에서 하는 방안을 검토한다.

---

## 5. 낮은 우선순위

### S4. 스케줄 잡의 기본 재시도가 25회다

- 원인: `register` 의 기본값이 `maxRetry: DefaultMaxRetry` 다 (`scheduler.go:93`).
- 영향: 실패한 회차가 backoff 뒤에 다시 돌면서 이후 회차와 겹친다. 주기 잡은 곧 다음 회차가 돌기 때문에 재시도의
  가치가 낮다. operator-review 의 주기 잡은 모두 `MaxRetry(0)` 이고 호환 어댑터도 `WithMaxRetry(0)` 을 준다.
- 개선안: README "Scheduling" 에 주기 잡은 `WithMaxRetry(0)` 을 권장한다고 적는다. 스케줄 기본값을 0 으로 바꾸는 것은
  동작 변경이라 semver 를 따져 정한다. S1 옵션을 쓰면 재시도 중에도 잠금이 유지되므로 겹침은 사라진다(대신 재시도가
  다음 회차를 막는다).

### S5. 테스트 이름이 단언과 반대로 읽힌다

- `schedule_test.go:38` 의 `TestComputeFires_Gap_Skip_FiresLatestOnly` 는 "최신 하나만 낸다"로 읽히지만, 실제로는
  아무것도 내지 않는 것을 단언한다.
- 개선안: `TestComputeFires_Gap_Skip_FiresNone` 으로 바꾼다.

### S6. 초 단위가 아닌 interval 이 조용히 잘린다

- `RegisterInterval` 의 godoc 대로 1500ms 는 1초로 돈다 (`scheduler.go:109-124`). 트리거 ID 가 unix 초라 잘리는 것
  자체는 설계상 자연스럽다.
- operator-review 는 같은 경우를 등록 시점에 거절하기로 했다(1100ms 로 등록하던 잡이 실제로는 1초로 돌고 있었음).
- 개선안: 경고 로그를 남긴다. 거절은 동작 변경이라 S3 과 같은 semver 판단을 따른다.

---

## 6. 참고: 이미 문서화된 한계

README "Known limitations" 에 있는 것. 이번 조사에서 새로 찾은 것은 아니지만 스케줄러 개선 때 함께 본다.

- **Scheduler fencing:** dedup TTL(`10 × LeaderTTL`, 기본 50초)에 기댄다. 그보다 오래 멈췄다 깨어난 리더는 같은
  트리거를 두 번 낼 수 있다. fencing token 도입이 후보다.
- **시계와 Location:** 모든 인스턴스가 같은 시계와 `Location` 을 써야 한다. 트리거 ID 가 계산된 트리거 시각에서 나오기
  때문이다.

---

## 7. operator-review 가드와의 비교

| 문제 | operator-review (asynq + 가드) | chronos-go |
|---|---|---|
| 주기마다 인스턴스 수만큼 실행 | 소비 쪽 구간 선점(`SET NX`, 구간 = 처리 시각 + 200ms 를 interval 로 내림) | 생산 쪽. 리더 하나 + 트리거 시각 ID. 문제 없음 |
| 실행이 interval 보다 길면 겹침 | 실행 락(TTL 3초, 1초마다 연장), 못 잡으면 건너뜀 | **막는 장치 없음** (S1) |
| 정각 경계 | 소비 쪽 처리 시각의 어긋남을 200ms 버퍼로 흡수 | 생산 쪽 리더 폴링 위상 (S2) |
| 실패한 회차 재시도 | 0회 | 기본 25회 (S4) |
| 초 단위가 아닌 interval | 거절 | 1초로 잘림 (S6) |
