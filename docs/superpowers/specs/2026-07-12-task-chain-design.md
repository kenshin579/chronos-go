# chronos-go Task Chain (연쇄 실행) 설계

- 상태: 승인됨 (2026-07-12)
- 관련: `chronos.go`(Enqueue/옵션), `internal/base/task.go`(TaskMessage),
  `server.go`(process 성공 경로), `internal/rdb/rdb.go`(enqueueCmd/Done),
  `internal/rdb/inspect.go`(RunTask — 재개의 기반)
- 범위: **Chain만**. Group(병렬+완료 콜백)은 명시적 범위 제외.

## 배경 / 목적

태스크 간 의존 흐름("A 성공하면 B, B 성공하면 C")을 라이브러리가 지원한다.
지금은 핸들러 안에서 다음 태스크를 직접 enqueue해야 해서 흐름이 코드에 흩어지고,
at-least-once 재전달 시 후속 중복 enqueue 방지를 사용자가 재발명해야 한다.

## 확정 결정 (브레인스토밍)

1. **API = 빌더**: `NewChain().Then(args, opts...).Then(...).Enqueue(ctx, client)`.
   `Then(args TaskArgs, opts ...Option)` — 링크별 큐/재시도/retention 지정.
2. **실패 의미론 = 중단 + 재실행 시 재개**: 링크가 dead-letter되면 체인 중단.
   운영자가 `RunTask`로 재실행해 성공하면 그 지점부터 재개(꼬리 내장의 자연 귀결).
3. 저장 = **후속 링크를 TaskMessage에 내장**(별도 체인 상태 저장소 없음).
4. 중복 방지 = **결정적 후속 TaskID(`<chainID>:<i>`) + 존재 시 no-op enqueue**.

## 설계

### A. 공개 API (chronos.go 또는 신규 chain.go)

```go
// Chain builds a sequence of tasks where each link is enqueued only after the
// previous one succeeds.
type Chain struct{ ... }

func NewChain() *Chain

// Then appends a link. args must implement TaskArgs; opts are the same
// per-task options Enqueue accepts (queue, retry, retention, unique, ...).
func (ch *Chain) Then(args TaskArgs, opts ...Option) *Chain

// Enqueue makes the FIRST link available for processing and returns its
// TaskInfo. Later links run only as their predecessors succeed.
func (ch *Chain) Enqueue(ctx context.Context, c *Client) (*TaskInfo, error)
```

- 빈 체인 `Enqueue` → 에러(`chronos: empty chain`).
- 링크 1개 = 일반 Enqueue와 동일 동작.
- `Then`에서 인코딩 실패(직렬화 불가 args)는 `Enqueue` 시점에 에러로 반환
  (빌더는 에러를 축적했다가 Enqueue에서 일괄 반환 — 체이닝 문법 유지).
- 체인 전체 옵션 없음(YAGNI). `WithProcessIn/At`은 **1번째 링크에만 유효**
  (후속 링크는 "앞 링크 성공 즉시"가 의미론 — 후속 링크에 지정 시 그 링크가
  승격될 때 지연 적용, 이는 자연스러운 확장이므로 허용하고 문서화).

### B. 데이터 모델 (internal/base)

```go
// ChainLink is one pending successor task, carried inside its predecessor's
// message. It is a serializable snapshot of the enqueue parameters.
type ChainLink struct {
	Kind      string `json:"kind"`
	Payload   []byte `json:"payload"`
	Queue     string `json:"queue"`
	MaxRetry  int    `json:"max_retry"`
	NoArchive bool   `json:"no_archive,omitempty"`
	Retention int64  `json:"retention,omitempty"`  // seconds
	UniqueTTL int64  `json:"unique_ttl,omitempty"` // seconds
	Delay     int64  `json:"delay,omitempty"`      // seconds (WithProcessIn)
}
```

- `TaskMessage`에 추가: `Chain []ChainLink json:"chain,omitempty"` +
  `ChainID string json:"chain_id,omitempty"` + `ChainIndex int json:"chain_index,omitempty"`.
- 링크 i의 msg: `ID = "<chainID>:<i>"`, `Chain` = 남은 꼬리(i+1..n).
- 1단계도 체인 소속이면 `ID = "<chainID>:0"` (WithTaskID와 병용 불가 — 체인이
  ID를 소유. Then의 opts에 WithTaskID가 있으면 Enqueue에서 에러).
- 메시지 크기는 체인 길이에 비례 — 합리적 길이에서 무시 가능, README 명시.

### C. 실행 흐름 (server.go process 성공 경로)

성공 시 순서(순서가 정확성의 핵심):

1. `msg.Chain`이 비어 있지 않으면 **후속 링크 enqueue** (아래 D의 no-op 가드 Lua).
   enqueue가 에러를 반환하면 **로그만 남기고 Done을 건너뛴 채 process를 종료**한다
   — 현재 태스크가 PEL에 남아 recoverer가 재전달하고, 핸들러 재실행 후 후속
   enqueue를 다시 시도하게 된다. (Redis 장애 시 체인 유실 대신 재시도를 선택.
   핸들러가 한 번 더 실행되는 비용은 기존 at-least-once 계약 안이다.)
2. `rdb.Done` (기존 로직 그대로 — retention 분기 포함).

- ①과 ② 사이 크래시: 재전달 → 핸들러 재실행(기존 at-least-once) → ①은
  no-op(중복 방지) → ② 완료. 체인 정합 유지.
- 실패 경로(retry/dead-letter): 변경 없음 — Chain 꼬리는 msg에 실려 보존.
  dead-letter된 링크를 `RunTask`로 재실행해 성공하면 ①이 실행돼 체인 재개.
- discard 링크 실패 시 꼬리도 함께 소멸(중단) — 문서 명시.

### D. 중복 방지 (internal/rdb)

신규 `chainEnqueueCmd` Lua — enqueueCmd의 create-if-absent 변형:

```lua
-- KEYS[1] task hash, KEYS[2] stream (+ scheduled zset variant는 구현 시 판단)
-- 이미 존재하는 태스크(재전달로 인한 두 번째 시도, 또는 완료 후 보관 중)면 no-op.
if redis.call("EXISTS", KEYS[1]) == 1 then
  return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[2])
redis.call("XADD", KEYS[2], "*", "task_id", ARGV[3])
return 1
```

- v0.5.0의 "재enqueue 시 잔존 completed/archived ZREM 정리"와 의도적으로 다름:
  체인 후속 enqueue는 **순수 create-if-absent** — 후속이 이미 완료·보관 중이면
  재실행하지 않는 것이 올바른 동작(중복 방지가 목적).
- `Delay > 0` 링크는 scheduled ZSET 버전으로(동일 EXISTS 가드).
- `UniqueTTL > 0` 링크는 기존 unique 경로에 EXISTS 가드를 결합(구현 시
  기존 uniqueEnqueueCmd 변형 또는 호출 전 EXISTS 체크 — 원자성 유지 필수).
- rdb 공개 함수: `EnqueueChainLink(ctx, msg *base.TaskMessage, link 파라미터...)
  (enqueued bool, err error)` 형태(정확한 시그니처는 구현 시).

### E. 관찰 / 노출

- `TaskInfo.ChainPending int` — 남은 링크 수(0 = 체인 없음 또는 마지막 링크).
  `taskInfoFromMsg`에서 `len(m.Chain)` 매핑. dead-letter 조회 시 "이 뒤에 N개
  링크가 걸려 있다"가 보임.
- tour **섹션 12**: (1) 3링크 체인 성공 완주(실행 순서 출력), (2) 중간 실패
  체인이 dead-letter로 멈춘 것 확인(ChainPending 표시) → `RunTask` 재개 →
  완주까지 눈으로 확인.
- cluster 스모크 +1: `chainEnqueueCmd`(신규 Lua) — 스크립트-완전 원칙.
- README: "Chains" 섹션 — 빌더 예제, 실패=중단/재실행=재개 의미론, 핸들러
  멱등 전제(체인에서도 동일), 메시지 크기 주의.

## 테스트 (TDD)

1. **순차 실행**: 3링크(서로 다른 kind) 체인 → 순서대로 실행(실행 기록 검증),
   링크별 옵션(다른 큐) 적용 확인.
2. **중단+재개**: 2번째 링크가 항상 실패(MaxRetry 0) → 3번째 미실행 확인 →
   dead-letter를 `RunTask` → (핸들러를 성공으로 바꾼 뒤) 3번째까지 완주.
3. **중복 방지** (rdb): 후속 태스크 hash가 이미 존재할 때 `chainEnqueueCmd`
   no-op(enqueued=false), 스트림에 엔트리 추가 없음.
4. **빌더 검증**: 빈 체인 에러, WithTaskID 병용 에러, 링크 1개 정상.
5. **ChainPending**: 1단계 조회 시 2, 마지막 링크 0.
6. **cluster 스모크**: 체인 완주 1개 (14번째 시나리오).

## 검증 / 마무리

- `make check` 무회귀 + `make test-cluster`(14개) + tour 섹션 12 눈 확인.
- k:code-reviewer 리뷰 → PR(assignee kenshin579) → 머지.

## 알려진 한계 / 후속

- 링크 간 **결과 전달 없음** — 각 링크의 payload는 체인 생성 시점에 고정.
  (결과 전달은 Temporal 영역 — 필요 시 사용자가 공유 저장소로 해결.)
- Group(병렬+완료 콜백)은 범위 외 — 후속 후보.
- 체인 전체 취소 API 없음 — 현재 링크를 DeleteTask하면 꼬리도 함께 소멸
  (사실상의 취소, 문서로 안내).
