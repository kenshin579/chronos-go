# chronos-go chain×group 조합 + 결과 전달 설계

- 상태: 승인됨 (2026-07-13)
- 관련: `chain.go`(꼬리 내장·후속 enqueue→Done), `group.go`(pending SET·
  groupCompleteCmd), `handler.go`(AddHandler), `internal/base/task.go`
  (TaskMessage/ChainLink), docs/superpowers/specs/2026-07-12(chain)·(group)
- 범위: 한 스펙, 두 PR — PR 1 결과 전달, PR 2 ThenGroup. 그룹 멤버가
  체인/그룹인 재귀 중첩과 워크플로 밖 임의 태스크의 결과 조회 API는 범위 외.

## 확정 결정 (브레인스토밍)

1. **한 스펙, 두 단계 구현** (a안): 대표 usecase "팬아웃→팬인→후속"은 결과
   전달과 중첩이 결합해야 완성 — API 정합을 위해 스펙은 하나, 구현·PR은 분리.
2. **결과 반환 = `AddHandlerR` 신설** (a안): `func(ctx, *Task[T]) (R, error)`.
   기존 `AddHandler` 무변경(**breaking 없음** — v1 순서 제약 해소). 성공
   반환과 결과가 원자적(에러 시 결과 없음). SetResult식 뮤테이션(재시도
   덮어쓰기 애매함)과 AddHandler 시그니처 변경(전 핸들러에 R 강요)은 기각.
3. **결과 저장 = 릴레이 내장** (a안): 별도 결과 저장소 없음.
   - chain: 후속 링크 메시지에 `PrevResult []byte` 내장(기존 "후속 enqueue→
     Done" 순서 활용). 재시도·재전달·dead-letter에도 결과가 메시지와 함께
     이동 — RunTask 재개 의미론 무변경.
   - group: 콜백 큐 슬롯의 결과 HASH에 모았다가 콜백 생성 시 콜백 메시지에
     내장 후 HASH 삭제. asynq식 전역 결과 키(TTL·슬롯·조회 API 표면)는 기각.
4. **중첩 표현 = `Chain.ThenGroup(group)`** (a안): 워크플로 = 스테이지
   시퀀스, 스테이지 = 단일 태스크 또는 그룹(병렬+OnComplete 팬인). 내부
   메커니즘은 "그룹 콜백이 체인 꼬리를 상속" — 별도 Workflow 빌더와 완전
   재귀 중첩은 기각.
5. **콜백의 결과 수신 = Add 순서 슬라이스** (a안): 타입드 헬퍼
   `GroupResults[R]` + raw `RawGroupResults()`. 결과 없는 멤버(AddHandler
   처리)는 해당 위치 nil. ID→결과 map은 기각(콜백이 멤버 ID를 알 방법이
   나쁨).
6. **결과 크기 상한 1MiB**: 초과 시 해당 태스크를 **재시도 없이** dead-letter
   (같은 결과는 재시도해도 같은 크기). 큰 산출물은 참조(S3 경로 등)를 결과로
   넘기라고 문서 권고.

## PR 1 — 결과 전달

### 핸들러 API (`handler.go`)

```go
// AddHandlerR registers a handler whose success return value becomes the
// task's result: relayed to the next chain link (PrevResult) and collected
// for the group callback (GroupResults). Marshalled as JSON; results larger
// than MaxResultSize (1MiB) dead-letter the task without retry.
func AddHandlerR[T TaskArgs, R any](mux *Mux, fn func(ctx context.Context, task *Task[T]) (R, error))
```

- 내부: 기존 `AddHandler` 경로를 재사용하되, 성공 시 결과를 직렬화해 process
  루프에 돌려준다(핸들러 래퍼가 `*Task[T]`의 내부 result 슬롯에 기록하는
  방식 — 시그니처 전파 최소화).
- 직렬화 실패·1MiB 초과 → `ErrResultTooLarge`(센티넬) 포함 에러로
  `SkipRetry` 의미론 적용(기존 SkipRetry 경로 재사용) → dead-letter/discard.
- 결과가 없는 기존 `AddHandler` 핸들러 = 결과 nil (혼용 자유).

### chain 릴레이 (`chain.go`, `internal/base/task.go`, `internal/rdb`)

- `TaskMessage.PrevResult []byte \`json:"prev_result,omitempty"\`` 추가.
- 링크 성공 시 기존 "후속 enqueue → Done" 지점에서 후속 메시지에 이번 링크의
  결과를 담아 enqueue (`chainEnqueueCmd`/`chainScheduleCmd`는 메시지 JSON을
  통째로 받으므로 Lua 무변경 예상 — 메시지 구성 Go 코드만 변경).
- 수신 API (`chronos.go`):

```go
// ErrNoResult is returned when the previous step produced no result.
var ErrNoResult = errors.New("chronos: no result")

// PrevResult decodes the previous chain step's result.
func PrevResult[R any](t interface{ prevResult() []byte }) (R, error)
```

  실제 시그니처는 구현 시 `*Task[T]`의 비공개 접근자에 맞춰 확정하되, 사용
  형태는 `chronos.PrevResult[EncodeResult](t)` 고정. 첫 링크·결과 없음 →
  `ErrNoResult`.
- 재시도: PrevResult는 메시지 필드라 재시도·XAUTOCLAIM 재전달·dead-letter
  후 RunTask 재실행 모두에서 보존.

### group 수집 (`group.go`, `internal/rdb`)

- 결과 HASH `chronos:{cbq}:groupresult:<groupID>` (pending SET과 같은 해시
  슬롯 — 기존 GroupKey와 동일 규칙, `base.GroupResultKey(cbQueue, id)`).
- 멤버 메시지에 `GroupIndex int` 추가(Add 순서, 0-based).
- `groupCompleteCmd` Lua 확장: `SREM pending` + (결과 있으면) `HSET
  groupresult <index> <result>` + `SCARD==0`이면 HGETALL→콜백 메시지에 내장
  →콜백 create-if-absent→`DEL pending, groupresult`. 전 키가 같은 슬롯 —
  cluster-safe. TTL: pending SET과 동일하게 GroupTTL(7d)로 EXPIRE, 완료
  보고마다 갱신(기존 관례).
- 콜백 메시지: `GroupResults [][]byte \`json:"group_results,omitempty"\``
  (Add 순서, 결과 없는 멤버 nil). 콜백 직렬화 크기가 걱정되는 수준(멤버 수
  × 결과 크기)은 1MiB/멤버 상한과 문서 권고로 관리 — 별도 상한 없음.
- 수신 API:

```go
// GroupResults decodes every member result in Add order. It assumes a
// homogeneous group (every member returned R); a nil member result fails
// with ErrNoResult. Heterogeneous or partial results: use RawGroupResults.
func GroupResults[R any](t ...) ([]R, error)
// RawGroupResults returns raw member results in Add order (nil = no result).
func (t *Task[T]) RawGroupResults() [][]byte
```

  `PrevResult`와 마찬가지로 `t` 파라미터의 정확한 시그니처는 구현 시
  `*Task[T]` 접근자에 맞춰 확정하되, 사용 형태는
  `chronos.GroupResults[EncodeResult](t)` 고정.

### 표면·검증 (PR 1)

- Inspector: `TaskInfo.HasResult bool` + `ResultSize int`(결과 자체는 payload
  처럼 필요 시 raw — webui 태스크 상세에서 hex/JSON 표시 여부는 구현 시).
- 테스트(TDD): AddHandlerR 성공/에러/1MiB 초과 dead-letter(no retry),
  chain 2링크 결과 릴레이(+재시도 후에도 보존), PrevResult ErrNoResult,
  group 3멤버 결과 수집 순서(+AddHandler 혼용 nil), dead-letter 멤버 RunTask
  재개 후 콜백이 전체 결과 수신, cluster 스모크 18번째(결과 릴레이 —
  groupCompleteCmd Lua 변경 검증 필수).
- tour 섹션 15(파이프라인: OCR→번역→요약 축소판), README Workflow 섹션 갱신.

## PR 2 — ThenGroup

### 빌더 (`chain.go`)

```go
// ThenGroup appends a parallel stage: every member of g runs concurrently
// (each receiving the previous stage's result via PrevResult), and g's
// OnComplete callback fans in member results (GroupResults) before the chain
// continues with the callback's own result.
func (ch *Chain) ThenGroup(g *Group) *Chain
```

- 모델: 스테이지 시퀀스. 내부 표현 — `ChainLink`에 그룹 스테이지 변형 추가:

```go
type ChainLink struct {
	// 기존 필드 유지(단일 태스크 스테이지)...
	Group []GroupMemberLink `json:"group,omitempty"` // 병렬 스테이지면 비어있지 않음
	// Kind/Payload/Queue 등 기존 필드는 그룹 스테이지에서는 콜백(OnComplete)
	// 태스크를 서술한다 — 콜백이 곧 스테이지의 "완료 지점"이므로.
}
type GroupMemberLink struct { // ChainLink의 단일 태스크 필드 부분집합
	Kind string; Payload []byte; Queue string; MaxRetry int; Delay int64 ...
}
```

- 실행: 앞 스테이지 성공 → 다음 링크가 그룹 스테이지면, 후속 단일 태스크
  대신 **그룹을 enqueue**(기존 Group.Enqueue의 create-if-absent 경로 재사용,
  결정적 ID `<chainID>:<i>:m<j>`/콜백 `<chainID>:<i>` — 체인 재전달 시 그룹
  중복 생성 방지). 전 멤버 메시지에 앞 스테이지의 PrevResult 복제 내장.
  콜백 메시지가 체인의 남은 꼬리(`Chain`)·ChainID·ChainIndex를 상속 → 콜백
  성공 시 기존 체인 메커니즘이 다음 스테이지로.
- 실패·재개: 멤버 dead-letter → 그룹 대기(기존 stall) → RunTask 재개(기존).
  콜백 dead-letter → 꼬리가 콜백 메시지에 있으므로 RunTask로 그 지점부터
  (기존 체인 재개와 동일).
- 검증(Enqueue 시): ThenGroup의 그룹은 OnComplete 필수, 멤버·콜백에
  WithTaskID/WithUnique 금지(체인이 ID 소유), 그룹 스테이지가 마지막이어도
  허용(콜백이 마지막 링크), 빈 그룹 금지 — 기존 chain·group 사전 검증 결합.

### 표면·검증 (PR 2)

- webui: 태스크 상세 체인 스테퍼에 그룹 스테이지 표시(병렬 묶음 시각화 —
  형태는 구현 시 결정), `TaskInfo`의 체인 정보에 스테이지 종류 노출.
- 소크 워크로드에 ThenGroup 경로 추가(10초 티커의 chain을 2스테이지+그룹
  스테이지로 교체 또는 병행) — 결과 HASH(`groupresult`) 잔존을 소크 패밀리
  카운트로 검증(sampler의 group SCAN 패턴이 `group:*`라 `groupresult:*`도
  매치되는지 확인, 아니면 패턴 추가).
- cluster 스모크 19번째(ThenGroup 전 구간), tour 15 확장(팬아웃→팬인→후속),
  README.

## 알려진 한계 / 후속

- 그룹 멤버가 체인/그룹인 재귀 중첩 미지원(범위 외 — ChainLink 팽창·재개
  복잡도). 필요 시 후속 브레인스토밍.
- 워크플로 밖 임의 태스크의 결과 조회 API 없음(결과는 릴레이 전용).
  completed retention이 있으면 Inspector에서 결과 유무·크기만 확인 가능.
- chain의 기존 at-most-once caveat(후속 hash 존재 동안만)와 구조적으로 동일한
  창이 그룹 스테이지에도 적용된다 — 단일 링크보다 넓지 않다. 완료된 스테이지의
  펜스는 **콜백 hash**(`<chainID>:<i>:cb`)이며(멤버 create-if-absent가 아니라 —
  그건 스테이지 진행 중 멤버 중복만 막는다), 재전달 창은 **콜백의 retention**으로
  닫힌다. 멤버 retention은 이 창과 무관하다: 잔존 completed 멤버는 재실행이 아니라
  저장된 결과로 드레인(재보고)되므로, 재실행되는 것은 콜백+꼬리뿐이다. 즉 창을 닫는
  노브가 단일 링크(자신의 retention)와 달리 그룹 스테이지에서는 OnComplete 콜백의
  retention으로 이동한다.
- PrevResult 복제 내장이라 멤버 수 N인 그룹 스테이지는 앞 결과를 N번 복제
  (1MiB 상한 × N — 문서에 명시).
