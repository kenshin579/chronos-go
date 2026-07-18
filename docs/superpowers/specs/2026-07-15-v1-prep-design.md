# chronos-go v1.0.0 준비 설계

- 상태: 승인됨 (2026-07-15)
- 관련: 루트 패키지 전 공개 심볼(chronos.go/handler.go/chain.go/group.go/
  server.go/scheduler.go/inspector.go/retry.go/metrics.go), README, contrib/*
- 범위: 공개 API 감사 → 보수적 수정 → v1 문서 → 3중 게이트 → v1.0.0 태그.
  기능 추가 없음. 적극적 재설계 없음.

## 확정 결정 (브레인스토밍)

1. **보수적 동결** (질문1-a): 현재 API를 그대로 굳히고 **명백한 결함·불일치만**
   수정(오타·비일관 네이밍·검증 구멍·godoc↔코드 불일치). "더 나은 이름" 수준의
   취향 변경 없음. additive 개선은 v1.x 백로그. 애매한 항목은 사용자 사안별 결정.
2. **코어만 v1 안정성 보증** (질문2-a): 루트 패키지
   `github.com/kenshin579/chronos-go`만 semver 보증. `contrib/webui`·
   `contrib/prometheus`는 실험적(별도 버전, 보증 밖). 코어 `Metrics` 훅(의존성
   0)은 코어라 포함. contrib는 별도 go.mod라 태그도 독립.
3. **필수 문서 세트** (질문3-a): doc.go, Example 함수 6~8개(example_test.go),
   README Stability 소절, CONTRIBUTING.md, 알려진 한계 통합. asynq 마이그레이션
   가이드·아키텍처 문서는 v1.x 백로그.
4. **간결한 semver 보증문** (질문4-a): README에 몇 줄 — 코어 semver, breaking은
   major만, deprecation은 `// Deprecated:`로 최소 1 minor 예고, contrib/internal
   보증 밖, 지원 Go 최신 2 마이너. 상세 LTS/백포트 정책은 만들지 않음.
5. **3중 선언 게이트** (질문5-a): `make check` 그린 + cluster 스모크 20/20 +
   4시간 소크 3지표 PASS. 커버리지 임계·린터 게이트는 v1.x 백로그.

## 단계

### Phase A — API 감사 (발견 → 분류 → 체크포인트)

~60개 공개 심볼을 그룹별로 **독립 감사**. 그룹:
- ① 옵션 10종(WithQueue/WithTaskID/WithMaxRetry/WithDeadLetterDiscard/
  WithProcessIn/WithProcessAt/WithUnique/WithRetention/WithMisfirePolicy)
- ② 빌더 Chain(Then/ThenGroup/Enqueue)·Group(Add/AddChain/OnComplete/Enqueue)
- ③ Inspector(Queues/ListTasks/GetTask/RunTask/DeleteTask/PauseQueue/
  ResumeQueue/PausedQueues/GroupMembers/SchedulerStatus) + 반환 타입
  (QueueInfo/TaskInfo/ScheduleInfo/SchedulerStatus/GroupInfo)
- ④ Server(NewServer/Start/Shutdown/ServerConfig)·Scheduler(NewScheduler/
  Start/Shutdown/SchedulerConfig/RegisterInterval/RegisterCron/MisfirePolicy)
- ⑤ 제네릭 Task[T]/TaskArgs/Mux/AddHandler/AddHandlerR/Enqueue/Client/
  PrevResult/GroupResults/RawGroupResults
- ⑥ 에러·기타(SkipRetry/skipRetryError/ErrNoResult/ErrResultTooLarge/
  ErrDuplicateTask/ErrTaskNotFound/ErrInvalidState/MaxResultSize/
  DefaultRetryDelay/RetryDelayFunc/Metrics/TaskOutcome)

각 감사 점검 항목:
- 네이밍 일관성(같은 개념 = 같은 이름; 예 ID vs TaskID, Queue vs QueueName)
- 시그니처 에르고노믹스(제네릭 타입파라미터 순서 — 예 `PrevResult[R,T]`가 R만
  명시하고 T 추론되는가, ctx 위치, 반환 형태 `(T, error)` 일관)
- 에러 계약(센티넬 노출·`errors.Is` 래핑 일관, godoc에 문서화됐는가)
- godoc ↔ 코드 일치(주석이 실제 동작을 정확히 기술하는가)
- zero-value/기본값 동작(config 제로값이 안전한 기본으로 가는가)
- 하위호환(v0.x에서 이미 쓰던 시그니처를 깨는가 — 깨면 v1 전 마지막 기회로 정당화)

산출 = **findings 리포트**, 3분류:
- (i) 결함/불일치 → 수정(Phase B)
- (ii) 애매 → 사용자 사안별 결정(체크포인트)
- (iii) 확인됨 → 그대로

**체크포인트**: (ii)가 있으면 사용자에게 가져와 결정. 보수적 기준이라 대부분
(iii) 예상. (i)/(ii)가 비면 Phase B는 아래 확정 후보만.

### Phase B — 수정 (보수적)

(i)의 결함만. TDD + 회귀. 확정 후보:
- **pause 임의 큐 이름 수용**: `Inspector.PauseQueue`/CLI `queue pause`가 등록
  안 된 큐 이름을 받으면 `chronos:paused` SET에 고스트 멤버가 남음(`resume`으로
  복구 가능하나 UI 미표시). 감사에서 "등록된 큐만 허용(검증)" vs "문서화"
  최종 결정. **기본안: 검증 추가**(RegisterQueue된 이름이 아니면 에러) — 코어
  공개 API가 v1에서 임의 입력을 조용히 받지 않도록.
- 그 외는 Phase A 산출에 의존(보수적이라 소수 예상).

### Phase C — 문서

- `doc.go`(신규): 패키지 개요 + 핵심 개념 지도(Client/Server/Mux/Task/워크플로/
  Scheduler/Inspector). pkg.go.dev 첫 화면.
- `example_test.go`(신규): `Example`(기본 enqueue+handler), `ExampleAddHandlerR`,
  `ExampleNewChain`, `ExampleNewGroup`(ThenGroup·AddChain 포함), `ExampleNewScheduler`,
  `ExampleNewInspector` — 6~8개. `go test`로 컴파일·실행 검증(Output: 주석).
  Redis 필요한 예제는 Output 주석 없이 컴파일만 되게 하거나 `// Output:` 생략
  판단(구현 시 — pkg.go.dev 렌더는 Output 없어도 됨).
- README **Stability & Compatibility** 소절: 코어 semver 보증, breaking=major,
  deprecation `// Deprecated:` 최소 1 minor 예고, contrib/internal 보증 밖,
  지원 Go 최신 2 마이너.
- `CONTRIBUTING.md`(신규): 로컬 Redis, `make check`, `-p 1` 이유, cluster
  (`make test-cluster` + deploy/redis-cluster), 소크(`make soak`), PR 규칙(간결).
- **알려진 한계 통합**: 흩어진 caveat를 README 한 섹션으로 —
  at-least-once(멱등 핸들러 권고), fencing dedupTTL 의존, unique 락 TTL 커버리지
  (in-flight만 heartbeat), 2레벨 중첩 미지원, pause 반영 ~1s, 결과 1MiB 상한.
- contrib README(webui/prometheus) 상단에 "실험적 — v1 안정성 보증 밖, 별도
  버전 정책" 배지/문구.

### Phase D — 게이트 + 태그

최종 코드(감사·수정·문서 머지 후 main)에 대해 순서대로:
1. `make check` 그린
2. cluster 스모크 20/20 (deploy/redis-cluster + `make test-cluster`)
3. **4시간 소크**: `cd benchmarks && go run ./cmd/soak -duration 4h` — heap/
   goroutine/DBSIZE 3지표 PASS. 판정 표를 릴리스 노트에 첨부.
4. `v1.0.0` 태그 + GitHub 릴리스: 기능 요약(패리티+워크플로+성능+관찰) + 안정성
   보증 + 소크 결과.

## PR 전략

- **PR 1** = 감사 findings 반영 + 수정(Phase B) + 문서(Phase C). 감사 결과가
  소규모일 것으로 예상돼 한 PR. `make check` 그린 + cluster 20/20 확인 후 머지.
- **태그는 머지 후 별도**: main에서 4시간 소크 게이트 통과 → `v1.0.0` 태그.
  (4시간 소크는 PR CI가 아니라 로컬 게이트.)

## 알려진 한계 / 후속 (v1.x 백로그)

- asynq→chronos 마이그레이션 가이드(수요 확인 후).
- golangci-lint/staticcheck·커버리지 임계 CI 게이트.
- 2레벨 재귀 중첩(lazy materialization).
- Sentinel 공식 지원.
- webui 소소한 후속(queueDetail PausedQueues 오류 처리).

## 비고

- 감사는 읽기 전용 — Phase A 자체는 코드를 바꾸지 않음(findings만). 수정은 모두
  Phase B에서.
- 보수적 기준상 감사가 "수정 0건"으로 끝날 수도 있음(이미 리뷰된 API). 그 경우
  Phase B는 pause 검증만, 그래도 정상적 결과.
- 소크 중간에 코드가 바뀌면 무의미 → 게이트는 반드시 마지막.
