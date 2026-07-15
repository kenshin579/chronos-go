# v1.0.0 공개 API 감사

- 대상: 루트 패키지 `chronos`의 전 공개 심볼
  (`chronos.go` / `handler.go` / `chain.go` / `group.go` / `server.go` /
  `scheduler.go` / `inspector.go` / `retry.go` / `metrics.go`, 보조로
  `schedule.go`·`codec.go`)
- 방식: 6그룹(① 옵션 10종 ② 빌더 Chain/Group ③ Inspector+반환타입
  ④ Server/Scheduler config ⑤ 제네릭 Task[T]/Mux/AddHandler(R)/Enqueue/
  Register/PrevResult/GroupResults ⑥ 에러·기타)으로 나눠 네이밍 일관성·시그니처
  에르고노믹스·에러 계약·godoc↔코드 일치·zero-value/기본값·하위호환 6점검.
- 기준: **보수적 동결**. 명백한 결함·불일치만 (i), "더 나은 이름" 취향은 (iii),
  애매하면 (ii). 코드는 이 감사에서 변경하지 않음(findings 전용).

---

## (i) 결함/불일치 — 수정 대상

### 1. `WithTaskID` godoc — 낡은 마일스톤 표현 + 이제 거짓인 서술

- **심볼**: `WithTaskID(id string) Option` (`chronos.go:168-174`)
- **문제**: 주석이
  > "Enforced deduplication is provided by the unique lock introduced in a
  > later milestone; in M1, re-enqueueing with the same ID is not guaranteed to
  > prevent duplicates."

  라고 되어 있다. (a) 사용자에게 무의미한 내부 마일스톤 용어("M1", "a later
  milestone")를 노출하고, (b) "later milestone"으로 미룬 unique 락이 **이미 같은
  파일의 `WithUnique`로 출시**되어 서술이 과거·미래 시제가 뒤엉킨 채 사실과
  어긋난다. (c) 더 중요한 오해: `WithTaskID`는 애초에 **중복 방지 기능이 아니다** —
  dedup은 `WithUnique`(kind+payload 기준)의 몫이고 `WithTaskID`는 상관관계/조회용
  명시 ID 지정일 뿐이다. 현재 문장은 "ID로 dedup이 언젠가 된다"는 잘못된 기대를
  준다.
- **근거**: godoc↔코드 일치 위반, 에러 계약(중복 시 동작) 오도. `WithUnique`
  godoc(`chronos.go:208-225`)이 실제 dedup 계약을 정확히 기술하고 있어 대비된다.
- **제안**: 현재 동작만 기술하도록 재작성 — "명시 task ID를 지정한다(조회·상관관계
  용도). 중복 제거는 하지 않는다 — dedup은 `WithUnique`를 쓰라. 생략 시 임의
  UUID." **비파괴(문서만)**.

### 2. `ServerConfig.Queues` — 사실상 필수 필드인데 미문서화

- **심볼**: `ServerConfig.Queues map[string]int` (`server.go:59`),
  `Server.Start` (`server.go:228-230`)
- **문제**: `ServerConfig{}` 제로값은 그대로는 구동 불가다 — `Start`가
  `errors.New("chronos: server requires at least one queue")`로 즉시 실패한다.
  그런데 `Queues` godoc은 가중치 의미만 설명하고 **"큐가 최소 1개 필요하며 암묵적
  기본 큐가 없다"는 계약을 전혀 밝히지 않는다**. asynq는 미설정 시 `default` 큐를
  암묵 사용하는 데 반해 chronos는 명시 요구라 이질적이다(정당한 설계지만 문서 공백).
  zero-value/기본값 + godoc↔코드 완결성 결함.
- **근거**: 제로값 config가 안전한 기본으로 귀결되지 않고 명시 에러로 끝나는데
  필드 godoc이 이를 언급하지 않음. (Task 3 `doc.go`/Task 4 예제 초안이
  `ServerConfig{Concurrency: 10}`를 `Queues` 없이 쓰고 있어 그 예제도 이 계약대로
  `Queues`를 넣어야 실제로 뜬다 — 문서 정확성에 직접 영향.)
- **제안**: `Queues` godoc에 한 줄 추가 — "At least one queue is required; there
  is no implicit default queue and `Start` returns an error if `Queues` is
  empty." **비파괴(문서만)**.

---

## (ii) 애매 — 사용자 결정 필요

### 1. `PauseQueue`가 임의 큐 이름을 수용 → paused SET 고스트 멤버 (필수 항목)

- **심볼**: `Inspector.PauseQueue(ctx, qname)` (`inspector.go:74-78`) →
  `rdb.PauseQueue`(`internal/rdb/pause.go:11`, `SADD chronos:paused qname`)
- **관찰**: `PauseQueue`는 큐 이름을 검증 없이 받아 `chronos:paused` SET에
  아무 문자열이나 넣는다. 존재하지 않는 큐 이름을 pause하면 SET에 고스트 멤버가
  남는다.
- **분석(계획 그대로)**:
  > `Inspector.PauseQueue`가 임의 큐 이름을 받아 `chronos:paused` SET에 고스트
  > 멤버를 남길 수 있다. 단, `chronos:queues`(등록 큐 SET)는 **enqueue 시점 lazy
  > 등록**이라, 태스크가 아직 없는 "설정만 된 큐"를 pause하는 정당한 경우를
  > 등록-여부 검증이 **거짓 거부**한다. 따라서 코어 `PauseQueue`에 검증을 넣는
  > 것은 위험. 선택지 — **A) 문서화만**(고스트 멤버는 `resume`으로 복구 가능,
  > UI엔 미표시라 무해), **B) CLI/webui 계층에서만** 알려진 큐가 아니면
  > 경고(코어는 그대로). 권고: A 또는 B, 코어 검증은 하지 않음.
- **선택지**:
  - **A) 문서화만**: `PauseQueue` godoc에 "존재하지 않는 큐 이름도 받으며 그 경우
    `ResumeQueue`로 해제할 때까지 paused 목록에 남는다(활성 큐엔 무영향)" 추가 +
    `cmd/chronos` `queue pause` 도움말/README Known limitations 반영. 코어 불변.
  - **B) CLI 경고**: `cmd/chronos/main.go`의 `queue pause` 핸들러에서
    `insp.Queues()`에 없는 이름이면 stderr 경고 후 **계속 진행**(거부 아님 —
    설정만 된 큐 정당). 코어 `PauseQueue`는 여전히 불변.
- **트레이드오프**: A는 코드 무변경·최소 비용이나 CLI 사용자에게 오타 피드백이
  없다. B는 흔한 오타를 즉시 알려주나 CLI 계층에 얕은 검증 로직이 늘고 "설정만 된
  큐" 정당 케이스를 경고로만(거부 아님) 남긴다. **코어 검증(등록 큐만 허용)은
  두 선택지 모두 배제** — lazy 등록 때문에 거짓 거부 위험.

### 2. `RunTask`/`DeleteTask`가 없는 태스크에 `ErrTaskNotFound`를 내지 않음 (GetTask와 비대칭)

- **심볼**: `Inspector.RunTask` / `DeleteTask` (`inspector.go:251-259`) vs
  `Inspector.GetTask` (`inspector.go:126-133`)
- **관찰**: `GetTask`는 `redis.Nil`을 `ErrTaskNotFound`로 매핑해 명시적으로
  없음을 알린다. 반면 `RunTask`/`DeleteTask`는 rdb에 그대로 위임하는데, rdb 구현이
  **없는 태스크에 대해 멱등 no-op**이라(`RunTask`는 Lua가 대상 없으면 조용히 통과,
  `DeleteTask`는 ZREM/DEL이 no-op → 둘 다 `nil` 반환), 호출자·CLI가
  "실행/삭제됨"과 "애초에 없었음"을 구분할 수 없다. 노출된 Inspector 표면에서
  에러 계약이 서로 다르다.
- **선택지**:
  - **A) 현행 유지 + 문서화**: 멱등 삭제/재실행은 관행상 정상. 두 godoc에
    "대상이 없으면 no-op(에러 아님)" 한 줄 추가. **비파괴**.
  - **B) `RunTask`만 `ErrTaskNotFound` 반환**: 아무 것도 승격 못 했을 때
    `ErrTaskNotFound`를 내 계약을 `GetTask`와 통일(`DeleteTask`는 멱등 유지).
    **동작 변경 — `nil`을 기대하던 호출자에 breaking**, v1 전 마지막 기회.
- **트레이드오프**: A는 안전·무변경이나 비대칭 잔존. B는 일관된 계약을 주지만
  `RunTask` 반환 의미를 바꿔 하위호환을 깬다(승인 필요).

---

## (iii) 확인됨 — 그대로 동결

- **① 옵션 10종** (`chronos.go`): `optionFunc` 패턴 일관, ctx 무관 순수 setter.
  안전한 클램핑 확인 — `WithMaxRetry` n<0→0, `WithRetention` <1s→1s·≤0 무시,
  `WithProcessAt`/`WithProcessIn` 과거/비양수→즉시. `WithMisfirePolicy`는 "plain
  Enqueue에선 무시"됨이 godoc에 명시(체인에선 rejct 대신 무시라는 비대칭이 있으나
  문서화되어 수용). 기본값 상수 `DefaultQueue`/`DefaultMaxRetry` 공개·문서화. 문제
  없음(단, `WithTaskID` 문서는 (i)#1).
- **② 빌더 Chain/Group** (`chain.go`/`group.go`): 플루언트 체이닝, 검증 에러
  메시지가 원인·해결책을 구체적으로 서술. 1레벨 중첩 제한(멤버 체인 내 ThenGroup
  금지·그룹-of-그룹 금지)을 명시적 에러로 강제. 상대 지연/GroupTTL 상한 가드 일관.
  `WithTaskID`/`WithUnique`/중간 링크 `WithDeadLetterDiscard`를 Enqueue 시점에
  일관 거부. `NewChain`/`NewGroup` 제로 빌더 안전. 반환 `*TaskInfo`/`*GroupInfo`
  일관. 문제 없음.
- **③ Inspector + 반환 타입** (`inspector.go`): 센티넬 `ErrTaskNotFound`/
  `ErrInvalidState`를 `%w`로 래핑해 `errors.Is` 검사 가능, `ErrInvalidState`는
  허용 상태 목록을 메시지에 포함. `QueueInfo`/`TaskInfo`/`ScheduleInfo`/
  `SchedulerStatus`/`GroupInfo` 필드명·주석 명확(특히 `TaskInfo.GroupPending`이
  "hint, not authority"임을 정직하게 서술). 파라미터명 `qname`/`cbQueue`/`name`
  혼재는 godoc 시그니처에 보이나 순수 표기상 사안으로 무해(동결). 반환 형태
  `[]*QueueInfo`/`[]*TaskInfo`(포인터 슬라이스) vs `SchedulerStatus.Schedules
  []ScheduleInfo`(값 슬라이스)의 불일치가 있으나 작은 값 구조체의 값 슬라이스는
  정당하고 변경은 breaking이라 동결. (RunTask/DeleteTask 에러 비대칭만 (ii)#2로.)
- **④ Server/Scheduler config** (`server.go`/`scheduler.go`): `NewServer`가
  전 필드에 안전 기본값 적용(Concurrency≤0→1, ForwardInterval/RecoverInterval/
  RecoverMinIdle/ArchivedRetention/JanitorInterval, MaxArchived/MaxCompleted
  0→기본·음수→비활성 문서화). `HeartbeatInterval`은 RecoverMinIdle 대비 클램프 +
  `time.Millisecond` 바닥으로 `NewTicker` 패닉 회피. `SchedulerConfig{}` 제로값은
  완전 사용 가능(Location→Local, Logger→Default, LeaderTTL→5s). `ServerConfig`는
  Queues 필수만 (i)#2. 문제 없음.
- **⑤ 제네릭** (`chronos.go`/`handler.go`/`scheduler.go`): 타입파라미터 순서가
  추론 요구에 맞게 설계됨 — `PrevResult[R any, T TaskArgs]`·
  `GroupResults[R any, T TaskArgs]`는 R을 명시하면 T가 인자에서 추론(`PrevResult
  [EncodeResult](task)` 동작 확인). `AddHandler[T]`/`AddHandlerR[T, R]`는 둘 다
  `fn` 시그니처에서 추론되어 순서 무관. `Enqueue`/`RegisterInterval`/
  `RegisterCron`은 ctx-first·`(*TaskInfo, error)`/`error` 반환 일관. `Task[T]`는
  `Args` 공개·`id`/`queue`는 `ID()`/`Queue()` 접근자, `RawGroupResults()`도 접근자.
  중복 등록 panic·value-receiver `Kind()` 요구가 godoc에 명시. `TaskArgs`
  인터페이스 최소(단일 `Kind()`). 문제 없음.
- **⑥ 에러·기타** (`retry.go`/`metrics.go`/`handler.go`/`chronos.go`):
  `ErrDuplicateTask`가 `rdb.ErrDuplicateTask` 별칭이라 `errors.Is` 관통.
  `ErrNoResult`/`ErrResultTooLarge` 센티넬 + `%w` 래핑 일관. `SkipRetry`가
  `Unwrap` 구현으로 `errors.As` 검사 가능(핸들러 입력 마커라 공개 검사 함수
  불필요). `MaxResultSize = 1<<20` 공개·초과 시 SkipRetry로 데드레터 처리 문서화.
  `DefaultRetryDelay`(full-jitter)·`RetryDelayFunc` 시그니처 명확. `Metrics`는
  nil-safe(zero/nil 비활성 명시)·동시성 안전 요구 문서화. `TaskOutcome` 상수
  snake_case 일관(`success`/`retry`/`dead_letter`). `MisfirePolicy` 제로값이
  안전 기본(`MisfireSkip`), `MisfireRunAll` 의도적 미지원 문서화. 문제 없음.

---

## 요약

- (i) 2건: `WithTaskID` godoc 정정, `ServerConfig.Queues` 필수 문서화 — 둘 다
  **비파괴(문서만)**.
- (ii) 2건: PauseQueue 고스트 멤버(A 문서화 / B CLI 경고, 코어 검증 배제) —
  **필수 항목**; RunTask/DeleteTask 에러 비대칭(A 문서화 / B RunTask만 breaking).
- (iii): 6그룹 전반 동결 확인 — 옵션 기본값·클램핑, 빌더 검증·중첩 제한, 센티넬
  `errors.Is` 관통, config 안전 기본값, 제네릭 추론 순서, 에러/메트릭 계약 모두
  일관.
