# Go 스케줄러 / 태스크 큐 라이브러리 비교 조사

> 작성일: 2026-07-05
> 목적: chronos-go(Redis 기반 분산 스케줄러 + 태스크 큐 라이브러리) 설계에 앞서
> 기존 Go 생태계의 대표 라이브러리를 조사하고, 계승할 강점과 개선할 약점을 정리한다.

---

## 1. 배경

현재 사내 `operator-review` 프로젝트는 `hibiken/asynq`를 래핑해 분산 잡 스케줄러(`JobScheduler`)를
구현해 쓰고 있다. 그러나 asynq는 메인테이너의 대역폭 부족으로 사실상 정체 상태이며,
직접 대체 라이브러리(chronos-go)를 개발하기로 했다.

**chronos-go 요구사항 (확정):**

- **분산 형태**: 멀티 인스턴스(Pod/서버) 환경에서 스케줄 잡이 클러스터 내 단일 실행되도록 보장
- **범위**: 스케줄러 + 태스크 큐 (asynq 계열). 재시도, 지연 실행, 워커 부하 분산 포함
- **백엔드**: Redis 전용
- **기능 범위(v1)**: interval/cron 스케줄 + 재시도/지연 실행 (미니멀 + 재시도/지연)
- **공개 형태**: 오픈소스
- **성공 기준**: operator-review의 `JobScheduler`를 asynq에서 chronos-go 구현체로 교체해도 동일 동작

### operator-review의 asynq 실사용 패턴 (설계 힌트)

- **고빈도 interval 잡이 주력**: `RegisterScheduledJob`이 15곳 이상, 간격 1~3초. `TaskCheckInterval: 100ms`로 지연에 민감.
- **재시도 미사용**: 모든 잡 `MaxRetry(0)`, 실패 시 로그+메트릭만 남기고 폐기(주기 잡이라 다음 주기에 재실행).
- **큐 해싱 부하 분산**: 태스크 이름/키를 FNV 해시해 N개 큐에 분배.
- **중복 방지**: 스케줄 잡은 `Unique(interval)`, Enqueue 태스크는 `WithUniqueTTL`로 dedup.
- **Retention(30s) + Unique의 실제 용도**: asynq 스케줄러의 다중 인스턴스 중복 enqueue를 막는 **우회책**.

즉 asynq의 방대한 기능 중 실제로 쓰는 것은 "분산 단일 실행 스케줄러 + 가벼운 dedup 큐"로 좁다.

---

## 2. 조사 대상 및 유지보수 상태 요약

| 라이브러리 | 계열 | 최신 릴리스 | 유지보수 상태 | 분산 단일 실행 | 라이선스 |
|---|---|---|---|---|---|
| robfig/cron | 인프로세스 스케줄러 | v3.0.0 (2019-06) | 정체 (6년째 릴리스 없음) | 없음 | MIT |
| go-co-op/gocron v2 | 인프로세스 스케줄러 | v2.21.2 (2026-05) | 활발 | Locker / Elector (내장) | MIT |
| reugn/go-quartz | 인프로세스 스케줄러 | v0.15.2 (2025-09) | 유지 (1인 중심) | 없음 (JobQueue 확장점만) | MIT |
| hibiken/asynq | Redis 태스크 큐 | v0.26.0 (2026-02) | **정체** (stable but stagnant) | **스케줄러엔 없음** | MIT |
| RichardKnop/machinery | 멀티백엔드 큐 | v2.0.16 (2025-08) | 얇게 유지 (버그 대응만) | 없음 | MPL-2.0 |
| vmihailenco/taskq | 멀티백엔드 큐 | v4.0.0-beta.4 (2023-05) | 사실상 중단 | 없음 | BSD-2 |
| riverqueue/river | Postgres 큐 | v0.40.0 (2026-07) | **매우 활발** | Leader election (내장) | MPL-2.0 |

---

## 3. 인프로세스 스케줄러 계열

### 3.1 robfig/cron (v3)

- **유지보수**: Star ~14.2k로 생태계 표준(K8s, Caddy, GitLab Runner 등에서 사용)이나 v3.0.0(2019) 이후 신규 릴리스 없음. 사실상 정체.
- **분산**: 없음. 다중 인스턴스 시 모든 노드가 각자 실행 → 사용자가 외부 락을 잡 함수 안에 직접 구현해야 함.
- **기능**: 5필드 cron(기본), `WithSeconds()`로 초 단위 6필드. `@every 1h30m` interval. 타임존 `CRON_TZ=` 인라인. 재시도/지연 없음. 중복 방지는 `SkipIfStillRunning`/`DelayIfStillRunning` 래퍼.
- **API**: 함수형 옵션의 원조(`cron.New(WithSeconds(), WithChain(...))`). **context.Context 미지원** — 잡 함수에 ctx를 전달하지 않아 취소/타임아웃/트레이싱 전파 불가(구조적 한계). 그레이스풀 셧다운은 `c.Stop()`이 완료 대기 ctx 반환.
- **신뢰성**: 인메모리, crash 시 스케줄 소실, missed run 복구 없음.
- **운영성**: `logr` 호환 로깅. 메트릭 전용 훅 없음(Chain으로 직접 계측).

### 3.2 go-co-op/gocron (v2)

세 인프로세스 라이브러리 중 가장 건강하며, 분산 설계의 참고 사례.

- **유지보수**: v2.21.2(2026-05)로 활발. Star ~7.1k. 락 구현체를 별도 저장소로 분리 운영.
- **분산 (핵심)**: 두 가지 상호배타적 모델을 인터페이스로 제공.
  - **Locker** (`WithDistributedLocker`): 매 실행마다 락 획득한 노드만 담당. Redis 구현(`gocron-redis-lock`, redsync 기반)은 TTL + `autoExtendDuration`으로 장기 잡 중 락 유지. 락 실패 시 `AfterLockError` 이벤트 발화(조용히 스킵 아님). 노드 crash 시 TTL 만료로 자동 해제.
    - **한계**: 노드 간 **clock skew에 취약** — 시계 빠른 노드가 락을 먼저 잡아 잡을 독점(부하 불균형). 공식 문서도 명시.
  - **Elector** (`WithDistributedElector`): 리더 선출. 리더만 스케줄·실행, 리더 다운 시 재선출.
- **기능**: `CronJob(spec, withSeconds)` 초 단위 지원. DurationJob/CronJob/DailyJob/OneTimeJob 등 다양. `WithStartAt`/`WithLimitedRuns` 지연·제한. 중복 방지 `WithSingletonMode(Reschedule|Wait)`, 동시성 제한 `WithLimitConcurrentJobs`. **재시도는 네이티브 없음**(잡 함수 내부 구현).
- **API**: 함수형 옵션 전면. `NewJob(jobDefinition, NewTask(fn, args...), jobOptions...)` — 잡 정의/태스크/옵션 분리가 명료. context.Context 1급(잡 함수 첫 인자가 ctx면 주입, 셧다운 시 취소). 이벤트 리스너(`BeforeJobRuns`, `AfterJobRunsWithError`, `AfterLockError`).
- **신뢰성**: 인메모리, in-flight 실행 유실, missed run 복구 없음.
- **운영성**: `SchedulerMonitor` 인터페이스로 실행 횟수/지연/동시성 히트 메트릭 훅 제공. 3종 중 관측성이 가장 정비됨.

### 3.3 reugn/go-quartz

Java Quartz에서 영감받은 zero-dependency 스케줄러. missed run 처리가 유일하게 명시적.

- **유지보수**: v0.15.2(2025-09), 꾸준하나 릴리스 주기 느긋. Star ~2.0k, 사실상 1인(bus factor 우려). 여전히 v0.x.
- **분산**: 코어엔 없음. 단 `JobQueue` 인터페이스를 커스텀 구현하면 외부 저장소 기반으로 확장 가능(영속화/분산 여지).
- **기능**: **초 단위 필드 기본 지원**. CronTrigger/SimpleTrigger/RunOnceTrigger. **재시도 네이티브**(`MaxRetries` + `RetryInterval`). `WithBlockingExecution`/`WithWorkerLimit`. Pause/Resume, 내장 잡(ShellJob/CurlJob/FunctionJob).
- **API**: `ScheduleJob(jobDetail, trigger)`. 함수형 옵션. **context.Context 완전 지원**(`Job.Execute(ctx)`). 셧다운 `Stop()` + `Wait(ctx)`.
- **신뢰성**: **misfire 처리 명시적** — `OutdatedThreshold`(기본 100ms) 초과 시 outdated 판정, `WithMisfiredChan`으로 놓친 잡을 채널 통지. 커스텀 `JobQueue`로 영속화 시 재시작 후 복원 가능.
- **운영성**: `WithLogger`. 메트릭 훅은 gocron만큼 정비되진 않음.

---

## 4. 분산 태스크 큐 계열

### 4.1 hibiken/asynq — 대체 대상 (Redis 큐)

**유지보수: "stable but stagnant"(안정적이지만 정체).**

- v0.24.1(2023-05) → v0.25.0(2024-11) 사이 **약 18개월 릴리스 공백**. 이후 저속 재개.
- 이슈 #626 "Need maintainers?"에서 메인테이너 hibiken 본인이 "최근 이 프로젝트에 집중할 여력이 없다"고 답변. 공동 메인테이너 자원자에게 권한을 주지 않았고, 포크(`go-asynq/asynq`)는 곧 폐쇄.
- v1.0.0 미도달(API 불안정 명시), #242 "Version 1?"은 5년째 방치.
- **조직화된 후계 포크가 없다 = chronos-go의 명확한 기회 공간.**

**사용자가 사랑하는 기능 (계승 대상):**

1. **Web UI (asynqmon)** — 압도적 1위 선택 이유. 큐/태스크 시각 검사·재시도·삭제.
2. **CLI 툴**
3. **단순한 API/DX** — `NewTask → Enqueue → Handler` 직관적 3단계.
4. **기능 완결성** — 재시도·지연·우선순위·유니크·주기·집계 원스톱.
5. **at-least-once + 워커 크래시 자동 복구**, Prometheus 메트릭 내장.

**사용자가 불평하는 문제점 (개선 대상):**

- **Scheduler 다중 인스턴스 중복 enqueue (최대 결함)**: `asynq.Scheduler`는 내장 리더 선출/분산 조정이 없다. 여러 인스턴스가 같은 스케줄을 `Register()`하면 각자 독립적으로 enqueue → 중복. 공식 위키가 "한 번에 스케줄러 하나만 실행하라"고 경고 → **단일 스케줄러 = SPOF**.
  - 커뮤니티 우회책: (1) 스케줄러 1개만 운영(SPOF), (2) 고정 `TaskID` + `Retention`으로 dedup(사용자가 직접 조립), (3) `PeriodicTaskManager`(동적 재구성 담당이지 중복 방지는 아님). **모두 우회책, 근본 해결 아님.**
  - operator-review가 `Retention(30s)` + `Unique`를 쓰는 이유가 바로 이 우회.
- **Redis Cluster 미지원**: README가 "일부 Lua 스크립트가 Cluster 비호환"이라 명시(Sentinel HA는 지원).
- **상태 전이 버그**: #420 "태스크 active 상태 영구 고착", #532 "취소한 태스크가 재시도로 진입", #764 recoverer 실패.
- **Unique 락 조기 만료**: 락 TTL과 처리 시간이 독립이라 처리가 TTL보다 오래 걸리면 중복 enqueue 가능.
- **런타임 동적 변경 불가**, **API 불안정(v1 미도달)**.

#### asynq 내부 구현 (소스 레벨, v0.26.0)

차용할 검증된 Redis 설계:

- **키 네이밍**: 큐별 prefix `asynq:{<qname>}:` — 중괄호는 **Redis Cluster hash tag**로 한 큐의 모든 키를 같은 슬롯에 배치, Lua 멀티키 연산이 클러스터에서 동작하게 함.
- **본문/인덱스 분리**: 태스크 본문은 `asynq:{<q>}:t:<id>` HASH(msg는 protobuf), 큐 인덱스는 ID만 담음.
  - pending: LIST, active: LIST, **lease: ZSET(score=만료 unix)**, scheduled/retry/archived/completed: ZSET(score=시각).
- **상태 전이의 Lua 원자화**: enqueue(`EXISTS` ID 중복검사 + `HSET` + `LPUSH`), dequeue(`RPOPLPUSH pending→active` + `HSET state` + `ZADD lease now+30s`를 단일 원자 연산), done/retry/archive/forward(scheduled·retry→pending 승격, 100개씩 배치) 모두 단일 Lua.
- **Lease + heartbeat + recoverer 3단 신뢰성 모델 (핵심)**:
  - dequeue 시 lease ZSET에 `now+30s` 등록.
  - heartbeater가 interval마다 유효한 lease만 `now+30s`로 연장.
  - 워커 crash → 하트비트 멈춤 → lease 만료 → recoverer가 `score ≤ now-30s` 조회해 retry/archive로 복구. cutoff에 30초 여유를 둬 clock skew 흡수.
  - **"lease 무효 시 워커는 Redis에 안 쓰고 recoverer에 위임"** — 이중 상태 이동 원천 차단.
- **Scheduler**: `robfig/cron/v3`를 인메모리로 래핑. **중복 enqueue 방지 장치 없음**(위 최대 결함).
- **Unique**: `SET uniquekey taskID NX EX ttl`. done 시 `GET==taskID`일 때만 `DEL`. **락 자동 연장 없음**(개선 대상).
- **서버 고루틴**: processor(실행), forwarder(scheduled/retry→pending 승격), recoverer(lease 만료 복구), syncer(redis 쓰기 실패 재시도), heartbeater(상태 기록+lease 연장), subscriber(취소 pub/sub), janitor(retention 만료 정리), healthchecker, aggregator.
- **의존성**: go-redis/v9, robfig/cron/v3, protobuf, google/uuid, spf13/cast. 매우 적음.
- **개선 여지**: (1) 스케줄러 리더 선출 부재, (2) Unique 락 자동 연장 부재, (3) 폴링 기반 dequeue(지연 vs 부하 트레이드오프; BRPOPLPUSH가 부하 크다는 이유로 폴링 채택), (4) BatchEnqueue 비원자성, (5) ExtendLease GT 옵션 미사용.

### 4.2 RichardKnop/machinery & vmihailenco/taskq — 멀티백엔드의 함정

두 라이브러리의 쇠퇴는 **다중 백엔드 추상화가 유지보수를 잡아먹는다**는 강한 근거.

**machinery**
- 브로커 4종(AMQP/Redis/SQS/GCP Pub/Sub) × 결과 백엔드 5종. 브로커/결과 저장소 분리는 강점이나, 최근 커밋 대부분이 백엔드별 버그 땜질("Redis Sentinel 인증 수정", "Redis cluster 단일 엔드포인트", 드라이버 범프)로 채워짐. 신규 기능 대역폭 소진.
- **워커 crash 시 in-flight 태스크 복구 없음**(이슈 #253, 2018년부터 Open). 브로커 추상화가 최소공통분모로 내려가 라이브러리 레벨 일관 복구 실패.
- **리플렉션/문자열 시그니처 API**: `Signature`에 `{Type:"int64", Value:...}`를 손으로 나열. 컴파일 타임 타입 안전 없음 → 인자 불일치 시 **런타임 panic**. JSON 직렬화 타입만 지원. 리팩터링 취약.
- 워크플로(chain/group/chord)가 셀링 포인트지만 유지 부담.

**taskq**
- Redis/SQS/IronMQ/in-memory. SQS의 visibility timeout + 메시지 예약 모델을 공통 인터페이스로 삼음.
- **cron/워크플로 없음**. 처리량(자동 스케일/배치/압축/레이트리밋)에 강점.
- crash 복구는 visibility timeout 만료 후 자동 재전달로 machinery보다 구조적으로 나음.
- 원저자가 go-redis/bun/Uptrace로 이동, **2023년경부터 사실상 중단**. IronMQ 백엔드는 서비스 쇠퇴로 죽은 코드.

**공통 실패 원인**: 표면적(백엔드 × 기능 매트릭스)이 유지 인력 대비 너무 넓었다.

### 4.3 riverqueue/river — 현대적 설계의 모범 (Postgres 큐)

2023년 이후 신생답게 현대적 Go 설계를 채택, 가장 활발.

- **유지보수**: v0.40.0(2026-07), Star ~5.4k, 전담 팀(brandur 등) + **open-core 수익 모델**(River Pro 유료 구독으로 workflows/sequences/concurrency control 제공).
- **분산 (leader election, 핵심 참고)**:
  - `river_leader` unlogged 테이블로 현재 leader 관리, **TTL 5초**(짧아 빠른 failover), graceful shutdown 시 리더십 반납 + **Postgres LISTEN/NOTIFY로 즉시 재선출 유도**.
  - periodic 잡 삽입/유지보수는 **leader만** 수행 → 다중 노드 중복 없음.
  - 정직한 한계 문서화: 자정 경계에서 leader 교체 시 놓칠 수 있음 → unique + `RunOnStart`로 완화.
- **API (제네릭 타입 안전, 참고)**:
  - `JobArgs` 인터페이스(`Kind() string`) + `Worker[T]` + `river.Job[T]`. `job.Args`로 강타입 접근, `json.Unmarshal` 보일러플레이트 없음("100% reflect-free"). `WorkerDefaults[T]` 임베드로 기본 동작 + 선택 override.
  - 함수형 옵션 `InsertOpts`(MaxAttempts/Priority/Queue/ScheduledAt/UniqueOpts/Tags). "JobArgs 기본 옵션 + 삽입 시 override" 이중 레이어.
- **기능**: periodic/cron 내장, 지수 백오프 재시도, `ScheduledAt` 지연, unique jobs, 우선순위 1~4, COPY 배치 삽입. 워크플로는 Pro 전용.
- **신뢰성**: **트랜잭셔널 enqueue**(앱 DB 변경과 동일 트랜잭션), at-least-once, **rescuer**(`RescueStuckJobsAfter` 경과 시 stuck 잡 재큐잉/discard).
- **운영성**: River UI(별도), 유지보수 서비스(scheduler/rescuer/reindexer/cleaner), CLI 마이그레이터.

---

## 5. chronos-go 설계 반영 교훈 (종합)

### A. asynq에서 그대로 차용 — 검증된 Redis 설계

1. **본문(HASH) / 인덱스(LIST·ZSET) 분리 + 상태 전이의 Lua 원자화** — 부분 실패 없음.
2. **Redis Cluster hash tag(`{qname}`)** 로 큐 단위 멀티키 원자성 확보(asynq가 놓친 Cluster 지원을 처음부터).
3. **Lease + heartbeat + recoverer 3단 신뢰성 모델** — 크래시 자동 복구.
4. **"lease 무효 시 recoverer에 위임"** 규칙 — 이중 처리 방지.
5. **단순한 DX** — `NewTask → Enqueue → Handler` 흐름과 Web UI/CLI/Prometheus 운영 툴링.

### B. asynq에서 반드시 고칠 것 — chronos-go의 차별점

1. **스케줄러 분산화를 라이브러리에 내장** — river식 leader election을 Redis(`SET NX PX` + pub/sub 사임 통지)로 구현. **최대 차별점.** operator-review가 `Retention`+`Unique`로 우회하던 문제를 프레임워크가 기본 해결.
2. **Unique 락 자동 연장** — lease처럼 연장하거나 fencing token 도입해 완료 전 만료 방지.
3. **misfire 정책 명시화** — 재시작 시 놓친 실행을 "즉시 만회 / 스킵 / 다음 예정만" 잡별 옵션으로(go-quartz 교훈).
4. **Redis Cluster 완전 지원**, **런타임 동적 제어**, **안정 v1 API**.

### C. 현대적 API 설계 — river 교훈

1. **제네릭 기반 타입 안전** `Handler[T]` / `Job[T]` — machinery·taskq의 리플렉션·문자열 시그니처(런타임 panic) 안티패턴 탈피. robfig/cron의 ctx 부재도 피함.
2. **함수형 옵션**(`InsertOpts`/`EnqueueOpts` 스타일), **context.Context 1급 지원**(핸들러 `func(ctx, payload T) error`, 셧다운 시 취소).
3. 스케줄러는 "언제 큐에 넣을지"만, 큐는 재시도(지수 백오프+최대횟수+dead-letter)와 dedup(idempotency key)을 1급으로 — **관심사 분리**.

### D. 지속가능성 — machinery/taskq 실패 교훈

1. **Redis 전용 유지** — 다중 백엔드 추상화 유혹을 코어 인터페이스에 미리 반영하지 말 것(machinery v1의 실패).
2. **좁고 뾰족한 코어** — cron 스케줄링 + at-least-once 큐를 완성도 높게. chain/group/chord는 코어 안정화 후 선택 레이어로.
3. **관대한 라이선스**(MIT/Apache-2.0)로 기여 진입장벽 낮추기.

---

## 6. 미해결 설계 논점 (다음 단계)

- **Redis 자료구조 근간**: (A) asynq식 LIST+ZSET 폴링 답습, (B) Redis Streams Consumer Group(네이티브 lease/ack/PEL), (C) 하이브리드(지연·재시도는 ZSET, 즉시 실행은 Streams).
  - 고빈도(1~3초) 실사용 패턴에서 폴링 지연이 부담이므로 B/C가 신뢰성·지연 면에서 유리하나 복잡도 증가.
- 스케줄러 leader election의 구체 구현(Redis 락 + pub/sub 통지)
- 제네릭 API의 등록/직렬화 매핑 구체안
- misfire catch-up 정책의 기본값

이 논점들은 설계(design) 단계에서 확정한다.

---

## 참고 출처

- asynq: https://github.com/hibiken/asynq (이슈 #626, #653, #420; 소스 `internal/base/base.go`, `internal/rdb/rdb.go`)
- gocron v2: https://github.com/go-co-op/gocron, gocron-redis-lock
- go-quartz: https://github.com/reugn/go-quartz
- robfig/cron: https://github.com/robfig/cron
- machinery: https://github.com/RichardKnop/machinery (이슈 #253)
- taskq: https://github.com/vmihailenco/taskq
- river: https://github.com/riverqueue/river, https://riverqueue.com/docs/leader-election
