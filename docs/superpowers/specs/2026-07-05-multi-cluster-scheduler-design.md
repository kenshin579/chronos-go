# chronos-go 설계 문서

> 작성일: 2026-07-05
> 상태: 설계 승인 대기
> 관련 조사: [Go 스케줄러/태스크 큐 라이브러리 비교](../../research/2026-07-05-go-scheduler-library-comparison.md)

---

## 1. 개요

chronos-go는 **Redis 기반 분산 스케줄러 + 태스크 큐** 라이브러리다. 멀티 인스턴스(Pod/서버)
환경에서 스케줄 잡이 클러스터 내 **단일 실행**되도록 보장하고, 재시도·지연 실행·워커 부하 분산을
제공한다. 사내 `operator-review`가 쓰던 `hibiken/asynq`(사실상 유지보수 정체)의 대체를 목표로 한다.

### 확정된 요구사항

| 항목 | 결정 |
|---|---|
| 분산 형태 | 멀티 인스턴스 단일 실행 보장 |
| 범위 | 스케줄러 + 태스크 큐 (재시도, 지연 실행, 부하 분산 포함) |
| 백엔드 | **Redis 전용** (다중 백엔드 추상화 없음) |
| 기능 범위(v1) | 미니멀 + 재시도/지연 실행 |
| 공개 형태 | 오픈소스 |
| 라이선스 | MIT 또는 Apache-2.0 (기여 진입장벽 최소화) |
| 성공 기준 | operator-review의 `JobScheduler`를 chronos-go 제네릭 API로 재작성해도 동작 동일 |

### 핵심 설계 결정 요약

| # | 영역 | 결정 |
|---|---|---|
| 1 | Redis 자료구조 | **하이브리드** — 즉시 실행은 Streams Consumer Group, 지연/재시도/예약은 ZSET |
| 2 | 태스크 API | **제네릭 타입 안전** (river 방식, `TaskArgs.Kind()` + `Task[T]`) |
| 3 | 스케줄러 분산 | **리더 선출 + 결정적 TaskID 안전망** (이중화) |
| 4 | 실패 처리 | dead-letter(archived) 보관 기본 + `OnDeadLetter` 훅 + 잡별 폐기 모드 |
| 5 | 운영 툴링 | 코어 + 메트릭 + CLI (**Web UI는 v1 제외**, Inspector API는 코어 내장) |
| 6 | 스케줄 하한 | interval ≥ 1s, 위반 시 에러 (leader failover 공백 때문) |

---

## 2. 아키텍처

### 컴포넌트 (3역할)

- **Client** — 태스크를 큐에 넣음(`Enqueue`). 즉시/지연 실행.
- **Scheduler** — cron/interval 스케줄 관리. **리더로 선출된 인스턴스만** due 잡을 큐에 투입.
- **Server** — 워커. Streams에서 태스크를 꺼내 등록된 `Handler[T]`로 처리.

### 백그라운드 고루틴

| 고루틴 | 역할 |
|---|---|
| `processor` | Streams `XREADGROUP BLOCK`으로 소비 → 핸들러 실행 → `XACK` |
| `forwarder` | scheduled/retry ZSET의 due 항목을 Stream으로 승격(`XADD`) |
| `recoverer` | Streams PEL에서 죽은 워커의 미ACK 태스크를 `XAUTOCLAIM`으로 회수 |
| `leaderElector` | Redis 락으로 리더 선출·갱신, graceful 시 사임 통지 |
| `scheduleTicker` | (리더만) cron/interval 평가 → due 잡 enqueue |
| `janitor` | completed/archived retention 만료분 정리 |
| `healthchecker` | Redis 연결 상태 감시 |

**asynq 대비 핵심 개선**: asynq는 lease ZSET + heartbeater + recoverer를 Lua로 손수 구현했다.
chronos-go는 Streams Consumer Group의 **PEL이 lease를 네이티브로 대체**하므로(`XAUTOCLAIM min-idle-time`),
신뢰성 코드가 대폭 축소된다.

### 모듈 구조

```
chronos-go/
├── go.mod                    # deps: redis/go-redis/v9, robfig/cron/v3(파서만 재사용)
├── chronos.go                # 공개 API: Client, Server, Scheduler, Mux, Task, 옵션
├── inspector.go              # Inspector: 큐/태스크 상태 조회 (CLI 기반, 향후 UI 기반)
├── internal/
│   ├── base/                 # 키 네이밍, 상태 정의, 태스크 메시지 직렬화
│   ├── rdb/                  # Redis 연산 + Lua 스크립트 (원자적 상태 전이)
│   └── core/                 # processor, forwarder, recoverer, elector, ticker
└── cmd/chronos/              # CLI (chronos queue ls, task retry <id> …)
```

설계 원칙:
- 공개 API는 루트 패키지에 평평하게. 내부 복잡도는 `internal/`에 은닉.
- `internal/base`가 키 규칙·상태 정의의 단일 진실 공급원.
- cron 파싱은 robfig/cron/v3 파서만 재사용(스케줄러로는 정체됐어도 파서는 견고).

---

## 3. Redis 데이터 모델

### 키 네이밍 (Cluster 호환)

모든 키에 큐 이름을 **hash tag `{}`** 로 감싸 같은 슬롯에 배치 → Lua 멀티키 연산이 Cluster에서 동작
(asynq가 놓친 Cluster 지원을 처음부터 확보).

```
chronos:{<queue>}:stream        # Stream — 즉시 실행 대기 (Consumer Group)
chronos:{<queue>}:t:<id>        # HASH   — 태스크 본문/상태 (payload, kind, state, attempt…)
chronos:{<queue>}:scheduled     # ZSET   — 지연/예약 (score = process_at)
chronos:{<queue>}:retry         # ZSET   — 재시도 대기 (score = retry_at)
chronos:{<queue>}:archived      # ZSET   — dead-letter (score = died_at)
chronos:{<queue>}:completed     # ZSET   — 완료 보관 (score = expire_at, retention)
chronos:{<queue>}:dedup:<key>   # STRING — 결정적 TaskID dedup (SET NX)
chronos:leader                  # STRING — 리더 선출 락 (SET NX PX, 값=instance id)
chronos:queues                  # SET    — 등록된 큐 목록
```

### 태스크 생애주기

**즉시 실행 경로** (Streams Consumer Group):
```
Enqueue → XADD stream (state=pending)
       → XREADGROUP BLOCK (processor 소비, PEL 진입, state=active)
       → 성공: XACK + state=completed(또는 삭제)
       → 실패: XACK + retry ZSET로 이동
       → 워커 crash: PEL에 미ACK 잔존 → XAUTOCLAIM으로 회수
```

**지연/예약/재시도 경로** (ZSET → Stream 승격):
```
Enqueue(delay)/재시도 → scheduled|retry ZSET (score=실행시각)
       → forwarder가 ZRANGEBYSCORE(-inf, now)로 due 조회
       → XADD stream + ZREM (단일 Lua로 원자화)
```

### 본문/인덱스 분리

Stream 엔트리에는 `task_id`만, 본문·상태는 `t:<id>` HASH에 저장(asynq 계승). 상태 전이 시
인덱스 이동 + HASH `state` 갱신을 **단일 Lua로 원자 처리** → 부분 실패 없음. Inspector는
`t:<id>` HASH만 읽으면 태스크 전체 상태 조회 가능.

### 크래시 복구: PEL이 lease를 대체

- `XREADGROUP` 읽기 = 해당 consumer의 PEL 등록(lease 획득).
- 성공 시 `XACK` = PEL에서 제거(lease 반납).
- 워커 crash → XACK 없음 → PEL에 idle 잔존.
- recoverer가 `XAUTOCLAIM <min-idle-time>`으로 오래 idle된 엔트리를 살아있는 워커로 원자 재할당.
  PEL의 `delivery count`가 재시도 횟수를 추적 → 초과 시 archived로.

`min-idle-time`은 lease 만료 시간 역할(기본 30s, 설정 가능).

### forwarder 폴링 주기

기본 100ms(설정 가능). 지연/예약 태스크의 정밀도 하한을 결정한다. 서브초 "스케줄"과는 무관하며,
지연 실행 정밀도를 사용자가 조절할 수 있게 한다.

---

## 4. 공개 API (제네릭 타입 모델)

### 태스크 정의

```go
type EmailArgs struct {
    UserID string `json:"user_id"`
    Body   string `json:"body"`
}

func (EmailArgs) Kind() string { return "email:send" }
```

`Kind() string` 하나만 요구하는 `TaskArgs` 인터페이스가 태스크 타입의 전부. `Kind`가 Redis
저장 식별자이자 핸들러 라우팅 키.

### 핸들러 등록과 서버

```go
mux := chronos.NewMux()

// Go 메서드는 타입 파라미터 불가 → 패키지 레벨 제네릭 함수로 등록 (river 방식)
chronos.AddHandler(mux, func(ctx context.Context, task *chronos.Task[EmailArgs]) error {
    return send(ctx, task.Args.UserID, task.Args.Body)  // 강타입 접근, Unmarshal 불필요
})

srv := chronos.NewServer(redisClient, chronos.ServerConfig{
    Queues:      map[string]int{"default": 1},
    Concurrency: 8,
})
srv.Start(ctx, mux)
srv.Shutdown(ctx)
```

### Enqueue

```go
client := chronos.NewClient(redisClient)

info, err := chronos.Enqueue(ctx, client, EmailArgs{UserID: "u1", Body: "hi"},
    chronos.WithQueue("critical"),
    chronos.WithProcessIn(5*time.Minute),   // 지연 → scheduled ZSET
    chronos.WithMaxRetry(3),
    chronos.WithUnique(10*time.Minute),      // dedup 키 = kind + args 해시
    chronos.WithTaskID("custom-id"),
)
```

### 스케줄 등록 (리더 선출 내장)

```go
sched := chronos.NewScheduler(redisClient, chronos.SchedulerConfig{Location: time.Local})

// interval — 1초 미만은 에러
chronos.RegisterInterval(sched, 3*time.Second, DisasterCheckArgs{},
    chronos.WithDeadLetterDiscard(),  // 고빈도 잡: 실패 시 보관 없이 폐기
)
chronos.RegisterCron(sched, "0 0 * * *", DailyReportArgs{})

sched.Start(ctx)  // 전 인스턴스에서 호출해도 안전 — 리더만 enqueue
```

**asynq와의 결정적 차이**: `sched.Start`를 전 인스턴스에서 호출 가능. 리더 선출 + 결정적 TaskID가
중복을 이중으로 막으므로 "스케줄러는 하나만"이라는 각주가 불필요.

### 서버 옵션

```go
chronos.ServerConfig{
    OnDeadLetter: func(ctx, task *chronos.TaskInfo, err error) { alert(task.Kind, err) },
    Middleware:   []chronos.MiddlewareFunc{logging, metrics},
    ErrorHandler: func(ctx, task *chronos.TaskInfo, err error) { ... },
}
```

### Inspector (CLI 기반)

```go
insp := chronos.NewInspector(redisClient)
insp.Queues(ctx)
insp.ListArchived(ctx, "default", page)
insp.RetryTask(ctx, "default", taskID)
insp.DeleteTask(ctx, "default", taskID)
```

### 설계 노트

- **직렬화 JSON 기본, `Marshaler` 인터페이스로 교체 가능**(asynq는 protobuf 강제였음; redis-cli 디버깅 편의).
- **context.Context 1급** — 핸들러 첫 인자, 셧다운 시 취소 전파, `WithTimeout` 태스크별 타임아웃.
- 함수형 옵션 이중 레이어: enqueue 시점 옵션 + Args 타입 기본 옵션(river식 `DefaultOpts()` 선택 구현).

---

## 5. 에러 처리 · 신뢰성 시맨틱

### 전달 보장: at-least-once

태스크는 **최소 1회** 실행. "완료했으나 XACK 전 crash" 시 recoverer가 재실행하므로 **중복 실행 가능**
→ 핸들러 멱등성 권장을 문서 첫 페이지에 명시. 스케줄 잡 트리거는 결정적 TaskID로 **틱당 최대 1회
enqueue** 보장(실행은 여전히 at-least-once).

### 에러 분류와 재시도

| 반환 | 동작 |
|---|---|
| `nil` | XACK → completed(retention 후 정리) 또는 즉시 삭제 |
| 일반 `error` | XACK → retry ZSET, `attempt < MaxRetry`면 백오프 후 재실행 |
| `chronos.SkipRetry(err)` | 재시도 없이 즉시 dead-letter |
| panic | recover 후 일반 error 처리 + 스택 로깅 |

- **백오프**: 지수 + full jitter. `delay = min(base * 2^attempt, maxDelay) * rand(0,1)`,
  기본 base 5s / maxDelay 15m. `RetryDelayFunc`로 교체 가능.
- **재시도 소진**: archived 보관(기본) + `OnDeadLetter` 훅. `WithDeadLetterDiscard`는 훅만 발화 후 폐기.
- **워커 crash 재시도**: PEL `delivery count` + 핸들러 에러를 합산해 `attempt` 계산 → "active 영구
  고착"(asynq #420) 차단. crash 루프(poison pill)는 무한 재실행되지 않고 dead-letter로 수렴.

### Unique 락 시맨틱 (asynq 개선)

asynq 결함: 락 TTL과 처리 시간 독립 → 처리 중 만료 → 중복. chronos-go는 unique 락 값에 TaskID를
저장하고 **태스크가 최종 상태(completed/archived) 도달 시 상태 전이 Lua 안에서 원자 해제**. TTL은
"고아 락 안전망"으로만 동작. uniqueness가 대기+처리 중 전 구간 커버.

### Misfire 정책 (잡별 옵션)

```go
chronos.WithMisfirePolicy(chronos.MisfireSkip)     // 기본: 놓친 틱 버리고 다음 예정부터
chronos.WithMisfirePolicy(chronos.MisfireFireOnce) // 놓친 게 있으면 1회만 즉시 만회
```

- 리더가 잡별 `last_fired_at`을 Redis에 기록. 새 리더 취임 시 `now - last_fired_at > interval`이면
  misfire 판정 → 정책 적용.
- 기본 `MisfireSkip` 이유: 주력 사용처(1~3초 고빈도)는 "다음 주기에 하면 됨" 철학. 만회가 오히려 유해.
- `MisfireRunAll`(놓친 틱 전부 재실행)은 폭주 위험 + 실수요 없어 **미제공**(YAGNI).

### 리더 페일오버 시나리오

```
1. 리더 A가 chronos:leader 락(TTL 5s)을 2s 간격 갱신하며 스케줄 틱 실행
2. A crash → 갱신 중단 → 최대 5s 후 락 만료
3. 팔로워 B/C가 SET NX 경쟁 → B가 새 리더
4. B가 last_fired_at 로드 → misfire 판정 → 정책 처리 → 정상 틱 재개
   (graceful shutdown이면: A가 락 DEL + pub/sub 통지 → 공백 밀리초 수준)
5. 공백 중 A가 이미 enqueue한 태스크는 무영향 (큐/워커는 리더와 독립)
```

**좀비 리더 방어**: GC 정지 등으로 A가 늦게 enqueue해도 결정적 TaskID dedup이 B의 enqueue와
충돌시켜 중복 차단(fencing 역할). 결정 #3(리더+결정적 ID 이중화)의 실효.

### 그레이스풀 셧다운

```
Shutdown(ctx) → ① Stream 소비 중단 → ② in-flight 핸들러 완료 대기
             → ③ ctx 데드라인 도달 시 핸들러 ctx 취소 → ④ 미완료분 XACK 없이 종료
             → PEL에 남아 다른 인스턴스 recoverer가 회수 (유실 없음)
             → 리더였다면 락 DEL + 사임 통지
```

---

## 6. 테스트 전략

| 계층 | 도구 | 검증 대상 |
|---|---|---|
| 단위 | `miniredis` | 키 네이밍, 상태 전이, 옵션 파싱, cron 파싱, 백오프 계산 |
| 통합 | 실제 Redis (`testcontainers-go`) | Lua 원자성, Streams Consumer Group, XAUTOCLAIM, ZSET forwarder |
| 시나리오 | 실제 Redis + 다중 goroutine/프로세스 | 리더 페일오버, 크래시 복구, dedup 경합, 좀비 리더 |

**반드시 커버할 시나리오** (asynq 결함 회귀 방지):

1. **크래시 복구** — 워커가 XACK 전 죽으면 recoverer가 회수 재실행. 유실 0.
2. **Poison pill 수렴** — 항상 panic하는 핸들러가 정확히 `MaxRetry`회 후 dead-letter (asynq #420 방지).
3. **리더 페일오버** — 리더 죽였을 때 팔로워 승계 + 최대 TTL 내 틱 재개 + **중복 enqueue 0**.
4. **좀비 리더 dedup** — 두 인스턴스가 동시에 리더라 믿어도 결정적 TaskID가 중복 차단.
5. **Unique 락 생애** — 처리 시간 > 락 TTL인 잡도 완료까지 uniqueness 유지.
6. **Misfire 정책** — `MisfireSkip`은 만회 안 함, `MisfireFireOnce`는 정확히 1회.

- 모든 테스트 `-race`. goroutine 누수는 `go.uber.org/goleak`로 감시.
- CI: GitHub Actions + Redis 서비스 컨테이너 + `-race` + 커버리지. Redis 버전 매트릭스(6.2 / 7.x).

---

## 7. v1 마일스톤

의존성 순서대로 5단계. 각 단계 독립 테스트 가능하도록 분리.

**M1 — 코어 큐 (즉시 실행 경로)**
`internal/base` 키/상태 → `internal/rdb` enqueue/dequeue Lua → Streams processor → 제네릭
`Client.Enqueue`/`AddHandler`/`Server`. **성공 기준**: 태스크를 넣으면 워커가 타입 안전 처리.

**M2 — 신뢰성 (재시도 + 크래시 복구)**
retry ZSET + 지수 백오프 + forwarder → recoverer(XAUTOCLAIM) → dead-letter + `OnDeadLetter`.
**성공 기준**: 시나리오 1·2 통과.

**M3 — 지연 실행 + Unique**
scheduled ZSET + `WithProcessIn` → unique 락(생애 개선판) + `WithUnique`. **성공 기준**: 시나리오 5 통과.

**M4 — 분산 스케줄러 (차별점)**
리더 선출(`SET NX PX` + 갱신 + pub/sub 사임) → scheduleTicker(interval/cron, 1초 하한) →
결정적 TaskID → misfire 정책. **성공 기준**: 시나리오 3·4·6 통과. **operator-review 교체 가능 시점.**

**M5 — 운영성**
Inspector API → CLI(`cmd/chronos`) → Prometheus 메트릭 + `slog` → 그레이스풀 셧다운 마무리.
**성공 기준**: `chronos queue ls` 상태 조회, operator-review 메트릭 재현.

### v1 제외 (명시적 YAGNI)

Web UI, 우선순위 큐 strict 모드, task aggregation/그룹, 워크플로(chain/group/chord),
다중 백엔드 추상화, 서브초 스케줄링.

### operator-review 마이그레이션 경로

M4 완료 시점에 검증: operator-review의 `JobScheduler`를 chronos-go로 재구현하고, 대표 잡 3종
(고빈도 interval, cron, Enqueue+핸들러)을 교체해 동작 동일성 확인. 이것이 v1 최종 성공 기준.

---

## 8. 미해결 / 향후 검토

- 직렬화 코덱 기본값 확정(JSON) 외에 msgpack 옵션 제공 여부
- 우선순위 큐: v1은 큐별 가중치(weighted)만, strict 우선순위는 향후
- Web UI: 별도 모듈(`chronos-ui`)로 Inspector API 위에 구축 검토
- 메트릭 인터페이스: Prometheus 직접 의존 vs OpenTelemetry 추상화(gue가 후자 채택)
