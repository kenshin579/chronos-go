# chronos-go 큐 일시정지/재개 + 스케줄 레지스트리 설계 (webui Phase 2)

- 상태: 승인됨 (2026-07-12)
- 관련: webui v2 스펙(2026-07-12-webui-v2-design.md — Phase 2로 명시 연기),
  `server.go`(fetchLoop), `scheduler.go`(리더십 heartbeat 루프),
  `inspector.go`(SchedulerStatus), `internal/base/keys.go`
- 범위: 이 두 기능만. pause의 forwarder 중단(b안)·레지스트리 정리 API는 범위 외.

## 확정 결정 (브레인스토밍)

1. **pause = 소비만 중단** (asynq와 동일 의미론): 워커 dequeue만 멈춤.
   enqueue·forwarder·scheduler·recoverer는 계속 → pending 축적이 대시보드에
   보임. in-flight는 완주. resume 시 쌓인 것부터 소비.
2. pause 상태 = **전역 SET `chronos:paused`** + 서버 측 **1초 캐시**
   (반영 지연 최대 ~1s, 문서 명시).
3. 레지스트리 = **전역 HASH `chronos:schedules`** + `last_seen` 기반 stale
   판정. graceful Shutdown에서도 삭제하지 않음(동일 스케줄의 타 인스턴스
   보호) — 잔존 엔트리는 stale 표시로 충분(정리 API는 후속).

## 설계

### A. 큐 일시정지/재개

**키/rdb** (`internal/base/keys.go`, `internal/rdb/`):
```go
// PausedKey is the SET key listing paused queue names. Global (no hash tag):
// only single-key commands touch it, so it is cluster-safe.
func PausedKey() string { return "chronos:paused" }
```
- `rdb.PauseQueue(ctx, q)` = SADD, `ResumeQueue` = SREM,
  `PausedQueues(ctx) ([]string)` = SMEMBERS.

**서버 (fetchLoop)**:
- server에 paused 캐시: `pausedSet map[string]bool` + `pausedAt time.Time` +
  mutex. fetch 라운드 시작 시 `time.Since(pausedAt) > pauseCheckInterval(1s)`면
  `PausedQueues` 재조회(실패 시 이전 캐시 유지 + 로그 — pause 실패가 소비를
  멈추면 안 됨).
- 라운드의 우선순위 순서(order)에서 paused 큐 제외. 전부 paused면
  `pollBlock` 대기 대신 1초 sleep 후 재확인(블로킹 대상 큐가 없으므로).
- WRR 상호작용: picker가 뽑은 큐가 paused면 그 라운드는 나머지 큐로 폴백
  (기존 "빈 큐 폴백"과 동일 경로) — 가중치 왜곡은 기존 폴백 의미론과 동일
  수준으로 허용.

**Inspector/CLI/UI**:
- `Inspector.PauseQueue/ResumeQueue/PausedQueues`. `QueueInfo.Paused bool` —
  `Queues()`가 SMEMBERS 1회로 일괄 매핑.
- CLI: `chronos queue pause <q>` / `chronos queue resume <q>`,
  `queue ls`에 PAUSED 컬럼.
- webui: 큐 카드 ⏸ 배지, 큐 상세 Pause/Resume 토글(POST
  `/queues/{q}/pause`·`/resume`, Origin 가드, confirm 불필요 — 가역),
  `/api/stats`의 statsQueue에 `paused bool`(app.js가 배지 토글).

### B. 스케줄 레지스트리

**키/직렬화**:
```go
// SchedulesKey is the HASH key holding the registry of known schedules
// (field = deterministic schedule ID, value = JSON metadata). Global single
// key, single-key commands only — cluster-safe.
func SchedulesKey() string { return "chronos:schedules" }
```
- value JSON: `{kind, queue, spec, registered_at, last_seen}` (unix 초).
  spec은 interval이면 `"@every 30s"`, cron이면 원문 5필드.

**스케줄러 (scheduler.go)**:
- `Start` 시 자기 스케줄 전부 HSET(멱등 — scheduleID가 결정적이라 다중
  인스턴스 덮어쓰기 무해). `registered_at`은 최초 기록 보존이 이상적이나
  덮어써도 실해 없음(단순화: 항상 now — 문서에 "이 인스턴스가 마지막으로
  등록한 시각" 의미로 명시).
- **기존 리더십 heartbeat 루프에서** 주기적으로 자기 스케줄들의 `last_seen`
  갱신(HSET 재기록 — 새 고루틴 없음). 리더/팔로워 무관(등록은 인스턴스의
  사실이므로).
- Shutdown에서 HDEL 하지 않음.

**Inspector**:
- `ScheduleInfo` 확장: `{ID, Kind, Queue, Spec string; LastFired, LastSeen
  time.Time; Stale bool}`.
- `SchedulerStatus`: 레지스트리(HGETALL) + 기존 발화 이력(`chronos:sched:*:last`
  SCAN)을 **ID로 병합** — 등록됐지만 미발화 스케줄도 노출(LastFired zero),
  레지스트리에 없는 발화 이력(구버전 잔재)도 유지. `Stale = last_seen이
  staleAfter(60s)보다 오래됨`(레지스트리 출신 엔트리만).
- webui 스케줄러 페이지: Kind/Queue/Spec/LastFired/LastSeen 컬럼, stale 행
  회색(`.stale` 클래스) + "stale" 배지.

## Cluster / 호환성

- `chronos:paused`·`chronos:schedules` 모두 전역 단일 키 + 단일 키 명령 —
  cluster-safe, 신규 Lua 없음. cluster 스모크 1개 추가(pause→소비 정지→
  resume→소비 재개, 17번째).
- 하위호환: pause SET이 없으면(구버전 서버) 아무 일 없음. 레지스트리가
  비어도 SchedulerStatus는 기존 발화 이력만으로 동작(v2와 동일).

## 테스트 (TDD)

1. rdb: Pause/Resume/PausedQueues 왕복.
2. 서버 통합: pause → 소비 정지(pending 축적, 2초 대기 확인) → resume →
   소비 재개. 캐시로 인한 ~1s 지연 허용 검증. 두 큐 중 하나만 pause 시
   나머지는 계속 소비.
3. Inspector: QueueInfo.Paused, PausedQueues.
4. 스케줄러: Start 후 레지스트리 존재(kind/queue/spec 정확), heartbeat 후
   last_seen 갱신, SchedulerStatus 병합(등록+미발화 / 등록+발화 / 이력만),
   stale 판정.
5. CLI: pause/resume 명령 + queue ls PAUSED 컬럼.
6. webui: 토글 POST(Origin 가드 포함) → 배지 변화, /api/stats paused,
   스케줄러 페이지 컬럼.
7. cluster 스모크 17번째(pause 동작).

## 관찰 습관 / 문서

- tour 섹션 14: pause → pending 축적 눈 확인 → resume → 소비 재개.
- README: Queue pause 문단(반영 지연 ~1s 명시), 스케줄러 상태 서술 갱신
  (등록 목록 노출), Known limitations에서 Phase 2 항목 제거.
- webui README 기능 목록 갱신.

## 알려진 한계 / 후속

- pause 반영 지연 최대 ~1s(서버 캐시). 즉시성이 필요하면 후속에서 pub/sub.
- 레지스트리 잔존 엔트리(구성 변경으로 제거된 스케줄)는 stale 표시만 —
  정리 API는 후속.
- registered_at은 "마지막 등록 시각" 의미(다중 인스턴스 덮어쓰기).
