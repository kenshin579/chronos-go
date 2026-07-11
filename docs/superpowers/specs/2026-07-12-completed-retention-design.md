# chronos-go Completed Retention 설계

- 상태: 승인됨 (2026-07-12)
- 관련: `internal/rdb/rdb.go`(Done), `internal/rdb/janitor.go`(TrimArchived 패턴),
  `inspector.go`(zsetKeyForState), asynq의 `Retention` 옵션(패리티)

## 배경 / 목적

성공한 태스크는 `Done`이 즉시 모든 흔적을 삭제한다(XACK+XDEL+태스크 hash DEL).
실패는 archived로 남아 진단 가능(LastErr/GetTask)하지만 성공은 아무것도 확인할 수
없는 비대칭이 있다 — "어젯밤 그 태스크 돌았어?"에 답하지 못한다.

**Completed retention**: 성공한 태스크를 태스크별 지정 기간 동안 `completed` ZSET에
보관해 Inspector/CLI로 조회 가능하게 한다. asynq 패리티 항목 중 남은 것 하나.

## 확정 결정 (브레인스토밍)

1. **API**: 태스크별 옵션 `WithRetention(d)`만 (asynq 패리티). 서버 기본값 없음 —
   태스크 단위 opt-in이라 메모리 폭증 위험이 없고, 서버 기본값은 후속 하위호환 추가 가능.
2. **기본값 retention 0 = 즉시 삭제** — 기존 동작·성능 완전 무변화.
3. **completed 태스크 액션: run + delete 둘 다 허용** — 기존 `runTaskCmd`가 ZSET
   불문 동작하는 구조라 공짜이고, dead-letter는 재실행되는데 성공은 안 되는 비일관 방지.
   재실행은 at-least-once 의미론(핸들러 멱등 전제)과 충돌 없음.
4. **ZSET score = 만료 시각**(완료시각+retention) — janitor가 태스크별 retention을
   조회할 필요 없이 `score <= now`만 정리. 완료 시각 자체는 msg의 `CompletedAt`으로 보존.
5. **크기 상한 `ServerConfig.MaxCompleted`**: 기본 10000, 음수로 비활성(MaxArchived와
   동일 규칙). 정리 주기는 기존 `JanitorInterval` 재사용(신규 주기 설정 없음).

## 설계

### A. 공개 API (chronos.go)

```go
// WithRetention keeps a successfully completed task around for d, so it can be
// inspected (state "completed") before the janitor removes it. Default (no
// option) deletes the task immediately on success, as before.
func WithRetention(d time.Duration) Option
```
- 옵션 검증: `d <= 0`이면 0으로 취급(즉시 삭제) — 에러 아님(관대 처리).

### B. 데이터 모델 (internal/base)

- `TaskMessage`에 추가(둘 다 `omitempty` — 기존 저장분 디코딩 무영향):
  - `Retention int64` (초) — enqueue 시 기록, Done에서 사용.
  - `CompletedAt int64` (unix 초) — 완료 시점에 기록, 조회 표시용.
- `keys.go`에 `CompletedKey(qname) = QueueKeyPrefix(qname) + "completed"` 추가
  (해시태그 안 — cluster-safe).
- `StateCompleted`는 이미 존재(String() "completed").

### C. Done 경로 (internal/rdb)

- `msg.Retention == 0`: 기존 `Done` 그대로 (TxPipeline: XACK+XDEL+DEL, unique 해제).
- `msg.Retention > 0`: **XACK+XDEL 후 hash를 지우는 대신** msg에
  `CompletedAt=now`·`State=StateCompleted`를 기록해 재인코딩하고 completed ZSET에
  `score = now + Retention`으로 등록. 기존 `moveToZSetCmd`와 같은 원자 Lua 패턴
  (신규 스크립트 또는 score 의미만 다른 재사용 — 구현 시 판단).
- **unique 락은 두 경로 모두 지금처럼 즉시 해제** — 보관 중인 completed 태스크가
  동일 태스크의 새 enqueue를 막으면 안 됨.

### D. janitor (internal/rdb + server.go)

- `TrimCompleted(ctx, qname, maxSize, batch)`: TrimArchived와 동일 구조 —
  (1) `score <= now`(만료) 배치 삭제(ZSET 엔트리 + 태스크 hash),
  (2) `maxSize` 초과분을 오래된 것부터 배치 삭제. 음수 maxSize = 크기 상한 비활성
  가드 포함(TrimArchived의 delete-all 방지 가드와 동일).
- `janitorLoop`이 큐마다 TrimArchived에 이어 TrimCompleted 호출.
- `ServerConfig.MaxCompleted int` 추가: 0이면 기본 10000, 음수면 비활성.

### E. Inspector / CLI

- `zsetKeyForState`에 `"completed"` 추가 → ListTasks/GetTask/RunTask/DeleteTask가
  자동으로 completed 지원. 에러 메시지의 상태 나열도 갱신
  (`want scheduled|retry|archived|completed`).
- `QueueInfo.Completed int64` + `rdb.QueueStats`에 completed ZCARD 추가.
- `TaskInfo.CompletedAt time.Time` 필드 추가(완료 시각, zero면 미완료).
  completed 태스크의 `NextProcessAt`은 ZSET score(=만료 시각) — doc comment에 명시.
- CLI: `queue ls`에 COMPLETED 컬럼, `task ls <q> completed` 동작(상태 나열 도움말 갱신).

### F. 관찰 습관 + 문서

- `examples/tour` 섹션 11: `WithRetention(3*time.Second)`으로 enqueue → 처리 →
  Inspector로 completed 조회(완료시각 확인) → janitor 정리 후 0 확인.
- README: Enqueue options에 `WithRetention` 추가, vs asynq 표의 패리티 갱신,
  **메모리 경고**(처리량 높은 큐 + 긴 retention 조합 주의) 명시.

## 테스트 (TDD)

**단위/통합 (실제 Redis, DB 15, -p 1):**
1. `WithRetention` 성공 → completed ZSET 안착: `GetTask` state="completed",
   `CompletedAt` 채워짐, `NextProcessAt`=만료시각. 스트림은 비워짐(XACK+XDEL).
2. 미지정(기본) → 즉시 삭제(GetTask not found). 기존 Done 테스트가 회귀 보호.
3. janitor: 만료분 정리(짧은 retention 후 0), MaxCompleted 상한 정리(초과분만 삭제).
4. unique: `WithUnique`+`WithRetention` 태스크 완료 직후 동일 태스크 enqueue 성공
   (락 해제 확인) — completed 보관이 dedup에 영향 없음.
5. Inspector: completed에서 RunTask(재실행되어 핸들러 2회) / DeleteTask(사라짐) /
   ListTasks 필드 / QueueInfo.Completed 카운트.
6. **cluster 스모크 확장**: 새 Lua(completed 이동)와 TrimCompleted가 생기므로
   `cluster_test.go`에 completed 경로 테스트 1개 추가 + 체크리스트 갱신
   (스크립트-완전 원칙 유지).

## 검증 / 마무리

- `make check` 무회귀 + `make test-cluster`(13개) 그린.
- `go run ./examples/tour` 섹션 11 눈 확인.
- k:code-reviewer 리뷰 → PR(assignee kenshin579) → 머지.

## 알려진 한계 / 후속

- retention은 enqueue 시점에 고정(태스크와 함께 이동) — 사후 변경 불가.
- 서버 기본값(`CompletedRetention`)은 후속 하위호환 추가 가능(현재 YAGNI).
- 처리량 높은 큐 + 긴 retention = completed ZSET 메모리 증가. MaxCompleted가
  상한이지만, 상한 도달 시 오래된 것부터 조기 삭제됨(README 경고).
