# chronos-go Task Groups (병렬 fan-out + 완료 콜백) 설계

- 상태: 승인됨 (2026-07-12)
- 관련: Chain 스펙(2026-07-12-task-chain-design.md — 결정적 ID, create-if-absent,
  메시지 내장, "후속→Done" 순서 패턴 재사용), `internal/rdb/chain.go`,
  `server.go`(process 성공 경로), `internal/base/task.go`(ChainLink 재사용)
- 범위: Groups만. 그룹×체인 조합(그룹을 체인 링크로, 콜백에 체인 달기, 중첩
  그룹)은 명시적 범위 제외.

## 배경 / 목적

N개 태스크를 병렬 실행하고 **전부 성공하면** 콜백 태스크를 실행한다("파일 3개
병렬 처리 → 집계 리포트"). 사용자가 직접 만들기 가장 어려운 부분 — N개 완료를
at-least-once 세계에서 원자적·멱등하게 추적 — 을 라이브러리가 담당한다.
Chain(순차)과 합쳐 워크플로 기본기를 완성한다.

## 확정 결정 (브레인스토밍)

1. **부분 실패 = 그룹 대기 + 재실행 시 재개** (Chain과 동일 의미론):
   멤버가 dead-letter되면 그룹은 열린 채 대기. `RunTask` 재실행 → 성공 →
   마지막이었다면 콜백 발화. fail-fast/부분성공 콜백은 채택하지 않음.
2. **그룹 상태 = pending 멤버 ID SET** (카운터 아님) — SREM은 멱등이라
   재전달 중복 감소 문제가 원천 차단됨.
3. **SET은 콜백 큐의 해시태그 슬롯에 배치** — "SREM + 비었으면 콜백 생성"이
   단일 원자 Lua로 가능(Cluster-safe). 멤버 큐는 자유.
4. **SET TTL 기본 7일** — 방치된 그룹(멤버 삭제 등)의 안전망. 만료 시 콜백
   미발화, 상태만 소멸. v1에서는 TTL 조절 옵션 없음(고정 상수, 후속 후보).
5. 콜백은 **필수**(OnComplete 없으면 에러).

## 설계

### A. 공개 API (groups.go 신규, Chain과 대칭)

```go
info, err := chronos.NewGroup().
    Add(ResizeArgs{File: "a.jpg"}).
    Add(ResizeArgs{File: "b.jpg"}, chronos.WithQueue("low")).
    OnComplete(ReportArgs{Batch: "b1"}, chronos.WithRetention(time.Hour)).
    Enqueue(ctx, client)   // 멤버 전부 enqueue
```

반환값은 그룹 전용 타입으로 확정:

```go
// GroupInfo describes an enqueued group.
type GroupInfo struct {
	GroupID    string   // the group's identity
	MemberIDs  []string // deterministic member task IDs ("<groupID>:m<i>")
	CallbackID string   // the callback's task ID ("<groupID>:cb")
}
```

- `Add(args TaskArgs, opts ...Option)` — 멤버(병렬), 멤버별 옵션
  (큐/재시도/retention/지연 허용).
- `OnComplete(args TaskArgs, opts ...Option)` — 콜백. 1회만 호출 가능(재호출 에러).
- 검증(Enqueue 시 일괄): 멤버 0개 에러, OnComplete 누락 에러, 멤버·콜백의
  `WithTaskID`/`WithUnique` 거부, 콜백의 `WithProcessAt` 거부(`WithProcessIn`은
  "그룹 완료 후 상대 지연"으로 허용 — Chain 꼬리와 동일 스냅샷 규칙),
  중간·콜백의 NoArchive는 Chain 규칙 준용(멤버는 허용 — 멤버 discard 실패 시
  그룹은 TTL까지 대기 후 소멸함을 문서 명시; 콜백 NoArchive 허용).
- 결정적 ID: `groupID`(UUID), 멤버 `<groupID>:m<i>`, 콜백 `<groupID>:cb`.

### B. 데이터 모델

- `base.TaskMessage`에 추가(모두 omitempty):
  - `GroupID string` — 소속 그룹.
  - `GroupQueue string` — 그룹 SET이 사는 큐(=콜백 큐). 멤버 완료 시 어느
    슬롯의 SET을 건드릴지 알기 위함.
  - `GroupCallback *ChainLink` — 콜백 스냅샷(**기존 ChainLink 타입 재사용** —
    kind/payload/queue/maxRetry/noArchive/retention/delay). 마지막 멤버가 이
    스냅샷으로 콜백을 생성.
- 그룹 SET 키: `base.GroupKey(cbQueue, groupID) = "chronos:{<cbQueue>}:group:<groupID>"`
  — 멤버 ID들의 SET, EXPIRE 7일(`base` 또는 rdb에 상수 `GroupTTL = 7 * 24h`).

### C. Enqueue 흐름 (클라이언트)

1. 그룹 SET 생성: SADD(전체 멤버 ID) + EXPIRE — 콜백 큐 슬롯의 단일 키라
   원자성 문제 없음(하나의 SADD + EXPIRE, 파이프라인 가능).
2. 멤버 N개를 순차 enqueue(기존 dispatch 경로 재사용, msg에
   GroupID/GroupQueue/GroupCallback 기록).
- **SET 먼저** 만들므로 멤버 enqueue가 중간에 실패해도 콜백 오발화 없음
  (빠진 멤버의 ID가 SET에 남아 SCARD가 0이 되지 않음 → TTL 정리).
  다중 멤버 enqueue의 비원자성(일부만 실행될 수 있음)은 문서 명시.

### D. 멤버 완료 흐름 (서버, internal/rdb)

`process` 성공 경로에서 Chain과 동일한 순서("그룹 보고 → Done") + 백오프
재시도(`enqueueNextWithRetry`와 같은 패턴, 헬퍼 공유 리팩터 허용):

신규 `groupCompleteCmd` Lua (전 키가 콜백 큐 해시태그 슬롯):

```
KEYS[1] group set, KEYS[2] callback task hash, KEYS[3] callback stream(또는 scheduled zset)
ARGV: memberID, 콜백 encoded msg, state, callbackID, (score)

if EXISTS(group set) == 0 then return "noop" end   -- 이미 완료·만료된 그룹(재전달 안전)
SREM(set, memberID)
if SCARD(set) == 0 then
  if EXISTS(callback hash) == 0 then               -- create-if-absent(이중 발화 차단)
    HSET(callback hash, msg/state); XADD(stream) 또는 ZADD(scheduled)
  end
  DEL(set)
end
```

- rdb 공개 함수: `CompleteGroupMember(ctx, member *base.TaskMessage)
  (callbackFired bool, err error)` — msg의 GroupQueue/GroupCallback으로 키·콜백
  msg를 조립해 스크립트 실행. 지연 콜백(Delay>0)은 scheduled ZSET 버전.
- 콜백 msg: `ID = <groupID>:cb`, GroupCallback 스냅샷 필드 매핑(Chain의
  enqueueNext와 동일 요령). 콜백 msg에는 Group* 필드 없음(그룹 종료).
- 서버: 그룹 보고 실패 시 Done을 건너뛰고 반환(Chain과 동일 — PEL 재전달로
  재시도, 백오프 3회 선행). 성공 후 `msg.GroupID`/`GroupCallback`은 유지
  (재전달 시 no-op 경로가 알아서 처리 — Chain의 꼬리 제거와 달리 그룹 보고는
  SET 멱등이라 msg 정리 불필요. 단, retention 보관 시 GroupID가 남아 조회에
  보이는 것은 정보로서 유익).

### E. 실패·엣지 의미론

- 멤버 dead-letter: 그룹 SET에 그 멤버 ID 잔존 → 콜백 미발화(대기).
  `RunTask` 재실행 성공 → SREM → 마지막이면 콜백. **Chain과 같은
  "중단+재개" 규칙.**
- 멤버 `DeleteTask`: SET에 ID가 남아 그룹 영구 미완 → TTL(7일)로 상태 소멸,
  콜백 미발화. 문서 명시(그룹 포기의 사실상 수단).
- SET TTL 만료 후 뒤늦은 멤버 완료: EXISTS 가드로 no-op(콜백 오발화 없음).
- 콜백 자체의 실패: 일반 태스크와 동일(재시도→dead-letter, RunTask 재실행
  가능 — create-if-absent는 hash 존재로 no-op이므로 재발화 문제 없음).
- at-least-once 주의(Chain과 동일 계약): 콜백은 hash가 존재하는 동안 최대
  1회 생성. 콜백이 완료·미보관으로 hash가 사라진 뒤 멤버가 재전달되는 창은
  EXISTS(set) 가드(SET이 이미 DEL됨 → no-op)가 차단 — **Chain보다 창이 좁음**
  (SET DEL이 펜스 역할). 문서에 명시.

### F. 관찰 / 노출

- `TaskInfo.GroupID string` 추가. `GetTask`는 GroupID가 있으면 그룹 SET
  SCARD를 `GroupPending int`로 채움(남은 멤버 수; SET 부재 시 0).
- tour **섹션 13**: 3멤버 fan-out → 콜백 1회 발화 확인 → 실패 멤버 변형으로
  그룹 대기 확인(GroupPending 표시) → `RunTask` 재개 → 콜백 완주.
- cluster 스모크 **15번째**: 멤버를 다른 슬롯 큐 2개에 분산 + 콜백 제3 큐 —
  신규 Lua(그룹 SET 생성, groupCompleteCmd) 검증(스크립트-완전 원칙).
- README "Groups" 섹션: 예제, 실패=대기/재실행=재개, TTL 안전망, 비원자
  enqueue 주의, Chain과의 조합은 미지원(후속).

## 테스트 (TDD)

1. fan-out: 멤버 3개(서로 다른 큐 포함) 병렬 실행 → 전부 완료 시 콜백 정확히
   1회. 동시 완료 경합(Concurrency 크게, 멤버 다수)에서도 1회.
2. 중단+재개: 멤버 1개 dead-letter → 콜백 미발화 확인 → RunTask → 콜백 발화.
3. rdb 멱등: 같은 멤버 CompleteGroupMember 2회 → 두 번째 no-op·콜백 불변.
   SET 부재 시 no-op. 지연 콜백은 scheduled ZSET 안착.
4. 빌더 검증: 멤버 0개/OnComplete 누락/재호출/TaskID/Unique/콜백 ProcessAt 에러.
5. 노출: TaskInfo.GroupID, GetTask의 GroupPending(잔여 수), 콜백
   WithProcessIn 지연.
6. cluster 스모크 1개(15번째).

## 검증 / 마무리

- `make check` 무회귀 + `make test-cluster`(15개) + tour 섹션 13 눈 확인.
- k:code-reviewer 리뷰 → PR(assignee kenshin579) → 머지. v0.7.0 후보.

## 알려진 한계 / 후속

- 그룹×체인 조합·중첩 그룹·부분 성공 콜백·그룹 TTL 옵션·그룹 단위 Inspector
  조회(목록/취소 API)는 범위 외 — 후속 후보.
- 멤버 간/콜백으로의 결과 전달 없음(Chain과 동일 원칙).
- 다중 멤버 enqueue는 비원자(중간 실패 시 일부 멤버만 실행될 수 있음 —
  SET 선생성으로 콜백 오발화는 없음).
