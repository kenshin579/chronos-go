# chronos-go 그룹 멤버 체인 (1레벨 재귀 중첩) 설계

- 상태: 승인됨 (2026-07-14)
- 관련: `group.go`(Add/OnComplete/Enqueue), `chain.go`(ThenGroup·chainStage),
  `server.go`(process 성공 경로·enqueueNext), `internal/rdb/group.go`
  (CompleteGroupMember/CreateGroup), `internal/base/task.go`(TaskMessage)
- 관련 스펙: `docs/superpowers/specs/2026-07-13-workflow-results-design.md`
  (결과 전달·ThenGroup — 이번 기능이 그 위에 얹힘)
- 범위: **1레벨만** — 그룹 멤버가 체인일 수 있다. 그 이상(멤버 체인 안의
  ThenGroup, 그룹 안의 그룹)은 명시적으로 범위 외이며 Enqueue 시점에 거부.

## 확정 결정 (브레인스토밍)

1. **중첩 범위 = 그룹 멤버 = 체인 (1레벨)** (a안): 동기 usecase(마이그레이션
   N개 각각 "덤프→변환→적재" 체인을 병렬로, 전부 끝나면 검증 콜백)를 정확히
   커버. 그룹-of-그룹·완전 재귀는 usecase 빈도 대비 복잡도가 과함(특히 완전
   재귀는 지수 팽창 → 근본적으로 다른 lazy materialization 요구).
2. **API = `Group.AddChain(chain)`** (a안): 기존 `Add`(단일)와 대칭. ThenGroup이
   세운 "빌더를 빌더에 넘기는" 패턴과 일관. 체인 멤버는 자기 옵션을 체인 빌더
   안에서 관리(별도 opts 없음).
3. **1레벨 경계를 코드로 강제** (a안): `AddChain`의 체인이 `ThenGroup` 스테이지를
   포함하면 Enqueue 시점 거부 — 미지원 조합의 조용한 오작동 방지.

## 설계

### 완료 보고 메커니즘 (핵심)

"그룹 멤버 = 체인"은 **체인의 마지막 링크가 부모 그룹에 보고**하면 성립한다.
기존 flat 멤버가 "성공 시 부모 SET에 SREM"하는 것과 동일하고, 체인 멤버는 그
보고 지점이 마지막 링크로 미뤄질 뿐이다.

- **그룹 보고 필드가 멤버 체인을 타고 흐른다**: 멤버 체인의 첫 링크 메시지에
  `GroupID/GroupQueue/GroupCallback/GroupIndex/GroupMemberID`를 싣고,
  `enqueueNext`가 후속 링크로 이 필드들을 전파한다.
- **보고 게이트**: `server.go`의 성공 경로에서 그룹 보고는 **"이 링크가 후속을
  갖지 않을 때(= 마지막 링크)"에만** 발화한다. 현재 코드는

  ```go
  if len(msg.Chain) > 0 { enqueueNext(msg); msg.Chain = nil }
  if msg.GroupID != "" { completeGroupWithRetry(msg) }
  ```

  인데, 중간 링크(Chain 비어있지 않음)가 후속 enqueue 후 `msg.Chain=nil`이 되어
  두 번째 조건에 걸려 **조기 SREM**된다. 따라서 다음으로 바꾼다:

  ```go
  hadSuccessor := len(msg.Chain) > 0
  if hadSuccessor { enqueueNext(msg); msg.Chain = nil }
  if msg.GroupID != "" && !hadSuccessor { completeGroupWithRetry(msg) }
  ```

  - flat 멤버: `hadSuccessor=false`, GroupID 설정 → 보고. (기존과 동일)
  - 체인 멤버 중간 링크: `hadSuccessor=true` → enqueueNext만, 보고 안 함.
  - 체인 멤버 마지막 링크: `hadSuccessor=false`, GroupID 설정(전파돼 옴) → 보고.
- **`enqueueNext` 전파**: 후속 메시지 구성 시 `msg`의 그룹 보고 필드(GroupID/
  GroupQueue/GroupCallback/GroupIndex/GroupMemberID)를 후속에 복사한다. (기존
  PrevResult 전파 옆에 추가.)
- **`TaskMessage.GroupMemberID` 신설**: pending SET 엔트리(`<groupID>:m<j>`)와
  태스크 자신의 ID를 분리한다. 마지막 링크의 자기 ID는 `<groupID>:m<j>:<i>`지만
  SREM 대상은 멤버 슬롯 `<groupID>:m<j>`. `CompleteGroupMember`가
  `msg.GroupMemberID`(비면 `msg.ID`로 폴백 → flat 멤버 하위호환)로 SREM.
- **멤버 결과**: 마지막 링크가 자기 `Result`로 보고 → 부모 콜백의
  `GroupResults[j]` = 그 체인의 최종 산출물. 멤버 체인 내부 스텝 간 전달은 기존
  `PrevResult` 릴레이 그대로.

**새 Lua 없음, 새 cross-slot 연산 없음.** 보고는 여전히 부모 콜백 큐 슬롯 안의
단일 Lua(`groupCompleteCmd`)이고, 멤버 체인 링크는 자기 슬롯에서 진행한다.
기존 ThenGroup(그룹=체인 스테이지) 경로는 "멤버가 단일 태스크"라 GroupID+Chain을
동시에 가진 메시지가 없었으므로 이 전파·게이트 변경의 영향을 받지 않는다.

### ID 체계

- 멤버 체인 chainID = `<parentGroupID>:m<j>` (부모 그룹의 j번째 멤버 슬롯)
- 그 체인 링크들 = `<parentGroupID>:m<j>:0`, `:1`, … (기존 `<chainID>:<i>` 스킴)
- pending SET 엔트리(= `GroupMemberID`) = `<parentGroupID>:m<j>`
- flat 멤버 ID(`<g>:m<j>`)와 체인 멤버 첫 링크 ID(`<g>:m<j>:0`)는 달라 충돌
  없음. 둘 다 SET 엔트리는 `<g>:m<j>`로 통일.

### 메시지 크기 / cluster / 실패

- **메시지 크기**: 체인 멤버는 자기 선형 꼬리(체인 길이만큼)만 내장. 부모 그룹
  Enqueue = pending SET(N) + 멤버별 첫 링크 메시지 N개(각자 꼬리 + 보고 번들).
  총량은 폭 × 길이의 합. 지수 팽창은 "멤버 체인 안 ThenGroup 금지"로 차단.
- **cluster**: 부모 pending SET·콜백 = 부모 콜백 큐 슬롯(불변). 멤버 체인 각
  링크 = 자기 큐 슬롯(chainEnqueueCmd, 기존). 마지막 링크 보고 =
  CompleteGroupMember(부모 슬롯 단일 Lua, 별도 연산) → 링크가 다른 슬롯이어도
  CROSSSLOT 없음.
- **실패·재개**: 멤버 체인 중간 링크 dead-letter → 마지막 링크 미도달 → SREM
  안 함 → 부모 그룹 정지(GroupTTL 안전망). 그 링크 RunTask 재개 → 체인 완주 →
  부모 보고. 기존 체인 재개 + 그룹 stall 합성.
- **CreateGroup 불변식**: pending SET에 전 멤버 슬롯 ID를 먼저 추가한 뒤 각
  멤버(체인이면 첫 링크) enqueue — 부분 실패가 콜백 조기 발화 못 함(기존).

### 빌더 API

```go
g := chronos.NewGroup()
for _, t := range tenants {
	g.AddChain(chronos.NewChain().
		Then(Dump{t}, chronos.WithQueue("dump")).
		Then(Transform{t}, chronos.WithQueue("cpu")).
		Then(Load{t}, chronos.WithQueue("db")))
}
g.OnComplete(Verify{}, chronos.WithQueue("ctrl")).Enqueue(ctx, client)
```

- `func (g *Group) AddChain(ch *Chain) *Group` 신설 — 기존 `Add`(단일)와 대칭·병존.
- 내부: `Group`의 멤버 목록이 "단일 태스크 | 체인"을 판별하도록 확장
  (ThenGroup의 `chainStage.isGroup`와 같은 패턴 — 예: `groupMember{args; opts;
  chain *Chain; isChain bool}`).

### 검증 (Enqueue 시점)

- `AddChain`의 체인이 `ThenGroup` 스테이지를 하나라도 포함하면 거부:
  `chronos: a group member chain cannot contain a parallel stage (ThenGroup) — recursive nesting beyond one level is not supported`
- 멤버 체인 링크는 기존 체인 규칙 상속: WithTaskID/WithUnique 거부
  (`resolveChainOptions` 재사용).
- **멤버 체인 전체 noArchive(discard) 거부**: 어느 링크든 discard면 그 멤버가
  좌초 시 재실행 불가 → 그룹 영구 정지. (flat 멤버 discard 거부와 동일 취지.)
- nil 체인 거부, 빈 체인 멤버 거부.

## 표면 / 관찰

- `TaskInfo`: 체인 멤버의 대기 링크는 기존 `ChainNext`(대기 링크 kind)로 이미
  노출 — 추가 변경 불필요. 멤버 정지 시 dead-letter 링크가 Inspector/CLI에
  그대로 보임.
- tour 섹션 16: 마이그레이션 축소판(N=3 테넌트, 각 "덤프→변환→적재" 체인 병렬 →
  검증 콜백).
- 소크: 워크로드에 그룹-of-체인 경로 추가.
- README "Parallel stages" 절에 "그룹 멤버는 체인일 수 있다(`AddChain`)" 소절,
  Known limitations의 "재귀 중첩 미지원"을 "2레벨(멤버 체인 안의 병렬 스테이지,
  그룹 안의 그룹) 미지원 — 1레벨(그룹 멤버 체인)은 지원"으로 정밀화.

## 테스트 (TDD)

1. base: `GroupMemberID` 왕복.
2. server/통합: 그룹-of-체인 정상 완주 — 멤버별 최종 결과가 `GroupResults`
   순서대로, 마지막 링크만 보고(중간 링크 SREM 안 함).
3. 통합: 멤버 체인 중간 링크 dead-letter → RunTask 재개 → 완주·콜백 발화.
4. 빌더 검증: ThenGroup 포함 체인 멤버 거부, WithUnique/discard 거부, nil/빈 거부.
5. 혼용: flat 멤버 + 체인 멤버 한 그룹.
6. 멱등: 멤버 체인 첫 링크 create-if-absent(선행 없음 — 그룹이 최상위라 재전달
   트리거 없음; 단 recoverer 재전달 시 링크 재실행 멱등 확인).
7. cluster 스모크 20번째: 멤버 체인 링크가 서로 다른 슬롯(다른 큐).

## Cluster / 호환성

- 새 다중키 Lua 없음. `GroupMemberID`는 신규 omitempty 필드 — 구버전 메시지는
  빈 값 → `CompleteGroupMember`가 `msg.ID` 폴백(하위호환).
- cjson 왕복(groupCompleteCmd가 콜백 메시지 decode/encode)에 새 슬라이스 필드를
  추가하지 않으므로 빈 배열 함정 무관. `GroupMemberID`는 문자열.

## 알려진 한계 / 후속

- 2레벨 이상(멤버 체인 안의 ThenGroup, 그룹 안의 그룹)은 미지원 — 지수 팽창을
  피하려면 lazy materialization이 필요(후속 브레인스토밍 주제).
- 멤버 체인의 재전달 창은 그 체인이 최상위 그룹의 멤버일 때 선행이 없으므로
  ThenGroup-스테이지 그룹의 콜백-hash 펜스 이슈와 무관. 단 이 그룹 자체가
  ThenGroup 스테이지로도 쓰이면(그룹=체인 스테이지 + 멤버=체인) 두 기능이
  합성된다 — 테스트로 확인(2번에 스테이지-그룹 변형 1건 포함 권장).
