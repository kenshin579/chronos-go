# chronos-go Web UI v2 (태스크 관리 콘솔 재개발) 설계

- 상태: 승인됨 (2026-07-12, Visual Companion으로 UX 선택 완료)
- 관련: v1 스펙(2026-07-11-web-ui-task-console-design.md — 원칙 계승),
  v1 구현(git 히스토리 PR #17, PR #18에서 철회 — 코드 자산),
  PR #17 코드 리뷰 교훈(CSRF/상태코드/템플릿 캐시/zero-time/리스너 순서)
- 범위: **Phase 1만**. Phase 2(큐 일시정지, 스케줄 레지스트리)는 별도 스펙.

## 배경

v0.3.0의 웹 콘솔은 standalone 전용이라 철회했다(PR #18). 이후 코어에
cluster 지원(v0.4.0), chains(v0.6.0), groups(v0.7.0), 성능 최적화(v0.8.0)가
쌓였다. cluster-aware로 재개발하고, 그동안 추가된 워크플로 기능을 UI의
중심 가치로 삼는다. "콘솔이 너무 심플하다"는 v1 피드백도 반영한다.

## 확정 결정 (브레인스토밍 — 브라우저 목업 선택)

1. **대시보드 = 카드 그리드**: 큐마다 카드(큰 pending 숫자·가중치 뱃지·
   스파크라인·retry/dead/completed 요약). 문제 있는 큐(archived>0)는 빨간
   좌측 보더 + ⚠ 강조. 하단에 스케줄러 상태 바. (테이블/사이드바 안 대신 선택)
2. **태스크 상세 = 스테퍼**: 체인을 `✓완료 → ✗현재(실패) → ③대기` 그래픽
   으로, 그룹은 "남은 멤버 N" + pending 멤버 격자. (컴팩트 표 안 대신 선택)
3. **기능 8종 전부 채택**, 2단계 분할:
   - Phase 1: 기본기+cluster 접속, 워크플로 시각화, 자동 새로고침, 검색/필터,
     벌크 액션, 스파크라인, 디자인+다크모드, E-lite 스케줄러 상태
   - Phase 2(별도): 큐 일시정지/재개(코어 pause 필요), 스케줄 레지스트리(코어)
4. **테마**: `prefers-color-scheme` 기본 + 수동 토글. 상태 색: pending 파랑 /
   retry 주황 / archived 빨강 / completed 초록.

## 아키텍처 (v1 원칙 유지 + 리뷰 교훈 선반영)

- **`contrib/webui` 별도 Go 모듈** (contrib 패턴, `replace ../`). 코어 의존성 0.
- **`html/template` + `embed` + 바닐라 JS/CSS. npm 없음.**
- 공개 `chronos.Inspector`에만 의존 (internal 접근 금지).
- v1 리뷰 교훈 선반영: 템플릿 시작 시 1회 캐싱(map), HTTP 상태 구분
  (`ErrTaskNotFound`→404, `ErrInvalidState`→400, 기타 500), 파괴적 POST
  Origin 체크, zero-time `—` 표시, 리스너 bind 후 브라우저 오픈.
- 서버 렌더링 + PRG 유지. 예외로 **JSON 엔드포인트 1개**: `GET /api/stats`
  (자동 새로고침·스파크라인용 큐 카운트 스냅샷).

```
contrib/webui/
  go.mod                replace ../
  webui.go              Handler(insp, opts) http.Handler + 라우팅
  handlers.go           페이지/액션/api 핸들러
  render.go             embed 템플릿 1회 파싱 + 렌더
  sparkline.go          인메모리 링버퍼(큐별 최근 ~10분 카운트) + 인라인 SVG
  templates/            layout/dashboard/queue/task/scheduler.html
  static/style.css      라이트+다크(prefers-color-scheme + data-theme 토글)
  static/app.js         자동 새로고침 폴링(5s, 토글), 벌크 확인 다이얼로그
  cmd/webui/main.go     플래그 + 서버 + 브라우저 오픈
  README.md             quickstart(보안 경고 포함)
  webui_test.go         httptest + 실제 Redis (+cluster 스모크)
```

## 기능 상세 (Phase 1)

### F1. cluster 접속 (재개발의 이유)
- `cmd/webui` 플래그: CLI(cmd/chronos)와 동일한 `--standalone`/`--cluster`
  상호 배타 + `--redis`(콤마 시드) + `--db`(standalone 전용, cluster+db≠0
  에러). `buildClient` 로직은 cmd/chronos의 것을 미러링(모듈이 달라 공유
  불가 — 중복 허용, 각자 테스트).
- 헤더에 연결 정보 표시: `standalone 127.0.0.1:6379 db0` 또는 `cluster(3 nodes)`.

### F2. 대시보드 (카드 그리드)
- `insp.Queues()` → 큐 카드. 카드: 큐명+가중치는 표시 불가(가중치는 서버
  설정이라 Inspector가 모름 — **표시 생략**, 스펙 확정), pending 큰 숫자,
  상태 요약(retry/archived/completed), archived>0이면 경고 스타일.
- 스파크라인: UI 프로세스가 `/api/stats` 폴링 주기로 수집한 인메모리
  링버퍼(큐별 pending+active, 최근 10분) → 인라인 SVG. 프로세스 재시작 시
  초기화됨을 툴팁으로 명시. Grafana 대체가 아님(README).
- 하단 스케줄러 상태 바(F7).

### F3. 큐 상세
- 상태 탭 4개(scheduled/retry/archived/completed) + 목록(ID·Kind·에러 요약·
  시각·🔗/🧩 아이콘). 기본 탭 archived.
- **Kind 필터**: 로드된 목록(limit 100)에 대한 webui 계층 필터(쿼리 파라미터).
- **벌크 액션**: "이 상태 전체 재실행"/"전체 삭제" — webui가
  `ListTasks`→`RunTask`/`DeleteTask` 반복(코어 무변경). limit 단위로
  반복해 전량 처리, 진행 결과 요약을 리다이렉트 메시지로. JS 확인
  다이얼로그 필수(파괴적).

### F4. 태스크 상세 (스테퍼)
- 체인: `✓ (완료 i개) → ✗/● 현재(kind) → 대기 링크들(kind)` 스테퍼.
  코어 노출 추가 필요: `TaskInfo.ChainIndex int`(현재 위치),
  `ChainNext []string`(대기 링크 kind 순서 — msg.Chain에서 추출).
  과거 링크의 kind는 저장돼 있지 않음 → 완료 단계는 무명 ✓ 원으로 표시(개수 = ChainIndex).
- 그룹: "남은 멤버 N" + pending 멤버 ID 격자. 코어 노출 추가:
  `Inspector.GroupMembers(ctx, cbQueue, groupID) ([]string, error)`(SMEMBERS).
  전체 멤버 수는 msg에 없음 → **잔여만 표시**(스펙 확정).
- 에러 패널(빨강)/payload 패널(JSON pretty), 재실행 버튼 라벨에 재개 의미
  명시("Re-run — resumes the chain/group").

### F5. 검색
- 헤더 검색창: 태스크 ID 정확 일치 → 큐 선택 후 `GetTask`(또는 전 큐 순회
  — 큐 목록이 있으므로 순회 조회, 첫 매치로 이동). Kind 검색은 F3의 필터로.

### F6. 자동 새로고침
- `static/app.js`: 5초 폴링으로 `/api/stats` fetch → 대시보드 카운트·스파크
  라인 갱신(DOM 부분 교체). 토글 버튼(●/○), 기본 on. 목록/상세 페이지는
  수동(PRG 유지 — 액션 중 리로드 방지).

### F7. E-lite 스케줄러 상태
- 코어 노출 추가: `Inspector.SchedulerStatus(ctx) (*SchedulerStatus, error)` —
  `{LeaderID string(chronos:leader GET, 없으면 ""), Schedules []ScheduleInfo{ID, LastFired time.Time}}`
  (SCAN `chronos:sched:*:last`). **발화 이력 있는 스케줄만** 보임을 문서 명시
  (등록 전체 목록은 Phase 2 레지스트리).
- UI: 대시보드 하단 바 + `/scheduler` 상세 페이지(스케줄 표).

### 보안 (v1 원칙 + 강화)
- localhost 기본 바인드, 인증 없음(원격은 리버스 프록시 위임 — README 경고).
- 파괴적 POST(run/delete/벌크) Origin 체크(v1 리뷰 반영분 계승).
- 벌크는 JS 확인 다이얼로그 + POST 폼(GET 불가).

## 코어 변경 (Inspector 소폭 — Phase 1 허용 범위)

1. `TaskInfo.ChainIndex int` + `ChainNext []string` (msg.ChainIndex/Chain에서).
2. `Inspector.GroupMembers(ctx, cbQueue, groupID) ([]string, error)`.
3. `Inspector.SchedulerStatus(ctx)` + `SchedulerStatus`/`ScheduleInfo` 타입.
- 전부 읽기 전용, 기존 데이터에서 추출 — Lua/서버 로직 무변경.

## 테스트 (TDD)

**코어**: ChainIndex/ChainNext 노출(체인 태스크 GetTask), GroupMembers,
SchedulerStatus(리더 없음/있음, 스케줄 0/N개).
**webui** (httptest + 실제 Redis DB15):
1. 대시보드: 카드 렌더(큐·카운트), archived>0 경고 클래스.
2. 큐 상세: 탭·목록·Kind 필터·아이콘(🔗/🧩).
3. 태스크 상세: 스테퍼 렌더(체인 위치·대기 kind), 그룹 잔여, 에러/payload.
4. 액션: run/delete PRG, 벌크(여러 건 시딩→전체 재실행→목록 빈 것),
   405/403(Origin)/404.
5. `/api/stats` JSON 스키마.
6. **cluster 스모크**: `NewClusterRedis`로 Handler 생성→대시보드 200
   (webui 모듈 자체 헬퍼, REDIS_CLUSTER_ADDRS opt-in).
**cmd**: buildClient 단위 테스트(cmd/chronos와 동일 케이스) + smoke 200.

## 관찰 습관 / 문서

- `contrib/webui/README.md` quickstart: `go run ./examples/tour`(13섹션 —
  chain/group dead-letter 데모 포함) → `go run ./cmd/webui --db 15` →
  카드/스테퍼/재개를 눈으로. 보안 경고 상단.
- 루트 README Observability에 Web console 항목 복원(cluster 지원 명시).
- CI에 contrib/webui 테스트 스텝 복원.

## 알려진 한계 / 후속 (Phase 2+)

- 큐 일시정지/재개(코어 pause), 스케줄 레지스트리(등록 전체 목록).
- 카드의 큐 가중치 표시(서버 설정이라 Inspector가 모름).
- 그룹 전체 멤버 수(잔여만 표시), 체인 과거 링크 kind(무명 표시).
- 스파크라인은 UI 프로세스 수명 — 히스토리는 Grafana.
- 페이지네이션은 limit+"더 있음"(커서는 후속).
