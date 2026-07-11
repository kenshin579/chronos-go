# chronos-go Web UI (태스크 관리 콘솔) 설계

- 상태: 승인됨 (2026-07-11)
- 관련: Inspector API(`inspector.go`), CLI(`cmd/chronos`), `contrib/prometheus`(nested 모듈 선례)

## 목적

chronos-go는 헤드리스 라이브러리다. 이미 Grafana(시계열 메트릭)와 CLI(`task ls/run/rm`)가
있지만, **개별 dead-letter 태스크를 열어 "왜 죽었는지"(payload + 에러)를 확인하고 재실행/삭제**
하는 흐름은 둘 다 매끄럽게 못 한다. Grafana는 집계 메트릭만 다뤄 태스크 원본 데이터에 접근하지
않고, CLI는 명령어 조합이 필요하다. 이 공백을 브라우저 기반 **태스크 관리 콘솔**로 채운다.

명시적 비-목표: 시계열 그래프/차트(= Grafana 영역), 실시간 스트리밍, 자체 인증, SPA.

## 핵심 결정 (브레인스토밍 확정)

1. **배치**: `contrib/webui` nested 모듈(`contrib/prometheus`와 동일 패턴). 코어 의존성 0 유지.
   Wails 등 데스크톱 앱 대신 `embed` 정적 자산 내장 HTTP 서버 + 브라우저. 로컬 도구 경험은
   브라우저 자동 오픈으로 살리고, 서버/사이드카 원격 접속도 지원.
2. **스택**: 순수 Go `html/template` + `embed` + 최소 바닐라 JS. npm/빌드 툴체인 없음.
3. **범위**: 태스크 관리 콘솔 — 대시보드 + 큐 상세 + 태스크 상세 + run/delete 액션.
4. **에러 저장**: 마지막 에러를 Redis에 저장(콘솔의 핵심 가치). Lua 무변경으로 구현.
5. **보안**: 기본 localhost 바인드, 인증 없음. 원격은 리버스 프록시 위임(README 문서화).
6. **갱신**: 수동. 액션 후 PRG(Post/Redirect/Get)로 최신 상태 표시.

## 아키텍처

두 부분으로 나뉜다: (A) 코어 라이브러리(main 모듈)에 데이터 노출을 보강, (B) `contrib/webui`
모듈이 그 위에서 HTML을 렌더링. webui는 오직 공개 `chronos.Inspector`에만 의존한다 — 코어
내부(rdb/base)를 직접 건드리지 않는다.

### A. 코어 라이브러리 변경 (main 모듈)

**A1. 마지막 에러 저장 (Lua 무변경)**
- `base.TaskMessage`에 필드 추가: `LastErr string `json:"last_err,omitempty"``.
- `server.go`:
  - retry 경로: `s.rdb.Retry(...)` 호출 직전 `msg.LastErr = err.Error()`.
  - dead-letter 경로: `deadLetter(...)` 안에서 `s.rdb.Archive(...)` 직전 `msg.LastErr = cause.Error()`.
- `moveToZSet`가 이미 `base.EncodeMessage(msg)`로 재인코딩하므로 세팅만 하면 hash의 `"msg"`에
  함께 저장된다. **rdb Lua 스크립트/시그니처 변경 없음.**
- 회귀 안전: 기존 retry/archive 테스트가 그대로 통과해야 한다. 신규 필드는 omitempty라
  기존에 저장된 태스크(LastErr 없음)도 디코딩에 문제 없다.

**A2. `TaskInfo` 확장** (`chronos.go`)
```go
type TaskInfo struct {
    ID            string
    Kind          string
    Queue         string
    State         string    // "scheduled" | "retry" | "archived"
    Payload       []byte
    Retried       int
    MaxRetry      int
    LastErr       string
    NextProcessAt time.Time // ZSET score: 예약시각 / 재시도예정 / 사망시각
}
```
- 기존 필드(ID/Kind/Queue)는 유지 — CLI 등 기존 소비자 무영향.

**A3. `Inspector.ListTasks` 강화**
- rdb에 score를 함께 돌려주는 경로 필요. `ListZSetTasks`를 `ZRangeWithScores`로 바꾸거나
  score를 함께 반환하는 형태로 조정(내부 함수라 시그니처 변경 허용). 각 태스크의 시각을
  `NextProcessAt`에 채운다.
- 반환 `TaskInfo`에 Payload/Retried/MaxRetry/LastErr/State를 채운다.

**A4. `Inspector.GetTask` 신규**
```go
func (i *Inspector) GetTask(ctx context.Context, qname, taskID string) (*TaskInfo, error)
```
- `rdb.GetTask`로 `TaskMessage`를 읽어 TaskInfo로 매핑.
- State: 태스크 hash의 top-level `"state"` 필드(권위 필드)를 읽어 사용자용 문자열로 변환.
- NextProcessAt: state로 어느 ZSET인지 판단 후 `ZScore`로 시각을 채운다(없으면 zero time).
- 존재하지 않으면 명확한 not-found 에러.

### B. `contrib/webui` 모듈

```
contrib/webui/
  go.mod              module .../contrib/webui, replace ../../ => 코어
  webui.go            Handler(insp *chronos.Inspector) http.Handler
  handlers.go         각 페이지/액션 핸들러
  render.go           html/template 로딩(embed) + 공통 렌더 헬퍼
  templates/
    layout.html       공통 레이아웃(헤더/네비/푸터)
    dashboard.html    큐 목록 + 상태별 카운트
    queue.html        큐 상세: 상태 탭 + 태스크 목록
    task.html         태스크 상세: payload/에러/재시도/시각 + 액션 버튼
  static/
    style.css         최소 CSS (embed)
  cmd/webui/main.go   플래그 + Inspector 생성 + 서버 + 브라우저 자동 오픈
  README.md           quickstart + 보안(프록시) 안내
  webui_test.go       httptest + 실제 Redis
```

**B1. `Handler(insp) http.Handler`**
- `http.ServeMux`(Go 1.22+ 메서드/경로 패턴)로 라우팅. 자기 앱 mux에 서브트리로 마운트 가능.
- 정적 자산은 `embed.FS`를 `http.FileServer`로 `/static/` 아래 서빙.

**B2. 라우트**
| 경로 | 메서드 | 동작 |
|---|---|---|
| `/` | GET | 대시보드: `insp.Queues()` → 큐별 카운트 표 |
| `/queues/{queue}` | GET | 큐 상세: 쿼리 `?state=archived`(기본 archived), `insp.ListTasks(q, state, limit)` |
| `/queues/{queue}/tasks/{id}` | GET | 태스크 상세: `insp.GetTask(q, id)` |
| `/queues/{queue}/tasks/{id}/run` | POST | `insp.RunTask` → 303 리다이렉트(큐 상세) |
| `/queues/{queue}/tasks/{id}/delete` | POST | `insp.DeleteTask` → 303 리다이렉트(큐 상세) |
- 목록 상태는 scheduled/retry/archived. 기본 진입은 archived(dead-letter 진단이 주 용도).
- 액션은 POST만(GET으로 파괴적 동작 금지). 성공/실패는 리다이렉트 쿼리(`?msg=`)로 표시.
- 목록 limit는 기본값(예: 100) + 상한. 초과 시 "N개 표시(더 있음)" 안내.

**B3. 템플릿/렌더링**
- `template.Must(template.ParseFS(embedFS, "templates/*.html"))`를 프로세스 시작 시 1회 파싱.
- payload는 JSON이면 들여쓰기 포맷, 아니면 원문/16진 요약. 큰 payload는 잘라서 표시.
- XSS 방지: `html/template`의 자동 이스케이프에 의존(payload/에러 문자열 그대로 이스케이프됨).

**B4. `cmd/webui/main.go`**
- 플래그: `--addr`(기본 `127.0.0.1:8080`), `--redis`(기본 `127.0.0.1:6379`),
  `--db`(기본 0), `--no-open`(브라우저 자동 오픈 비활성).
- redis 클라이언트 생성 → `chronos.NewInspector` → `webui.Handler` → `http.Server` 구동.
- 기동 후 `--no-open`이 아니면 OS별로 브라우저를 연다(darwin `open`, linux `xdg-open`,
  windows `rundll32`). 실패해도 서버는 계속(로그만).
- graceful shutdown(SIGINT/SIGTERM).

## 보안

- 기본 `127.0.0.1`에만 바인드 → 로컬 도구로 안전.
- 원격 접속: `--addr 0.0.0.0:8080`로 열되, **인증/TLS는 리버스 프록시(nginx, oauth2-proxy 등)
  에 위임**. README에 명시. 라이브러리에 자체 인증을 내장하지 않는다(관찰 도구 표준, 보안 착시
  방지). run/delete가 파괴적이므로 이 경고를 README 상단에 굵게 둔다.

## 테스트 (TDD)

**코어(main 모듈)** — 실제 Redis, DB 15, `-p 1`:
1. `LastErr` 저장: 실패하는 핸들러 → retry 후 `Inspector.GetTask`가 마지막 에러 문자열 반환.
   dead-letter 후 archived 태스크의 `GetTask`도 에러 반환.
2. `ListTasks` 강화: scheduled 태스크의 `NextProcessAt`가 예약시각과 일치, payload/retried 채워짐.
3. `GetTask` not-found: 없는 ID → 에러.
4. 회귀: 기존 retry/archive/inspector 테스트 전부 통과.

**webui 모듈** — `httptest` + 실제 Redis:
5. 대시보드 GET → 시딩한 큐/카운트가 HTML에 렌더됨.
6. 큐 상세 GET(`?state=archived`) → dead-letter 태스크 행이 보임.
7. 태스크 상세 GET → payload와 LastErr 문자열이 페이지에 포함.
8. run POST → 303 + Location, 이후 태스크가 스트림으로 승격(재조회로 확인).
9. delete POST → 303, 이후 태스크 사라짐.
10. GET으로 run/delete 시도 → 405.

## 관찰 습관 (완료 조건)

webui는 별도 모듈이라 `examples/tour`에 넣지 않는다. 대신:
- `contrib/webui/README.md`에 **quickstart**: 데이터 만들기(`go run ./examples/tour` 또는
  prometheus loadgen을 특정 DB에 실행) → `go run ./contrib/webui/cmd/webui --db <n>` →
  브라우저에서 dead-letter 열어 에러 확인 → 재실행. 각 화면이 무엇을 보여주는지 서술.
- 루트 `README.md` Observability 섹션에 webui 한 줄 추가.
- 사용자가 실제로 `go run`으로 콘솔을 띄워 눈으로 확인(헤드리스 라이브러리의 UI 확인 습관).

## 검증

- 코어: `make check`(gofmt + vet + core `-race -p 1` + contrib).
- webui: `cd contrib/webui && go test ./... -race`. CI에 이 스텝 추가.
- `go run ./contrib/webui/cmd/webui`로 수동 눈 확인.
- k:code-reviewer 리뷰 후 PR(assignee kenshin579).

## 알려진 한계 / 후속

- 목록은 ZSET 상태(scheduled/retry/archived)만. pending/active는 스트림/PEL의 일시적 상태라
  카운트로만 표시(개별 목록 없음).
- 페이지네이션은 단순 limit + "더 있음" 안내(커서 기반 페이징은 후속).
- 인증/TLS는 범위 밖(프록시 위임).
