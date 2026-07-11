# Web UI (태스크 관리 콘솔) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** dead-letter 태스크를 브라우저에서 열어 payload/에러를 확인하고 재실행/삭제하는 태스크 관리 콘솔을 `contrib/webui` nested 모듈로 추가한다.

**Architecture:** 코어(main 모듈)에 태스크 데이터 노출을 보강(`TaskMessage.LastErr` 저장, `Inspector.GetTask`/`TaskInfo` 확장/`ListTasks` 강화)하고, `contrib/webui`가 공개 `chronos.Inspector`에만 의존해 `html/template`+`embed`로 HTML을 렌더링한다. 코어 의존성 0 유지.

**Tech Stack:** Go stdlib(`net/http` 1.22+ 라우팅, `html/template`, `embed`), `redis/go-redis`, 최소 바닐라 JS/CSS. npm/빌드 툴체인 없음.

---

## File Structure

**코어 (main 모듈, `github.com/kenshin579/chronos-go`):**
- Modify `internal/base/task.go` — `TaskMessage`에 `LastErr` 필드.
- Modify `internal/base/task_test.go` (또는 신규) — LastErr 인코딩 라운드트립.
- Modify `internal/rdb/inspect.go` — `ListZSetTasks`가 score 포함 반환(`[]*ZSetTask`), `ZScore` 헬퍼.
- Modify `internal/rdb/inspect_test.go` — score 반환 검증.
- Modify `chronos.go` — `TaskInfo` 확장.
- Modify `inspector.go` — `ListTasks` 강화 + `GetTask` 신규.
- Modify `inspector_test.go` — 강화된 필드 + GetTask 통합 테스트.
- Modify `server.go` — retry/deadLetter 직전 `msg.LastErr` 세팅.
- Modify `server_reliability_test.go` (또는 신규 파일) — LastErr가 GetTask로 노출되는지.

**webui 모듈 (`github.com/kenshin579/chronos-go/contrib/webui`):**
- Create `contrib/webui/go.mod`
- Create `contrib/webui/webui.go` — `Handler(insp) http.Handler`, 라우팅.
- Create `contrib/webui/handlers.go` — 페이지/액션 핸들러.
- Create `contrib/webui/render.go` — embed 템플릿 파싱 + 렌더 헬퍼.
- Create `contrib/webui/templates/{layout,dashboard,queue,task}.html`
- Create `contrib/webui/static/style.css`
- Create `contrib/webui/cmd/webui/main.go` — 플래그 + 서버 + 브라우저 오픈.
- Create `contrib/webui/webui_test.go` — httptest + 실제 Redis.
- Create `contrib/webui/README.md`
- Modify 루트 `README.md` — Observability에 webui 한 줄.
- Modify `.github/workflows/ci.yml` — contrib/webui 테스트 스텝.

---

## Task 1: `TaskMessage.LastErr` 필드

**Files:**
- Modify: `internal/base/task.go`
- Test: `internal/base/task_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`internal/base/task_test.go`에 추가 (파일 없으면 생성, `package base`):

```go
package base

import "testing"

func TestTaskMessage_LastErrRoundTrips(t *testing.T) {
	msg := &TaskMessage{ID: "t1", Kind: "k", Queue: "default", LastErr: "boom: timeout"}
	encoded, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.LastErr != "boom: timeout" {
		t.Errorf("LastErr = %q, want %q", got.LastErr, "boom: timeout")
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/base/ -run TestTaskMessage_LastErrRoundTrips`
Expected: FAIL — `got.LastErr undefined (type *TaskMessage has no field or method LastErr)`

- [ ] **Step 3: 필드 추가**

`internal/base/task.go`의 `TaskMessage` 구조체에서 `UniqueKey` 필드 바로 아래에 추가:

```go
	// LastErr is the error message from the most recent failed attempt. It is
	// persisted so the Inspector and Web UI can show why a task was retried or
	// dead-lettered. Empty until the first failure.
	LastErr string `json:"last_err,omitempty"`
```

- [ ] **Step 4: 통과 확인**

Run: `go test ./internal/base/ -run TestTaskMessage_LastErrRoundTrips`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/base/task.go internal/base/task_test.go
git commit -m "feat: TaskMessage에 LastErr 필드 (마지막 실패 에러 저장)"
```

---

## Task 2: `rdb.ListZSetTasks` score 포함 반환 + `ZScore` 헬퍼

**Files:**
- Modify: `internal/rdb/inspect.go`
- Test: `internal/rdb/inspect_test.go`

**배경:** 현재 `ListZSetTasks`는 `ZRange`로 score(시각)를 버린다. UI가 예약/재시도/사망 시각을 보여주려면 score가 필요하다. 유일한 호출부는 `inspector.go:74`이므로 시그니처를 바꿔도 안전하다.

- [ ] **Step 1: 실패 테스트 작성**

`internal/rdb/inspect_test.go`에 추가 (기존 테스트의 Redis 헬퍼 `newTestRDB`/`testutil` 사용 — 파일 내 기존 패턴을 따를 것):

```go
func TestListZSetTasks_ReturnsScores(t *testing.T) {
	r := newTestRDB(t) // 기존 inspect_test.go의 헬퍼 패턴을 따른다
	ctx := context.Background()

	msg := &base.TaskMessage{ID: "s1", Kind: "k", Queue: "default", State: base.StateScheduled}
	// 예약 태스크를 score=12345로 직접 넣는다.
	encoded, _ := base.EncodeMessage(msg)
	if err := r.client.HSet(ctx, base.TaskKey("default", "s1"), "msg", encoded, "state", int(base.StateScheduled)).Err(); err != nil {
		t.Fatalf("hset: %v", err)
	}
	if err := r.client.ZAdd(ctx, base.ScheduledKey("default"), redis.Z{Score: 12345, Member: "s1"}).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}

	got, err := r.ListZSetTasks(ctx, "default", base.ScheduledKey("default"), 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Msg.ID != "s1" {
		t.Errorf("ID = %q, want s1", got[0].Msg.ID)
	}
	if got[0].Score != 12345 {
		t.Errorf("Score = %v, want 12345", got[0].Score)
	}
}
```

> 주의: `newTestRDB`/import(`context`, `base`, `redis`)는 기존 `inspect_test.go`에 이미 있는 것을 사용. 없으면 파일 상단 패턴을 그대로 복제.

- [ ] **Step 2: 실패 확인**

Run: `go test ./internal/rdb/ -run TestListZSetTasks_ReturnsScores -p 1`
Expected: FAIL — `got[0].Msg undefined` (현재는 `[]*base.TaskMessage` 반환).

- [ ] **Step 3: 구현**

`internal/rdb/inspect.go`에서 `ListZSetTasks`를 교체하고 `ZScore` 헬퍼를 추가:

```go
// ZSetTask is a task referenced by a state ZSET together with its score
// (a Unix timestamp: scheduled-for / retry-at / died-at).
type ZSetTask struct {
	Msg   *base.TaskMessage
	Score float64
}

// ListZSetTasks returns up to limit tasks referenced by a state ZSET, each with
// its score. Entries whose task body has been deleted are skipped.
func (r *RDB) ListZSetTasks(ctx context.Context, qname, zsetKey string, limit int) ([]*ZSetTask, error) {
	if limit <= 0 {
		return nil, nil
	}
	zs, err := r.client.ZRangeWithScores(ctx, zsetKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	tasks := make([]*ZSetTask, 0, len(zs))
	for _, z := range zs {
		id, _ := z.Member.(string)
		msg, err := r.GetTask(ctx, qname, id)
		if err == redis.Nil {
			continue // body gone; skip
		}
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, &ZSetTask{Msg: msg, Score: z.Score})
	}
	return tasks, nil
}

// ZScore returns the score of taskID in zsetKey, or (0, false) if absent.
func (r *RDB) ZScore(ctx context.Context, zsetKey, taskID string) (float64, bool, error) {
	score, err := r.client.ZScore(ctx, zsetKey, taskID).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return score, true, nil
}
```

그런 다음 `inspector.go:74-82`의 호출부를 임시로 컴파일되게 맞춘다 (Task 3에서 최종 형태로 다시 손댐):

```go
	entries, err := i.rdb.ListZSetTasks(ctx, qname, zsetKey, limit)
	if err != nil {
		return nil, err
	}
	infos := make([]*TaskInfo, 0, len(entries))
	for _, e := range entries {
		infos = append(infos, &TaskInfo{ID: e.Msg.ID, Kind: e.Msg.Kind, Queue: e.Msg.Queue})
	}
	return infos, nil
```

- [ ] **Step 4: 통과 확인**

Run: `go test ./internal/rdb/ -run TestListZSetTasks_ReturnsScores -p 1 && go build ./...`
Expected: PASS + 빌드 성공.

- [ ] **Step 5: 커밋**

```bash
git add internal/rdb/inspect.go internal/rdb/inspect_test.go inspector.go
git commit -m "feat: rdb ListZSetTasks가 score 반환 + ZScore 헬퍼"
```

---

## Task 3: `TaskInfo` 확장 + `Inspector.ListTasks` 강화 + `Inspector.GetTask`

**Files:**
- Modify: `chronos.go` (TaskInfo)
- Modify: `inspector.go` (ListTasks, GetTask)
- Test: `inspector_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`inspector_test.go`에 추가 (기존 파일의 Redis 헬퍼/`NewClient`/`Enqueue` 패턴 사용):

```go
func TestInspector_ListTasks_RichFields(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	// 1시간 뒤 예약 → scheduled ZSET에 즉시 안착 (서버 불필요).
	if _, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithProcessIn(time.Hour)); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)
	got, err := insp.ListTasks(ctx, "default", "scheduled", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	ti := got[0]
	if ti.State != "scheduled" {
		t.Errorf("State = %q, want scheduled", ti.State)
	}
	if len(ti.Payload) == 0 {
		t.Error("Payload empty, want non-empty")
	}
	if ti.NextProcessAt.IsZero() {
		t.Error("NextProcessAt is zero, want the scheduled time")
	}
}

func TestInspector_GetTask_ReturnsDetailAndNotFound(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u9"}, WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := NewInspector(client)

	got, err := insp.GetTask(ctx, "default", info.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != info.ID || got.Kind != "email:send" {
		t.Errorf("got %+v", got)
	}
	if got.State != "scheduled" || got.NextProcessAt.IsZero() {
		t.Errorf("state/time wrong: %+v", got)
	}

	if _, err := insp.GetTask(ctx, "default", "does-not-exist"); err == nil {
		t.Error("GetTask for missing id: want error, got nil")
	}
}
```

> `emailArgs`의 `Kind()`는 기존 테스트에서 `"email:send"`. 다르면 실제 값에 맞춰 문자열을 수정.

- [ ] **Step 2: 실패 확인**

Run: `go test . -run 'TestInspector_ListTasks_RichFields|TestInspector_GetTask' -p 1`
Expected: FAIL — `ti.State undefined` / `insp.GetTask undefined`.

- [ ] **Step 3: TaskInfo 확장**

`chronos.go`의 `TaskInfo`를 교체:

```go
// TaskInfo describes an enqueued or stored task. Enqueue returns one with only
// ID/Kind/Queue set; the Inspector fills the rest for stored tasks.
type TaskInfo struct {
	ID    string
	Kind  string
	Queue string

	// The following are populated by Inspector.ListTasks / GetTask for tasks
	// stored in a state ZSET (scheduled / retry / archived).
	State         string    // "scheduled" | "retry" | "archived" | ...
	Payload       []byte    // raw task payload
	Retried       int       // retries already attempted
	MaxRetry      int       // retry budget
	LastErr       string    // most recent failure message ("" if none)
	NextProcessAt time.Time // ZSET score as a time: scheduled-for / retry-at / died-at
}
```

- [ ] **Step 4: ListTasks 강화 + GetTask 추가**

`inspector.go`에서 `ListTasks`를 아래로 교체하고 그 아래에 `GetTask`를 추가. `taskInfoFromMsg` 헬퍼도 추가:

```go
// ListTasks returns up to limit tasks in the given state (scheduled|retry|archived).
func (i *Inspector) ListTasks(ctx context.Context, qname, state string, limit int) ([]*TaskInfo, error) {
	zsetKey, err := zsetKeyForState(qname, state)
	if err != nil {
		return nil, err
	}
	entries, err := i.rdb.ListZSetTasks(ctx, qname, zsetKey, limit)
	if err != nil {
		return nil, err
	}
	infos := make([]*TaskInfo, 0, len(entries))
	for _, e := range entries {
		ti := taskInfoFromMsg(e.Msg)
		ti.NextProcessAt = time.Unix(int64(e.Score), 0)
		infos = append(infos, ti)
	}
	return infos, nil
}

// GetTask returns full detail for a single stored task (scheduled/retry/archived).
func (i *Inspector) GetTask(ctx context.Context, qname, taskID string) (*TaskInfo, error) {
	msg, err := i.rdb.GetTask(ctx, qname, taskID)
	if err == redis.Nil {
		return nil, fmt.Errorf("chronos: task %q not found in queue %q", taskID, qname)
	}
	if err != nil {
		return nil, err
	}
	ti := taskInfoFromMsg(msg)
	// Fill the timestamp from whichever state ZSET this task lives in.
	if zsetKey, kerr := zsetKeyForState(qname, ti.State); kerr == nil {
		if score, ok, serr := i.rdb.ZScore(ctx, zsetKey, taskID); serr == nil && ok {
			ti.NextProcessAt = time.Unix(int64(score), 0)
		}
	}
	return ti, nil
}

// taskInfoFromMsg maps the stored message to the public TaskInfo (no timestamp).
func taskInfoFromMsg(m *base.TaskMessage) *TaskInfo {
	return &TaskInfo{
		ID:       m.ID,
		Kind:     m.Kind,
		Queue:    m.Queue,
		State:    m.State.String(),
		Payload:  m.Payload,
		Retried:  m.Retried,
		MaxRetry: m.MaxRetry,
		LastErr:  m.LastErr,
	}
}
```

`inspector.go` import에 `"fmt"`, `"time"`가 있는지 확인하고 없으면 추가(`redis`, `base`는 이미 있음).

- [ ] **Step 5: 통과 확인**

Run: `go test . -run 'TestInspector_ListTasks_RichFields|TestInspector_GetTask' -p 1`
Expected: PASS

- [ ] **Step 6: 커밋**

```bash
git add chronos.go inspector.go inspector_test.go
git commit -m "feat: Inspector.GetTask + TaskInfo 확장(payload/retried/error/시각)"
```

---

## Task 4: `server.go`에서 `LastErr` 저장

**Files:**
- Modify: `server.go` (retry 경로 + `deadLetter`)
- Test: `server_lasterr_test.go` (신규)

**한계 메모:** recoverer 경로(`rdb.Recover` Lua 내 archive)는 LastErr를 쓰지 않는다. 정상 처리 경로(핸들러가 error 반환 → retry/archive)만 커버한다. 이는 스펙의 "알려진 한계"에 부합.

- [ ] **Step 1: 실패 테스트 작성**

`server_lasterr_test.go` 신규 (`package chronos`):

```go
package chronos

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_PersistsLastErrOnDeadLetter(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		return errors.New("kaboom")
	})
	srv := NewServer(client, ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 1,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "u1"}, WithMaxRetry(0))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	insp := NewInspector(client)
	// MaxRetry 0 → 1회 실행 후 즉시 dead-letter. archived에 나타날 때까지 폴링.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, gerr := insp.GetTask(ctx, "default", info.ID)
		if gerr == nil && got.State == "archived" {
			if got.LastErr != "kaboom" {
				t.Fatalf("LastErr = %q, want kaboom", got.LastErr)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task not archived with LastErr in time (last err=%v)", gerr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `go test . -run TestServer_PersistsLastErrOnDeadLetter -p 1`
Expected: FAIL — `LastErr = "", want kaboom`.

- [ ] **Step 3: 구현**

`server.go`의 retry 경로에서 `msg.Retried++` 다음 줄, `s.rdb.Retry(...)` 호출 **직전**에 추가:

```go
	msg.Retried++
	msg.LastErr = err.Error()
	retryAt := time.Now().Add(s.cfg.RetryDelayFunc(msg.Retried, err))
```

`deadLetter` 함수의 첫 줄(`if msg.NoArchive {` 바로 위)에 추가:

```go
func (s *Server) deadLetter(ctx context.Context, qname, streamID string, msg *base.TaskMessage, cause error) {
	msg.LastErr = cause.Error()
	if msg.NoArchive {
```

- [ ] **Step 4: 통과 확인**

Run: `go test . -run TestServer_PersistsLastErrOnDeadLetter -p 1`
Expected: PASS

- [ ] **Step 5: 코어 전체 회귀 확인**

Run: `make check`
Expected: 모든 패키지 PASS (기존 retry/archive/inspector 테스트 포함).

- [ ] **Step 6: 커밋**

```bash
git add server.go server_lasterr_test.go
git commit -m "feat: retry/dead-letter 시 msg.LastErr 저장 (Lua 무변경)"
```

---

## Task 5: `contrib/webui` 모듈 스캐폴드 + 렌더 기반

**Files:**
- Create: `contrib/webui/go.mod`
- Create: `contrib/webui/render.go`
- Create: `contrib/webui/webui.go`
- Create: `contrib/webui/templates/layout.html`, `dashboard.html`, `queue.html`, `task.html`
- Create: `contrib/webui/static/style.css`
- Create: `contrib/webui/webui_test.go`

- [ ] **Step 1: go.mod 생성**

`contrib/webui/go.mod`:

```
module github.com/kenshin579/chronos-go/contrib/webui

go 1.26

replace github.com/kenshin579/chronos-go => ../../

require (
	github.com/kenshin579/chronos-go v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.21.0
)
```

- [ ] **Step 2: 최소 템플릿/정적 자산 생성**

`contrib/webui/templates/layout.html`:

```html
{{define "layout"}}<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>chronos-go — {{.Title}}</title>
<link rel="stylesheet" href="/static/style.css">
</head>
<body>
<header><a href="/">chronos-go console</a></header>
<main>{{template "content" .}}</main>
</body>
</html>{{end}}
```

`contrib/webui/templates/dashboard.html`:

```html
{{define "content"}}
<h1>Queues</h1>
<table>
<tr><th>Queue</th><th>Pending</th><th>Active</th><th>Scheduled</th><th>Retry</th><th>Archived</th></tr>
{{range .Queues}}
<tr>
<td><a href="/queues/{{.Queue}}">{{.Queue}}</a></td>
<td>{{.Pending}}</td><td>{{.Active}}</td><td>{{.Scheduled}}</td><td>{{.Retry}}</td>
<td><a href="/queues/{{.Queue}}?state=archived">{{.Archived}}</a></td>
</tr>
{{end}}
</table>
{{end}}
```

`contrib/webui/templates/queue.html`:

```html
{{define "content"}}
<h1>Queue: {{.Queue}}</h1>
{{if .Msg}}<p class="msg">{{.Msg}}</p>{{end}}
<nav class="states">
{{range .States}}<a href="/queues/{{$.Queue}}?state={{.}}" {{if eq . $.State}}class="active"{{end}}>{{.}}</a> {{end}}
</nav>
<table>
<tr><th>ID</th><th>Kind</th><th>When</th></tr>
{{range .Tasks}}
<tr>
<td><a href="/queues/{{$.Queue}}/tasks/{{.ID}}">{{.ID}}</a></td>
<td>{{.Kind}}</td>
<td>{{.NextProcessAt.Format "2006-01-02 15:04:05"}}</td>
</tr>
{{else}}
<tr><td colspan="3">no tasks in this state</td></tr>
{{end}}
</table>
{{end}}
```

`contrib/webui/templates/task.html`:

```html
{{define "content"}}
<h1>Task {{.Task.ID}}</h1>
<p><a href="/queues/{{.Task.Queue}}?state={{.Task.State}}">&larr; back to {{.Task.Queue}}</a></p>
<dl>
<dt>Kind</dt><dd>{{.Task.Kind}}</dd>
<dt>State</dt><dd>{{.Task.State}}</dd>
<dt>Retried</dt><dd>{{.Task.Retried}} / {{.Task.MaxRetry}}</dd>
<dt>When</dt><dd>{{.Task.NextProcessAt.Format "2006-01-02 15:04:05"}}</dd>
<dt>Last error</dt><dd class="err">{{if .Task.LastErr}}{{.Task.LastErr}}{{else}}(none){{end}}</dd>
</dl>
<h2>Payload</h2>
<pre>{{.Payload}}</pre>
<form method="post" action="/queues/{{.Task.Queue}}/tasks/{{.Task.ID}}/run"><button>Re-run now</button></form>
<form method="post" action="/queues/{{.Task.Queue}}/tasks/{{.Task.ID}}/delete"><button class="danger">Delete</button></form>
{{end}}
```

`contrib/webui/static/style.css`:

```css
body{font-family:system-ui,sans-serif;margin:2rem;color:#222}
header a{font-weight:bold;text-decoration:none;color:#0645ad}
table{border-collapse:collapse;margin-top:1rem}
th,td{border:1px solid #ccc;padding:.3rem .6rem;text-align:left}
nav.states a.active{font-weight:bold;text-decoration:underline}
pre{background:#f4f4f4;padding:1rem;overflow:auto}
.err{color:#a00;white-space:pre-wrap}
.msg{background:#efe;border:1px solid #7c7;padding:.4rem}
button.danger{color:#a00}
```

- [ ] **Step 3: render.go 생성**

`contrib/webui/render.go`:

```go
package webui

import (
	"embed"
	"html/template"
	"net/http"
)

//go:embed templates/*.html
var templatesFS embed.FS

//go:embed static/*
var staticFS embed.FS

// tmpl holds every page parsed with the shared layout.
var tmpl = template.Must(template.ParseFS(templatesFS, "templates/*.html"))

// render writes the named page (dashboard|queue|task) wrapped in the layout.
// Each page template defines "content"; we clone the layout per page so the
// right "content" block is bound.
func render(w http.ResponseWriter, page string, data any) {
	t := template.Must(template.ParseFS(templatesFS, "templates/layout.html", "templates/"+page+".html"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
```

> `tmpl` 전역은 embed 검증용으로 남겨두되 사용하지 않으면 삭제 가능. 실제 렌더는 `render`가 페이지별로 layout+page를 파싱한다(각 페이지가 같은 `content` 이름을 정의하므로 페이지별 파싱이 필요).

- [ ] **Step 4: webui.go 스캐폴드 (Handler + 정적 서빙 + 대시보드만)**

`contrib/webui/webui.go`:

```go
// Package webui serves a browser-based task-management console for chronos-go,
// backed entirely by the public chronos.Inspector. Mount Handler in your own
// http.ServeMux, or run cmd/webui for a standalone server.
package webui

import (
	"io/fs"
	"net/http"

	"github.com/kenshin579/chronos-go"
)

// listStates are the ZSET-backed states a queue's tasks can be listed by.
var listStates = []string{"scheduled", "retry", "archived"}

// Handler returns the console's HTTP handler backed by insp.
func Handler(insp *chronos.Inspector) http.Handler {
	s := &server{insp: insp}
	mux := http.NewServeMux()

	static, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(static))))

	mux.HandleFunc("GET /{$}", s.dashboard)
	mux.HandleFunc("GET /queues/{queue}", s.queueDetail)
	mux.HandleFunc("GET /queues/{queue}/tasks/{id}", s.taskDetail)
	mux.HandleFunc("POST /queues/{queue}/tasks/{id}/run", s.runTask)
	mux.HandleFunc("POST /queues/{queue}/tasks/{id}/delete", s.deleteTask)
	return mux
}

type server struct {
	insp *chronos.Inspector
}
```

- [ ] **Step 5: 첫 실패 테스트 작성 (대시보드)**

`contrib/webui/webui_test.go`:

```go
package webui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
)

// newTestRedis dials a test Redis (DB 15), flushes it, and cleans up. Skips if
// none is reachable. This module has no access to the core's internal testutil,
// so it carries its own helper.
func newTestRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("redis not available at %s: %v", addr, err)
	}
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	t.Cleanup(func() { _ = client.FlushDB(ctx); _ = client.Close() })
	return client
}

// seedScheduled enqueues a far-future task so it lands in the scheduled ZSET
// without needing a running server. Returns its task ID.
func seedScheduled(t *testing.T, client redis.UniversalClient) string {
	t.Helper()
	c := chronos.NewClient(client)
	defer c.Close()
	info, err := chronos.Enqueue(context.Background(), c, demoArgs{Msg: "hi"}, chronos.WithProcessIn(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return info.ID
}

type demoArgs struct {
	Msg string `json:"msg"`
}

func (demoArgs) Kind() string { return "demo:task" }

func TestDashboard_ShowsQueue(t *testing.T) {
	client := newTestRedis(t)
	seedScheduled(t, client)

	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "default") {
		t.Errorf("dashboard missing queue 'default':\n%s", body)
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b := new(strings.Builder)
	if _, err := b.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b.String()
}
```

- [ ] **Step 6: 실패 확인**

Run: `cd contrib/webui && go mod tidy && go test ./... -run TestDashboard_ShowsQueue`
Expected: FAIL — `s.dashboard undefined` (핸들러 미구현).

- [ ] **Step 7: dashboard 핸들러 구현 (handlers.go)**

`contrib/webui/handlers.go` 생성:

```go
package webui

import (
	"net/http"

	"github.com/kenshin579/chronos-go"
)

func (s *server) dashboard(w http.ResponseWriter, r *http.Request) {
	queues, err := s.insp.Queues(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render(w, "dashboard", struct {
		Title  string
		Queues []*chronos.QueueInfo
	}{Title: "queues", Queues: queues})
}
```

Task 8~10에서 나머지 핸들러(queueDetail/taskDetail/runTask/deleteTask)를 추가하기 전까지 컴파일되도록, `handlers.go`에 임시 스텁을 함께 둔다:

```go
func (s *server) queueDetail(w http.ResponseWriter, r *http.Request) { http.Error(w, "todo", 501) }
func (s *server) taskDetail(w http.ResponseWriter, r *http.Request)  { http.Error(w, "todo", 501) }
func (s *server) runTask(w http.ResponseWriter, r *http.Request)     { http.Error(w, "todo", 501) }
func (s *server) deleteTask(w http.ResponseWriter, r *http.Request)  { http.Error(w, "todo", 501) }
```

- [ ] **Step 8: 통과 확인**

Run: `cd contrib/webui && go test ./... -run TestDashboard_ShowsQueue`
Expected: PASS

- [ ] **Step 9: 커밋**

```bash
git add contrib/webui
git commit -m "feat: contrib/webui 스캐폴드 + 대시보드 (Handler/embed 템플릿)"
```

---

## Task 6: 큐 상세 핸들러

**Files:**
- Modify: `contrib/webui/handlers.go` (queueDetail 스텁 교체)
- Test: `contrib/webui/webui_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`webui_test.go`에 추가:

```go
func TestQueueDetail_ListsScheduledTask(t *testing.T) {
	client := newTestRedis(t)
	id := seedScheduled(t, client)

	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/queues/default?state=scheduled")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, id) {
		t.Errorf("queue detail missing task id %q:\n%s", id, body)
	}
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd contrib/webui && go test ./... -run TestQueueDetail_ListsScheduledTask`
Expected: FAIL — 응답이 501/"todo".

- [ ] **Step 3: 구현**

`handlers.go`의 `queueDetail` 스텁을 교체:

```go
const listLimit = 100

func (s *server) queueDetail(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	state := r.URL.Query().Get("state")
	if state == "" {
		state = "archived"
	}
	tasks, err := s.insp.ListTasks(r.Context(), queue, state, listLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	render(w, "queue", struct {
		Title  string
		Queue  string
		State  string
		States []string
		Tasks  []*chronos.TaskInfo
		Msg    string
	}{
		Title:  queue,
		Queue:  queue,
		State:  state,
		States: listStates,
		Tasks:  tasks,
		Msg:    r.URL.Query().Get("msg"),
	})
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd contrib/webui && go test ./... -run TestQueueDetail_ListsScheduledTask`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add contrib/webui/handlers.go contrib/webui/webui_test.go
git commit -m "feat: webui 큐 상세 페이지 (상태별 태스크 목록)"
```

---

## Task 7: 태스크 상세 핸들러 (payload + LastErr)

**Files:**
- Modify: `contrib/webui/handlers.go` (taskDetail 스텁 교체)
- Test: `contrib/webui/webui_test.go`

- [ ] **Step 1: 실패 테스트 작성**

archived + LastErr 태스크가 필요하므로, 실패 핸들러로 서버를 돌려 dead-letter를 만든다. `webui_test.go`에 헬퍼와 테스트 추가:

```go
// seedDeadLetter runs a failing handler once so the task is archived with a
// LastErr, and returns its task ID.
func seedDeadLetter(t *testing.T, client redis.UniversalClient) string {
	t.Helper()
	c := chronos.NewClient(client)
	defer c.Close()
	ctx := context.Background()

	mux := chronos.NewMux()
	chronos.AddHandler(mux, func(ctx context.Context, task *chronos.Task[demoArgs]) error {
		return errDemo
	})
	srv := chronos.NewServer(client, chronos.ServerConfig{
		Queues:      map[string]int{"default": 1},
		Concurrency: 1,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := chronos.Enqueue(ctx, c, demoArgs{Msg: "boom"}, chronos.WithMaxRetry(0))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	insp := chronos.NewInspector(client)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, gerr := insp.GetTask(ctx, "default", info.ID)
		if gerr == nil && got.State == "archived" {
			return info.ID
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("task did not reach archived state in time")
	return ""
}

var errDemo = errorsNew("demo failure")

// errorsNew avoids an extra import block edit in the plan; replace with
// errors.New("demo failure") and add the "errors" import.
func errorsNew(s string) error { return demoErr(s) }

type demoErr string

func (e demoErr) Error() string { return string(e) }

func TestTaskDetail_ShowsPayloadAndError(t *testing.T) {
	client := newTestRedis(t)
	id := seedDeadLetter(t, client)

	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/queues/default/tasks/" + id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "demo failure") {
		t.Errorf("task detail missing LastErr:\n%s", body)
	}
	if !strings.Contains(body, "boom") {
		t.Errorf("task detail missing payload:\n%s", body)
	}
}
```

> 정리: 위 `errorsNew`/`demoErr` 우회는 계획 가독성용이다. 구현 시 그냥 `import "errors"` 후 `var errDemo = errors.New("demo failure")`로 대체하라.

- [ ] **Step 2: 실패 확인**

Run: `cd contrib/webui && go test ./... -run TestTaskDetail_ShowsPayloadAndError`
Expected: FAIL — 응답이 501/"todo".

- [ ] **Step 3: 구현**

`handlers.go`의 `taskDetail` 스텁 교체 (payload를 문자열로 안전 표시):

```go
func (s *server) taskDetail(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	id := r.PathValue("id")
	task, err := s.insp.GetTask(r.Context(), queue, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	render(w, "task", struct {
		Title   string
		Task    *chronos.TaskInfo
		Payload string
	}{
		Title:   id,
		Task:    task,
		Payload: formatPayload(task.Payload),
	})
}

// formatPayload renders a payload for display: pretty-printed JSON when it
// parses, otherwise the raw string.
func formatPayload(p []byte) string {
	var buf bytes.Buffer
	if json.Valid(p) {
		if err := json.Indent(&buf, p, "", "  "); err == nil {
			return buf.String()
		}
	}
	return string(p)
}
```

`handlers.go` import에 `"bytes"`, `"encoding/json"` 추가.

- [ ] **Step 4: 통과 확인**

Run: `cd contrib/webui && go test ./... -run TestTaskDetail_ShowsPayloadAndError`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add contrib/webui/handlers.go contrib/webui/webui_test.go
git commit -m "feat: webui 태스크 상세 (payload 포맷 + LastErr 표시)"
```

---

## Task 8: run/delete 액션 (PRG) + 메서드 가드

**Files:**
- Modify: `contrib/webui/handlers.go` (runTask/deleteTask 스텁 교체)
- Test: `contrib/webui/webui_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`webui_test.go`에 추가:

```go
func TestRunTask_RedirectsAndPromotes(t *testing.T) {
	client := newTestRedis(t)
	id := seedScheduled(t, client)
	insp := chronos.NewInspector(client)

	srv := httptest.NewServer(Handler(insp))
	defer srv.Close()

	// 리다이렉트를 따라가지 않는 클라이언트로 303을 직접 확인.
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Post(srv.URL+"/queues/default/tasks/"+id+"/run", "", nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303", resp.StatusCode)
	}
	// 승격되면 scheduled에서 사라진다.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tasks, _ := insp.ListTasks(context.Background(), "default", "scheduled", 10)
		if len(tasks) == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("task still in scheduled after run")
}

func TestDeleteTask_RemovesTask(t *testing.T) {
	client := newTestRedis(t)
	id := seedScheduled(t, client)
	insp := chronos.NewInspector(client)

	srv := httptest.NewServer(Handler(insp))
	defer srv.Close()

	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Post(srv.URL+"/queues/default/tasks/"+id+"/delete", "", nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303", resp.StatusCode)
	}
	if _, err := insp.GetTask(context.Background(), "default", id); err == nil {
		t.Error("task still present after delete")
	}
}

func TestRunTask_RejectsGet(t *testing.T) {
	client := newTestRedis(t)
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/queues/default/tasks/x/run")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}
```

> 405 검증: `http.ServeMux`의 `POST /...` 패턴은 GET 요청에 자동으로 405를 반환하므로 별도 코드 불필요.

- [ ] **Step 2: 실패 확인**

Run: `cd contrib/webui && go test ./... -run 'TestRunTask_RedirectsAndPromotes|TestDeleteTask_RemovesTask'`
Expected: FAIL — 응답이 501/"todo" (`TestRunTask_RejectsGet`는 이미 통과할 수 있음).

- [ ] **Step 3: 구현**

`handlers.go`의 두 스텁 교체:

```go
func (s *server) runTask(w http.ResponseWriter, r *http.Request) {
	s.action(w, r, s.insp.RunTask, "queued for immediate run")
}

func (s *server) deleteTask(w http.ResponseWriter, r *http.Request) {
	s.action(w, r, s.insp.DeleteTask, "deleted")
}

// action runs a mutating Inspector call then redirects (PRG) back to the queue.
func (s *server) action(w http.ResponseWriter, r *http.Request, fn func(ctx, string, string) error, okMsg string) {
	queue := r.PathValue("queue")
	id := r.PathValue("id")
	msg := okMsg
	if err := fn(r.Context(), queue, id); err != nil {
		msg = "error: " + err.Error()
	}
	http.Redirect(w, r, "/queues/"+queue+"?msg="+url.QueryEscape(msg), http.StatusSeeOther)
}
```

`action`의 `fn` 타입이 컴파일되도록 파일 상단에 별칭을 추가하고 import에 `"context"`, `"net/url"` 추가:

```go
type ctx = context.Context
```

> `RunTask`/`DeleteTask` 시그니처는 `func(context.Context, string, string) error`이므로 `fn func(ctx, string, string) error`와 일치한다.

- [ ] **Step 4: 통과 확인**

Run: `cd contrib/webui && go test ./... -run 'TestRunTask|TestDeleteTask'`
Expected: 3개 모두 PASS

- [ ] **Step 5: webui 모듈 전체 테스트**

Run: `cd contrib/webui && go test ./... -race`
Expected: 전부 PASS

- [ ] **Step 6: 커밋**

```bash
git add contrib/webui/handlers.go contrib/webui/webui_test.go
git commit -m "feat: webui run/delete 액션 (PRG 리다이렉트 + 메서드 가드)"
```

---

## Task 9: `cmd/webui` 엔트리포인트 (플래그 + 브라우저 오픈)

**Files:**
- Create: `contrib/webui/cmd/webui/main.go`

**참고:** main은 자동 테스트 대상이 아니다(수동 확인). 컴파일 + graceful shutdown이 핵심.

- [ ] **Step 1: main.go 작성**

`contrib/webui/cmd/webui/main.go`:

```go
// Command webui runs the chronos-go task-management console.
//
//	go run ./cmd/webui --db 15
//
// It binds 127.0.0.1:8080 by default and opens your browser. To expose it
// remotely, set --addr 0.0.0.0:8080 and put it behind an authenticating
// reverse proxy — the console performs destructive actions (run/delete) and
// ships no authentication of its own.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
	"github.com/kenshin579/chronos-go/contrib/webui"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "Redis address")
	db := flag.Int("db", 0, "Redis logical database")
	noOpen := flag.Bool("no-open", false, "do not open a browser on start")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, DB: *db})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("cannot reach Redis at %s (db %d): %v", *redisAddr, *db, err)
	}
	defer rdb.Close()

	srv := &http.Server{Addr: *addr, Handler: webui.Handler(chronos.NewInspector(rdb))}

	go func() {
		log.Printf("chronos-go console on http://%s (redis %s db %d)", *addr, *redisAddr, *db)
		if !*noOpen {
			openBrowser("http://" + *addr)
		}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// openBrowser best-effort opens url in the default browser; failures are logged.
func openBrowser(url string) {
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		cmd = "open"
	case "windows":
		cmd, args = "rundll32", []string{"url.dll,FileProtocolHandler"}
	default:
		cmd = "xdg-open"
	}
	if err := exec.Command(cmd, append(args, url)...).Start(); err != nil {
		log.Printf("could not open browser (%v); visit %s manually", err, url)
	}
}
```

- [ ] **Step 2: 빌드 + go vet 확인**

Run: `cd contrib/webui && go mod tidy && go build ./... && go vet ./...`
Expected: 에러 없음.

- [ ] **Step 3: 수동 눈 확인 (선택, 강력 권장)**

```bash
# 다른 터미널에서 데이터 생성 (DB 15)
REDIS_ADDR=127.0.0.1:6379 go run ./examples/tour   # 코어 모듈 루트에서
# 콘솔 실행
cd contrib/webui && go run ./cmd/webui --db 15
```
브라우저에서 대시보드 → archived 큐 → dead-letter 클릭 → 에러/payload 확인 → Re-run/Delete 동작 확인.

- [ ] **Step 4: 커밋**

```bash
git add contrib/webui/cmd contrib/webui/go.mod contrib/webui/go.sum
git commit -m "feat: cmd/webui 엔트리포인트 (플래그 + 브라우저 자동 오픈 + graceful shutdown)"
```

---

## Task 10: 문서 + CI

**Files:**
- Create: `contrib/webui/README.md`
- Modify: `README.md` (루트, Observability 섹션)
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: contrib/webui/README.md 작성**

```markdown
# chronos-go web console

A browser-based **task-management console** for chronos-go, backed entirely by
the public `chronos.Inspector`. It complements Grafana (metrics) and the CLI:
open a dead-lettered task, read its payload and the error that killed it, then
re-run or delete it.

## Run

```bash
go run ./cmd/webui --db 0
```

Flags: `--addr` (default `127.0.0.1:8080`), `--redis` (default
`127.0.0.1:6379`), `--db` (default `0`), `--no-open` (skip opening a browser).

## Try it with sample data

```bash
# in the repo root, generate tasks (incl. dead-letters) on DB 15
go run ./examples/tour
# then point the console at the same DB
cd contrib/webui && go run ./cmd/webui --db 15
```

Open the dashboard, click a queue's **archived** count, open a task, and use
**Re-run now** / **Delete**.

## Security

The console binds `127.0.0.1` by default and **ships no authentication**. Its
actions (re-run, delete) are destructive. To expose it beyond localhost, set
`--addr 0.0.0.0:8080` **and put it behind an authenticating reverse proxy**
(nginx, oauth2-proxy, Cloudflare Access, …). Do not expose it directly.

## Mounting in your own server

```go
mux := http.NewServeMux()
mux.Handle("/chronos/", http.StripPrefix("/chronos", webui.Handler(insp)))
```
```

- [ ] **Step 2: 루트 README에 한 줄 추가**

`README.md`의 Observability 섹션에서 Prometheus 항목 뒤에 추가:

```markdown
- **Web console** — a browser task-management UI (inspect dead-letters, re-run /
  delete) in [`contrib/webui`](contrib/webui):
  ```bash
  cd contrib/webui && go run ./cmd/webui
  ```
```

- [ ] **Step 3: CI에 webui 테스트 스텝 추가**

`.github/workflows/ci.yml`의 `Test (contrib/prometheus)` 스텝 바로 아래에 추가:

```yaml
      - name: Test (contrib/webui)
        run: cd contrib/webui && go test ./... -race
```

- [ ] **Step 4: 문서/CI 검증**

Run: `cd contrib/webui && go test ./... -race` 그리고 루트에서 `make check`
Expected: 전부 PASS.

- [ ] **Step 5: 커밋**

```bash
git add contrib/webui/README.md README.md .github/workflows/ci.yml
git commit -m "docs: webui README + 루트 README + CI 스텝"
```

---

## Task 11: 최종 통합 검증 + 코드 리뷰

- [ ] **Step 1: 코어 + webui 전체 검증**

```bash
make check
cd contrib/webui && go test ./... -race && go vet ./... && gofmt -l .
```
Expected: 전부 PASS, gofmt 출력 없음.

- [ ] **Step 2: Makefile `check`가 webui를 포함하는지 확인 후 필요 시 추가**

`make check`가 contrib/prometheus만 돌리고 webui는 빠져 있으면 Makefile에 webui 테스트를 추가한다(선택). CI에는 Task 10에서 이미 추가됨.

- [ ] **Step 3: 코드 리뷰**

k:code-reviewer 서브에이전트로 uncommitted가 아닌 브랜치 전체 diff를 리뷰. 특히:
- 코어 변경이 기존 소비자(CLI, OnDeadLetter)를 깨지 않는지 (TaskInfo 필드 추가는 하위호환).
- `render`가 요청마다 ParseFS 하는 비용 — 콘솔은 저빈도이므로 허용, 지적 시 캐시화 고려.
- 템플릿 XSS: `html/template` 자동 이스케이프에 의존(payload/에러) — 안전 확인.
- webui가 코어 internal에 의존하지 않는지 (공개 Inspector만).

- [ ] **Step 4: 리뷰 반영 후 PR**

```bash
gh pr create --assignee kenshin579 --title "feat: Web UI 태스크 관리 콘솔 (contrib/webui)" --body "$(cat <<'EOF'
## 배경
dead-letter를 브라우저에서 열어 payload/에러 확인 후 재실행/삭제하는 태스크 관리 콘솔. 코어 의존성 0 유지(contrib/webui nested 모듈).

## 변경
- 코어: TaskMessage.LastErr(마지막 실패 에러, Lua 무변경 저장), Inspector.GetTask + TaskInfo 확장(payload/retried/error/시각), ListTasks가 score(시각) 반영.
- contrib/webui: html/template+embed HTTP 서버(대시보드/큐 상세/태스크 상세 + run/delete PRG), cmd/webui(플래그+브라우저 오픈), README(보안: localhost 기본+프록시 위임).

## 테스트 계획
- [x] make check (코어 -race -p 1)
- [x] contrib/webui go test -race (httptest + 실제 Redis)
- [x] go run ./cmd/webui 수동 눈 확인

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review (계획 작성자 확인 완료)

- **스펙 커버리지**: LastErr 저장(T1,T4) / TaskInfo·GetTask·ListTasks(T2,T3) / webui 4화면(T5~T7) / 액션 PRG+405(T8) / cmd 플래그·브라우저(T9) / 보안·관찰 README·CI(T10) / 리뷰·PR(T11). 스펙 전 섹션 매핑됨.
- **placeholder**: 각 스텝에 실제 코드/명령/기대출력 포함. `errorsNew` 우회는 대체 지침을 명시.
- **타입 일관성**: `ListZSetTasks → []*ZSetTask{Msg,Score}`(T2)를 T3에서 `e.Msg`/`e.Score`로 사용, `TaskInfo` 필드명(State/Payload/Retried/MaxRetry/LastErr/NextProcessAt)이 T3 정의와 템플릿·핸들러에서 일치, `Handler`/`server`/핸들러 시그니처가 T5~T8에서 일관.
- **알려진 한계**: recoverer 경로 dead-letter는 LastErr 미기록(T4에 명시) — 스펙의 한계와 일치.
