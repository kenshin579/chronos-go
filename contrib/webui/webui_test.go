package webui

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

var errDemo = errors.New("demo failure")

// seedDeadLetter runs a failing handler once so the task is archived with a
// LastErr, and returns its task ID.
func seedDeadLetter(t *testing.T, client redis.UniversalClient) string {
	t.Helper()
	return seedDeadLetters(t, client, 1)[0]
}

// seedDeadLetters runs a failing handler until n tasks are archived, returning
// their IDs.
func seedDeadLetters(t *testing.T, client redis.UniversalClient, n int) []string {
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
		Concurrency: 2,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		info, err := chronos.Enqueue(ctx, c, demoArgs{Msg: "boom"}, chronos.WithMaxRetry(0))
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		ids = append(ids, info.ID)
	}
	insp := chronos.NewInspector(client)
	deadline := time.Now().Add(10 * time.Second)
	for _, id := range ids {
		for {
			got, gerr := insp.GetTask(ctx, "default", id)
			if gerr == nil && got.State == "archived" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("task %s did not reach archived state in time", id)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return ids
}

func TestDashboard_CardGridWithWarning(t *testing.T) {
	client := newTestRedis(t)
	seedDeadLetter(t, client) // archived 1건 → 경고 카드

	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	body := readBody(t, resp)
	if !strings.Contains(body, `qcard warn`) {
		t.Errorf("dashboard missing warning card:\n%s", body)
	}
	if !strings.Contains(body, `data-theme`) {
		t.Errorf("layout missing theme attribute")
	}
	if !strings.Contains(body, `schedbar`) {
		t.Errorf("dashboard missing scheduler bar")
	}
}

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

func TestRunTask_RedirectsAndPromotes(t *testing.T) {
	client := newTestRedis(t)
	id := seedScheduled(t, client)
	insp := chronos.NewInspector(client)

	srv := httptest.NewServer(Handler(insp))
	defer srv.Close()

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

func TestAction_RejectsCrossOrigin(t *testing.T) {
	client := newTestRedis(t)
	id := seedScheduled(t, client)
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/queues/default/tasks/"+id+"/delete", nil)
	req.Header.Set("Origin", "http://evil.example.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	// task must still exist (delete rejected)
	insp := chronos.NewInspector(client)
	if _, err := insp.GetTask(context.Background(), "default", id); err != nil {
		t.Errorf("task was deleted despite cross-origin rejection: %v", err)
	}
}

func TestTaskDetail_MissingReturns404(t *testing.T) {
	client := newTestRedis(t)
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/queues/default/tasks/nope")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func mustGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	return resp
}

func TestTaskDetail_ChainStepper(t *testing.T) {
	client := newTestRedis(t)
	c := chronos.NewClient(client)
	defer c.Close()
	ctx := context.Background()
	info, err := chronos.NewChain().
		Then(demoArgs{Msg: "a"}, chronos.WithProcessIn(time.Hour)).
		Then(demoArgs{Msg: "b"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("chain: %v", err)
	}
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	resp := mustGet(t, srv.URL+"/queues/default/tasks/"+info.ID)
	defer resp.Body.Close()
	body := readBody(t, resp)
	if !strings.Contains(body, `class="stepper"`) {
		t.Errorf("missing stepper:\n%s", body)
	}
	if !strings.Contains(body, "step 1 of 2") {
		t.Errorf("missing position label")
	}
	if !strings.Contains(body, "resumes the chain") {
		t.Errorf("missing resume label on re-run button")
	}
}

func TestTaskDetail_GroupGrid(t *testing.T) {
	client := newTestRedis(t)
	c := chronos.NewClient(client)
	defer c.Close()
	ctx := context.Background()
	ginfo, err := chronos.NewGroup().
		Add(demoArgs{Msg: "m0"}, chronos.WithProcessIn(time.Hour)).
		Add(demoArgs{Msg: "m1"}, chronos.WithProcessIn(time.Hour)).
		OnComplete(demoArgs{Msg: "cb"}).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatalf("group: %v", err)
	}
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	resp := mustGet(t, srv.URL+"/queues/default/tasks/"+ginfo.MemberIDs[0])
	defer resp.Body.Close()
	body := readBody(t, resp)
	if !strings.Contains(body, `class="member-grid"`) {
		t.Errorf("missing member grid:\n%s", body)
	}
	if !strings.Contains(body, "2 member(s) still pending") {
		t.Errorf("missing pending count")
	}
}

func TestQueueDetail_KindFilterAndIcons(t *testing.T) {
	client := newTestRedis(t)
	c := chronos.NewClient(client)
	defer c.Close()
	ctx := context.Background()
	if _, err := chronos.NewChain().
		Then(demoArgs{Msg: "x"}, chronos.WithProcessIn(time.Hour)).
		Then(demoArgs{Msg: "y"}).
		Enqueue(ctx, c); err != nil {
		t.Fatalf("chain: %v", err)
	}
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	body := readBody(t, mustGet(t, srv.URL+"/queues/default?state=scheduled"))
	if !strings.Contains(body, "🔗") {
		t.Errorf("missing chain icon")
	}
	body = readBody(t, mustGet(t, srv.URL+"/queues/default?state=scheduled&kind=nope"))
	if strings.Contains(body, "🔗") {
		t.Errorf("kind filter did not exclude")
	}
}

func TestAPIStats_JSON(t *testing.T) {
	client := newTestRedis(t)
	seedScheduled(t, client)
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	resp := mustGet(t, srv.URL+"/api/stats")
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %s", ct)
	}
	var payload struct {
		Queues []struct {
			Queue     string `json:"queue"`
			Pending   int64  `json:"pending"`
			Scheduled int64  `json:"scheduled"`
			Spark     string `json:"spark"`
		} `json:"queues"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(payload.Queues) == 0 || payload.Queues[0].Scheduled != 1 {
		t.Errorf("payload = %+v", payload)
	}
	if !strings.Contains(payload.Queues[0].Spark, "<svg") {
		t.Errorf("spark missing svg: %q", payload.Queues[0].Spark)
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

func TestBulkRunAll_Archived(t *testing.T) {
	client := newTestRedis(t)
	seedDeadLetters(t, client, 3)
	insp := chronos.NewInspector(client)
	srv := httptest.NewServer(Handler(insp))
	defer srv.Close()

	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Post(srv.URL+"/queues/default/bulk/run?state=archived", "", nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	tasks, _ := insp.ListTasks(context.Background(), "default", "archived", 10)
	if len(tasks) != 0 {
		t.Errorf("archived not drained: %d left", len(tasks))
	}
}

func TestBulk_RejectsCrossOrigin(t *testing.T) {
	client := newTestRedis(t)
	seedDeadLetters(t, client, 1)
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/queues/default/bulk/delete?state=archived", nil)
	req.Header.Set("Origin", "http://evil.example.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestSearch_RedirectsToTask(t *testing.T) {
	client := newTestRedis(t)
	id := seedScheduled(t, client)
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Get(srv.URL + "/search?id=" + id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); !strings.Contains(loc, "/tasks/"+id) {
		t.Errorf("location = %s", loc)
	}
}

func TestSchedulerPage(t *testing.T) {
	client := newTestRedis(t)
	client.Set(context.Background(), "chronos:leader", "inst-9", 0)
	client.Set(context.Background(), "chronos:sched:jobx:last", 1700000000, 0)
	srv := httptest.NewServer(Handler(chronos.NewInspector(client)))
	defer srv.Close()
	body := readBody(t, mustGet(t, srv.URL+"/scheduler"))
	if !strings.Contains(body, "inst-9") || !strings.Contains(body, "jobx") {
		t.Errorf("scheduler page missing data:\n%s", body)
	}
}
