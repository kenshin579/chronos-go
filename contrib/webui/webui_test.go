package webui

import (
	"context"
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
