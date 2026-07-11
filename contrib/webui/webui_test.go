package webui

import (
	"context"
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
