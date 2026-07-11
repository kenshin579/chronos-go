package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestBuildClient_ModesAndErrors(t *testing.T) {
	// 기본(둘 다 false) = standalone
	c, err := buildClient(false, false, "127.0.0.1:6379", 3)
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	if _, ok := c.(*redis.Client); !ok {
		t.Errorf("default: got %T, want *redis.Client", c)
	}
	_ = c.Close()

	// 명시적 standalone
	c, err = buildClient(true, false, "127.0.0.1:6379", 0)
	if err != nil {
		t.Fatalf("standalone: %v", err)
	}
	if _, ok := c.(*redis.Client); !ok {
		t.Errorf("standalone: got %T, want *redis.Client", c)
	}
	_ = c.Close()

	// cluster: 콤마 분리 다중 주소
	c, err = buildClient(false, true, "n1:7000,n2:7001", 0)
	if err != nil {
		t.Fatalf("cluster: %v", err)
	}
	cc, ok := c.(*redis.ClusterClient)
	if !ok {
		t.Fatalf("cluster: got %T, want *redis.ClusterClient", c)
	}
	_ = cc.Close()

	// 콤마 뒤 공백/빈 조각 허용
	c, err = buildClient(false, true, " n1:7000 , n2:7001 ,", 0)
	if err != nil {
		t.Fatalf("cluster with spaces: %v", err)
	}
	_ = c.Close()
	// 빈 주소 목록은 에러
	if _, err := buildClient(false, true, " , ", 0); err == nil {
		t.Error("cluster with empty addrs: want error, got nil")
	}

	// 상호 배타
	if _, err := buildClient(true, true, "x:1", 0); err == nil {
		t.Error("standalone+cluster: want error, got nil")
	}
	// cluster + db != 0
	if _, err := buildClient(false, true, "x:1", 15); err == nil {
		t.Error("cluster with db!=0: want error, got nil")
	}
}

func TestRun_QueueLs(t *testing.T) {
	client := testutil.NewRedis(t)
	c := chronos.NewClient(client)
	defer c.Close()
	if _, err := chronos.Enqueue(context.Background(), c, greetArgs{Name: "x"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var out bytes.Buffer
	code := run([]string{"queue", "ls"}, client, &out)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; out=%s", code, out.String())
	}
	if !strings.Contains(out.String(), "default") {
		t.Errorf("output missing queue name; got:\n%s", out.String())
	}
}

func TestRun_UnknownCommand(t *testing.T) {
	client := testutil.NewRedis(t)
	var out bytes.Buffer
	if code := run([]string{"bogus"}, client, &out); code == 0 {
		t.Error("unknown command should return non-zero")
	}
}

type greetArgs struct {
	Name string `json:"name"`
}

func (greetArgs) Kind() string { return "cli:greet" }
