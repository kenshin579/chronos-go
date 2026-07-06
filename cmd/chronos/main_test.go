package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/kenshin579/chronos-go"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

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
