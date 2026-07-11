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
