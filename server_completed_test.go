package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_WithRetentionKeepsCompletedTask(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(done)
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "keep"}, WithRetention(time.Hour))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-done

	insp := NewInspector(client)
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, gerr := insp.GetTask(ctx, "default", info.ID)
		if gerr == nil && got.State == "completed" {
			if got.CompletedAt.IsZero() {
				t.Error("CompletedAt is zero, want completion time")
			}
			if got.NextProcessAt.IsZero() {
				t.Error("NextProcessAt (expiry) is zero")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task not in completed state in time (last err=%v)", gerr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestServer_UniqueLockReleasedDespiteRetention(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	done := make(chan struct{}, 2)
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		done <- struct{}{}
		return nil
	})
	srv := NewServer(client, ServerConfig{Queues: map[string]int{"default": 1}, Concurrency: 1})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	if _, err := Enqueue(ctx, c, emailArgs{UserID: "uq"}, WithUnique(time.Hour), WithRetention(time.Hour)); err != nil {
		t.Fatalf("enqueue1: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("first task not processed")
	}
	// 완료 직후: 보관 중이어도 unique 락은 해제 → 동일 태스크 enqueue 가능해야 한다.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := Enqueue(ctx, c, emailArgs{UserID: "uq"}, WithUnique(time.Hour), WithRetention(time.Hour))
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("re-enqueue still blocked: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestServer_JanitorTrimsExpiredCompleted(t *testing.T) {
	client := testutil.NewRedis(t)
	c := NewClient(client)
	defer c.Close()
	ctx := context.Background()

	done := make(chan struct{})
	mux := NewMux()
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		close(done)
		return nil
	})
	srv := NewServer(client, ServerConfig{
		Queues:          map[string]int{"default": 1},
		Concurrency:     1,
		JanitorInterval: 200 * time.Millisecond,
	})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	info, err := Enqueue(ctx, c, emailArgs{UserID: "short"}, WithRetention(time.Second))
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	<-done

	insp := NewInspector(client)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, gerr := insp.GetTask(ctx, "default", info.ID); gerr != nil {
			return // trimmed
		}
		if time.Now().After(deadline) {
			t.Fatal("completed task not trimmed by janitor in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
