package rdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func uniqueMsg(id, queue string) *base.TaskMessage {
	m := &base.TaskMessage{ID: id, Kind: "k", Payload: []byte(`{"a":1}`), Queue: queue}
	m.UniqueKey = base.UniqueKey(queue, base.UniqueSuffix(m.Kind, m.Payload))
	return m
}

func TestEnqueueUnique_SecondIsRejected(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	first := uniqueMsg("t1", "default")
	if err := r.EnqueueUnique(ctx, first, time.Minute); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	// Same kind+payload → same unique key → rejected while the lock is held.
	second := uniqueMsg("t2", "default")
	err := r.EnqueueUnique(ctx, second, time.Minute)
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second enqueue err = %v, want ErrDuplicateTask", err)
	}

	// Only the first task is in the stream; the lock stores the first task's ID.
	if slen, _ := client.XLen(ctx, base.StreamKey("default")).Result(); slen != 1 {
		t.Errorf("stream len = %d, want 1", slen)
	}
	val, _ := client.Get(ctx, first.UniqueKey).Result()
	if val != "t1" {
		t.Errorf("unique lock value = %q, want t1", val)
	}
}

func TestScheduleUnique_SecondIsRejected(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	first := uniqueMsg("t1", "default")
	if err := r.ScheduleUnique(ctx, first, time.Now().Add(time.Hour), time.Minute); err != nil {
		t.Fatalf("first schedule: %v", err)
	}
	second := uniqueMsg("t2", "default")
	if err := r.ScheduleUnique(ctx, second, time.Now().Add(time.Hour), time.Minute); !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("second schedule err = %v, want ErrDuplicateTask", err)
	}
	if _, err := client.ZScore(ctx, base.ScheduledKey("default"), "t1").Result(); err != nil {
		t.Errorf("first task not scheduled: %v", err)
	}
}

func TestDone_ReleasesUniqueLock(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	msg := uniqueMsg("t1", "default")
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := r.EnqueueUnique(ctx, msg, time.Minute); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, streamID, err := r.Dequeue(ctx, "c1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	if err := r.Done(ctx, "default", streamID, got); err != nil {
		t.Fatalf("done: %v", err)
	}
	// Lock released → a new identical task can be enqueued.
	if exists, _ := client.Exists(ctx, msg.UniqueKey).Result(); exists != 0 {
		t.Error("unique lock should be released after Done")
	}
}

func TestRetry_KeepsUniqueLock_ArchiveReleases(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()
	if err := r.EnsureGroup(ctx, "default"); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	// Retry keeps the lock (task still in flight).
	msg := uniqueMsg("t1", "default")
	msg.MaxRetry = 5
	if err := r.EnqueueUnique(ctx, msg, time.Minute); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	got, streamID, _ := r.Dequeue(ctx, "c1", 0, "default")
	got.Retried = 1
	if err := r.Retry(ctx, "default", streamID, got, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if exists, _ := client.Exists(ctx, msg.UniqueKey).Result(); exists != 1 {
		t.Error("unique lock must be kept across a retry")
	}

	// Archive releases the lock (terminal).
	// Bring it back to the stream and dequeue to get a fresh streamID.
	if _, err := r.ForwardRetry(ctx, "default", time.Now().Add(2*time.Hour), 10); err != nil {
		t.Fatalf("forward: %v", err)
	}
	got2, streamID2, err := r.Dequeue(ctx, "c1", 0, "default")
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if err := r.Archive(ctx, "default", streamID2, got2, time.Now()); err != nil {
		t.Fatalf("archive: %v", err)
	}
	if exists, _ := client.Exists(ctx, msg.UniqueKey).Result(); exists != 0 {
		t.Error("unique lock should be released after Archive")
	}
}
