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
