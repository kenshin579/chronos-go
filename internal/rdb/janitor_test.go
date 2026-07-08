package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

// archiveDirect places a task hash + archived ZSET entry with a given died-at,
// bypassing the normal flow, so retention can be tested deterministically.
func archiveDirect(t *testing.T, client redis.UniversalClient, qname, id string, diedAt time.Time) {
	t.Helper()
	ctx := context.Background()
	msg := &base.TaskMessage{ID: id, Kind: "k", Payload: []byte("{}"), Queue: qname, State: base.StateArchived}
	enc, _ := base.EncodeMessage(msg)
	if err := client.HSet(ctx, base.TaskKey(qname, id), "msg", enc, "state", int(base.StateArchived)).Err(); err != nil {
		t.Fatalf("hset: %v", err)
	}
	if err := client.ZAdd(ctx, base.ArchivedKey(qname), redis.Z{Score: float64(diedAt.Unix()), Member: id}).Err(); err != nil {
		t.Fatalf("zadd: %v", err)
	}
}

func TestTrimArchived_DeletesExpiredByAge(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	now := time.Now()
	archiveDirect(t, client, "default", "old", now.Add(-2*time.Hour))     // expired
	archiveDirect(t, client, "default", "fresh", now.Add(-1*time.Minute)) // within retention

	cutoff := now.Add(-1 * time.Hour) // older than 1h → delete
	n, err := r.TrimArchived(ctx, "default", cutoff, 10000, 100)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if n != 1 {
		t.Errorf("trimmed = %d, want 1", n)
	}
	// old gone (ZSET + hash), fresh kept.
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "old").Result(); err == nil {
		t.Error("expired 'old' should be removed from archived")
	}
	if ex, _ := client.Exists(ctx, base.TaskKey("default", "old")).Result(); ex != 0 {
		t.Error("expired 'old' task hash should be deleted")
	}
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "fresh").Result(); err != nil {
		t.Error("fresh task should be kept")
	}
}

func TestTrimArchived_EnforcesMaxSize(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	now := time.Now()
	// 5 fresh tasks (none age-expired), staggered died-at so "oldest" is well-defined.
	for i := 0; i < 5; i++ {
		archiveDirect(t, client, "default", string(rune('a'+i)), now.Add(time.Duration(i)*time.Second))
	}
	// cutoff far in the past (nothing age-expired); maxSize=2 → keep 2 newest, delete 3 oldest.
	n, err := r.TrimArchived(ctx, "default", now.Add(-24*time.Hour), 2, 100)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if n != 3 {
		t.Errorf("trimmed = %d, want 3 (over max)", n)
	}
	card, _ := client.ZCard(ctx, base.ArchivedKey("default")).Result()
	if card != 2 {
		t.Errorf("archived size = %d, want 2", card)
	}
	// The 2 newest (d, e) must remain.
	if _, err := client.ZScore(ctx, base.ArchivedKey("default"), "e").Result(); err != nil {
		t.Error("newest 'e' should be kept")
	}
}
