package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

// mkGroupMember returns a member message reporting to group "g" whose set (and
// callback) lives on queue "cbq".
func mkGroupMember(id string) *base.TaskMessage {
	return &base.TaskMessage{
		ID: id, Kind: "m", Queue: "default",
		GroupID: "g", GroupQueue: "cbq",
		GroupCallback: &base.ChainLink{Kind: "cb", Payload: []byte(`{}`), Queue: "cbq", MaxRetry: 25},
	}
}

func TestGroup_CompleteMembersFiresCallbackOnce(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.CreateGroup(ctx, "cbq", "g", []string{"g:m0", "g:m1"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if n, _ := r.GroupPending(ctx, "cbq", "g"); n != 2 {
		t.Fatalf("pending = %d, want 2", n)
	}

	// 첫 멤버 완료: 콜백 미발화.
	fired, err := r.CompleteGroupMember(ctx, mkGroupMember("g:m0"))
	if err != nil {
		t.Fatalf("m0: %v", err)
	}
	if fired {
		t.Fatal("callback fired after first member")
	}
	// 같은 멤버 재보고(재전달): 멱등 no-op.
	if fired, _ = r.CompleteGroupMember(ctx, mkGroupMember("g:m0")); fired {
		t.Fatal("duplicate report fired callback")
	}
	if n, _ := r.GroupPending(ctx, "cbq", "g"); n != 1 {
		t.Fatalf("pending after dup = %d, want 1", n)
	}

	// 마지막 멤버: 콜백 발화 + SET 삭제.
	fired, err = r.CompleteGroupMember(ctx, mkGroupMember("g:m1"))
	if err != nil {
		t.Fatalf("m1: %v", err)
	}
	if !fired {
		t.Fatal("callback did not fire on last member")
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("cbq")).Result(); xlen != 1 {
		t.Errorf("callback stream len = %d, want 1", xlen)
	}
	if _, err := r.GetTask(ctx, "cbq", "g:cb"); err != nil {
		t.Errorf("callback task hash missing: %v", err)
	}
	if n, _ := r.GroupPending(ctx, "cbq", "g"); n != 0 {
		t.Errorf("pending after done = %d, want 0 (set deleted)", n)
	}

	// SET 소멸 후 뒤늦은 재보고: no-op, 콜백 불변.
	if fired, _ = r.CompleteGroupMember(ctx, mkGroupMember("g:m1")); fired {
		t.Error("late report after group done fired callback again")
	}
	if xlen, _ := client.XLen(ctx, base.StreamKey("cbq")).Result(); xlen != 1 {
		t.Errorf("callback duplicated: stream len = %d, want 1", xlen)
	}
}

func TestGroup_DelayedCallbackGoesToScheduled(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.CreateGroup(ctx, "cbq", "gd", []string{"gd:m0"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	m := &base.TaskMessage{
		ID: "gd:m0", Kind: "m", Queue: "default",
		GroupID: "gd", GroupQueue: "cbq",
		GroupCallback: &base.ChainLink{Kind: "cb", Payload: []byte(`{}`), Queue: "cbq", MaxRetry: 25, Delay: 2},
	}
	fired, err := r.CompleteGroupMember(ctx, m)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !fired {
		t.Fatal("callback not fired")
	}
	score, err := client.ZScore(ctx, base.ScheduledKey("cbq"), "gd:cb").Result()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	want := float64(time.Now().Add(2 * time.Second).Unix())
	if score < want-5 || score > want+5 {
		t.Errorf("score = %v, want ~%v", score, want)
	}
}

func TestGroup_SetHasTTL(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if err := r.CreateGroup(ctx, "cbq", "gt", []string{"gt:m0"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	ttl, err := client.TTL(ctx, base.GroupKey("cbq", "gt")).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= 0 || ttl > GroupTTL {
		t.Errorf("ttl = %v, want (0, %v]", ttl, GroupTTL)
	}
}
