package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestLeadership_SingleWinnerThenRenew(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	// A acquires; B loses while A holds.
	okA, err := r.AcquireOrRenewLeadership(ctx, "A", time.Second)
	if err != nil || !okA {
		t.Fatalf("A acquire: ok=%v err=%v", okA, err)
	}
	okB, err := r.AcquireOrRenewLeadership(ctx, "B", time.Second)
	if err != nil {
		t.Fatalf("B: %v", err)
	}
	if okB {
		t.Error("B should not become leader while A holds")
	}
	// A renews (still leader).
	okA2, _ := r.AcquireOrRenewLeadership(ctx, "A", time.Second)
	if !okA2 {
		t.Error("A should renew and stay leader")
	}
}

func TestLeadership_FailoverAfterExpiry(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	if ok, _ := r.AcquireOrRenewLeadership(ctx, "A", 200*time.Millisecond); !ok {
		t.Fatal("A acquire")
	}
	time.Sleep(300 * time.Millisecond) // let A's lock expire (A "died")
	if ok, _ := r.AcquireOrRenewLeadership(ctx, "B", time.Second); !ok {
		t.Error("B should take over after A's lock expires")
	}
}

func TestResignLeadership_ReleasesForOthers(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	r.AcquireOrRenewLeadership(ctx, "A", time.Minute)
	if err := r.ResignLeadership(ctx, "A"); err != nil {
		t.Fatalf("resign: %v", err)
	}
	// B can immediately acquire after A resigns.
	if ok, _ := r.AcquireOrRenewLeadership(ctx, "B", time.Minute); !ok {
		t.Error("B should acquire immediately after A resigns")
	}
	// A resigning again (not owner) is a no-op, not an error.
	if err := r.ResignLeadership(ctx, "A"); err != nil {
		t.Fatalf("resign non-owner: %v", err)
	}
	if ok, _ := r.AcquireOrRenewLeadership(ctx, "C", time.Minute); ok {
		t.Error("A's second resign must not have released B's lock")
	}
}
