package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestScheduleRegistry(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client)
	ctx := context.Background()

	metas := []ScheduleMeta{
		{ID: "job-a#1", Kind: "report:daily", Queue: "default", Spec: "0 0 * * *"},
		{ID: "job-b#2", Kind: "health:ping", Queue: "ops", Spec: "@every 30s"},
	}
	if err := r.RegisterSchedules(ctx, metas); err != nil {
		t.Fatalf("register: %v", err)
	}
	// 멱등 재등록 + touch.
	if err := r.RegisterSchedules(ctx, metas[:1]); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	before := time.Now().Add(-time.Minute).Unix()
	if err := r.TouchSchedules(ctx, []string{"job-a#1"}); err != nil {
		t.Fatalf("touch: %v", err)
	}
	got, err := r.ListSchedules(ctx)
	if err != nil || len(got) != 2 {
		t.Fatalf("list = %v err=%v, want 2", got, err)
	}
	byID := map[string]ScheduleMeta{}
	for _, m := range got {
		byID[m.ID] = m
	}
	a := byID["job-a#1"]
	if a.Kind != "report:daily" || a.Spec != "0 0 * * *" || a.LastSeen < before {
		t.Errorf("job-a = %+v", a)
	}
	if byID["job-b#2"].Queue != "ops" {
		t.Errorf("job-b = %+v", byID["job-b#2"])
	}
}
