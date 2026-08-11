package rdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestScheduleRegistry(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client, base.DefaultKeys)
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

func TestPruneSchedules(t *testing.T) {
	client := testutil.NewRedis(t)
	r := NewRDB(client, base.DefaultKeys)
	ctx := context.Background()

	// mine: 이 스케줄러가 지금 등록한 것. ghost: 스펙이 바뀌어 버려진 옛 ID.
	// rolling: 롤링 배포 중인 구버전 파드가 아직 touch 하고 있는 남의 ID.
	if err := r.RegisterSchedules(ctx, []ScheduleMeta{
		{ID: "mine#1", Kind: "report:daily", Queue: "default", Spec: "0 0 * * *"},
		{ID: "ghost#2", Kind: "report:daily", Queue: "default", Spec: "30 9 * * *"},
		{ID: "rolling#3", Kind: "health:ping", Queue: "ops", Spec: "@every 30s"},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	// ghost 만 오래 전에 마지막으로 보였다고 되돌린다.
	old := time.Now().Add(-10 * time.Minute).Unix()
	client.HSet(ctx, base.DefaultKeys.SchedulesKey(), "ghost#2",
		fmt.Sprintf(`{"kind":"report:daily","queue":"default","spec":"30 9 * * *","registered_at":%d,"last_seen":%d}`, old, old))
	// 발화 이력도 심어 둔다 — 함께 지워지지 않으면 inspector 가 kind="" 항목으로 되살린다.
	client.Set(ctx, base.DefaultKeys.ScheduleLastFiredKey("ghost#2"), 1700000000, 0)
	client.Set(ctx, base.DefaultKeys.ScheduleLastFiredKey("mine#1"), 1700000001, 0)

	n, err := r.PruneSchedules(ctx, []string{"mine#1"}, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 1 {
		t.Errorf("pruned = %d, want 1", n)
	}

	got, err := r.ListSchedules(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	left := map[string]bool{}
	for _, m := range got {
		left[m.ID] = true
	}
	if left["ghost#2"] {
		t.Error("stale entry not owned by the caller survived the prune")
	}
	if !left["mine#1"] {
		t.Error("caller's own entry was pruned")
	}
	// 구버전 파드가 살아 있는 동안은 남의 것이어도 지우면 안 된다.
	if !left["rolling#3"] {
		t.Error("fresh entry owned by another instance was pruned")
	}

	if n, err := client.Exists(ctx, base.DefaultKeys.ScheduleLastFiredKey("ghost#2")).Result(); err != nil || n != 0 {
		t.Errorf("ghost last-fired key = %d err=%v, want deleted", n, err)
	}
	if n, err := client.Exists(ctx, base.DefaultKeys.ScheduleLastFiredKey("mine#1")).Result(); err != nil || n != 1 {
		t.Errorf("own last-fired key = %d err=%v, want kept", n, err)
	}
}
