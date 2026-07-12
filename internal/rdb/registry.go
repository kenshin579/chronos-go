package rdb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kenshin579/chronos-go/internal/base"
)

// ScheduleMeta is one registered schedule's registry entry.
type ScheduleMeta struct {
	ID           string `json:"-"` // hash field, not part of the JSON value
	Kind         string `json:"kind"`
	Queue        string `json:"queue"`
	Spec         string `json:"spec"`
	RegisteredAt int64  `json:"registered_at"` // unix seconds (last registration)
	LastSeen     int64  `json:"last_seen"`     // unix seconds (scheduler heartbeat)
}

// RegisterSchedules upserts the given schedules into the registry. Schedule
// IDs are deterministic, so concurrent registration from multiple instances
// overwrites identical data — idempotent by construction.
func (r *RDB) RegisterSchedules(ctx context.Context, metas []ScheduleMeta) error {
	if len(metas) == 0 {
		return nil
	}
	now := time.Now().Unix()
	pairs := make([]interface{}, 0, len(metas)*2)
	for _, m := range metas {
		m.RegisteredAt, m.LastSeen = now, now
		v, err := json.Marshal(m)
		if err != nil {
			return err
		}
		pairs = append(pairs, m.ID, string(v))
	}
	return r.client.HSet(ctx, base.SchedulesKey(), pairs...).Err()
}

// TouchSchedules refreshes last_seen for the given schedule IDs (scheduler
// heartbeat). Unknown IDs are skipped.
func (r *RDB) TouchSchedules(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	vals, err := r.client.HMGet(ctx, base.SchedulesKey(), ids...).Result()
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	pairs := make([]interface{}, 0, len(ids)*2)
	for i, raw := range vals {
		s, ok := raw.(string)
		if !ok {
			continue
		}
		var m ScheduleMeta
		if json.Unmarshal([]byte(s), &m) != nil {
			continue
		}
		m.LastSeen = now
		v, _ := json.Marshal(m)
		pairs = append(pairs, ids[i], string(v))
	}
	if len(pairs) == 0 {
		return nil
	}
	return r.client.HSet(ctx, base.SchedulesKey(), pairs...).Err()
}

// ListSchedules returns every registry entry.
func (r *RDB) ListSchedules(ctx context.Context) ([]ScheduleMeta, error) {
	all, err := r.client.HGetAll(ctx, base.SchedulesKey()).Result()
	if err != nil {
		return nil, err
	}
	out := make([]ScheduleMeta, 0, len(all))
	for id, raw := range all {
		var m ScheduleMeta
		if json.Unmarshal([]byte(raw), &m) != nil {
			continue // foreign value shape; skip
		}
		m.ID = id
		out = append(out, m)
	}
	return out, nil
}
