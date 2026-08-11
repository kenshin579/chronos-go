package rdb

import (
	"context"
	"encoding/json"
	"time"
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
	return r.client.HSet(ctx, r.keys.SchedulesKey(), pairs...).Err()
}

// TouchSchedules refreshes last_seen for the given schedule IDs (scheduler
// heartbeat). Unknown IDs are skipped.
func (r *RDB) TouchSchedules(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	vals, err := r.client.HMGet(ctx, r.keys.SchedulesKey(), ids...).Result()
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
	return r.client.HSet(ctx, r.keys.SchedulesKey(), pairs...).Err()
}

// PruneSchedules removes registry entries the caller no longer registers and
// that nothing has refreshed since staleBefore, together with their last-fired
// keys. It returns how many entries were removed.
//
// Both conditions are required. A schedule ID embeds its spec, so changing a
// cron's spec mints a new ID and strands the old entry: nothing touches or
// deletes it again, it goes stale a minute later, and it pins the stale gauge to
// 1 for good. Pruning on "not mine" alone would instead delete a schedule a
// previous-version pod is still firing mid-rollout — that pod keeps its own
// entries fresh, so the staleness check spares them until it exits.
func (r *RDB) PruneSchedules(ctx context.Context, keepIDs []string, staleBefore time.Time) (int, error) {
	all, err := r.client.HGetAll(ctx, r.keys.SchedulesKey()).Result()
	if err != nil {
		return 0, err
	}
	keep := make(map[string]bool, len(keepIDs))
	for _, id := range keepIDs {
		keep[id] = true
	}
	cutoff := staleBefore.Unix()
	fields := make([]string, 0, len(all))
	for id, raw := range all {
		if keep[id] {
			continue
		}
		var m ScheduleMeta
		if json.Unmarshal([]byte(raw), &m) != nil {
			continue // foreign value shape; not ours to delete
		}
		if m.LastSeen >= cutoff {
			continue // another instance is still heartbeating it
		}
		fields = append(fields, id)
	}
	if len(fields) == 0 {
		return 0, nil
	}
	if err := r.client.HDel(ctx, r.keys.SchedulesKey(), fields...).Err(); err != nil {
		return 0, err
	}
	// One key at a time: last-fired keys are global (no queue hash tag), so a
	// multi-key DEL would cross slots on Redis Cluster. Best-effort — the
	// registry entry is already gone, and a surviving last-fired key only
	// resurfaces as a kind-less fire-history entry.
	for _, f := range fields {
		r.client.Del(ctx, r.keys.ScheduleLastFiredKey(f))
	}
	return len(fields), nil
}

// ListSchedules returns every registry entry.
func (r *RDB) ListSchedules(ctx context.Context) ([]ScheduleMeta, error) {
	all, err := r.client.HGetAll(ctx, r.keys.SchedulesKey()).Result()
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
