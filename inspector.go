package chronos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
)

// Errors returned by the Inspector, exposed so callers (e.g. the web console)
// can map them to the right response.
var (
	ErrTaskNotFound = errors.New("chronos: task not found")
	ErrInvalidState = errors.New("chronos: invalid task state")
)

// Inspector provides read and administrative access to queues and tasks. It is
// the foundation the CLI (and any future UI) is built on.
type Inspector struct {
	rdb *rdb.RDB
}

// NewInspector returns an Inspector backed by the given Redis client.
func NewInspector(r redis.UniversalClient) *Inspector {
	return &Inspector{rdb: rdb.NewRDB(r)}
}

// QueueInfo is a queue's per-state task counts.
type QueueInfo struct {
	Queue     string
	Pending   int64
	Active    int64
	Scheduled int64
	Retry     int64
	Archived  int64
	Completed int64
}

// Queues lists all known queues with their per-state counts.
func (i *Inspector) Queues(ctx context.Context) ([]*QueueInfo, error) {
	names, err := i.rdb.Queues(ctx)
	if err != nil {
		return nil, err
	}
	infos := make([]*QueueInfo, 0, len(names))
	for _, name := range names {
		st, err := i.rdb.QueueStats(ctx, name)
		if err != nil {
			return nil, err
		}
		infos = append(infos, &QueueInfo{
			Queue: st.Queue, Pending: st.Pending, Active: st.Active,
			Scheduled: st.Scheduled, Retry: st.Retry, Archived: st.Archived,
			Completed: st.Completed,
		})
	}
	return infos, nil
}

// zsetKeyForState maps a user-facing state name to its ZSET key.
func zsetKeyForState(qname, state string) (string, error) {
	switch state {
	case "scheduled":
		return base.ScheduledKey(qname), nil
	case "retry":
		return base.RetryKey(qname), nil
	case "archived":
		return base.ArchivedKey(qname), nil
	case "completed":
		return base.CompletedKey(qname), nil
	default:
		return "", fmt.Errorf("%w %q (want scheduled|retry|archived|completed)", ErrInvalidState, state)
	}
}

// ListTasks returns up to limit tasks in the given state (scheduled|retry|archived).
func (i *Inspector) ListTasks(ctx context.Context, qname, state string, limit int) ([]*TaskInfo, error) {
	zsetKey, err := zsetKeyForState(qname, state)
	if err != nil {
		return nil, err
	}
	entries, err := i.rdb.ListZSetTasks(ctx, qname, zsetKey, limit)
	if err != nil {
		return nil, err
	}
	infos := make([]*TaskInfo, 0, len(entries))
	for _, e := range entries {
		ti := taskInfoFromMsg(e.Msg)
		ti.NextProcessAt = time.Unix(int64(e.Score), 0)
		infos = append(infos, ti)
	}
	return infos, nil
}

// GetTask returns full detail for a single stored task (scheduled/retry/archived/completed).
func (i *Inspector) GetTask(ctx context.Context, qname, taskID string) (*TaskInfo, error) {
	msg, err := i.rdb.GetTask(ctx, qname, taskID)
	if err == redis.Nil {
		return nil, fmt.Errorf("%w: %q in queue %q", ErrTaskNotFound, taskID, qname)
	}
	if err != nil {
		return nil, err
	}
	ti := taskInfoFromMsg(msg)
	if zsetKey, kerr := zsetKeyForState(qname, ti.State); kerr == nil {
		if score, ok, serr := i.rdb.ZScore(ctx, zsetKey, taskID); serr == nil && ok {
			ti.NextProcessAt = time.Unix(int64(score), 0)
		}
	}
	if msg.GroupID != "" && msg.GroupQueue != "" {
		if n, perr := i.rdb.GroupPending(ctx, msg.GroupQueue, msg.GroupID); perr == nil {
			ti.GroupPending = int(n)
		}
	}
	return ti, nil
}

// taskInfoFromMsg maps the stored message to the public TaskInfo (no timestamp).
func taskInfoFromMsg(m *base.TaskMessage) *TaskInfo {
	ti := &TaskInfo{
		ID:       m.ID,
		Kind:     m.Kind,
		Queue:    m.Queue,
		State:    m.State.String(),
		Payload:  m.Payload,
		Retried:  m.Retried,
		MaxRetry: m.MaxRetry,
		LastErr:  m.LastErr,
	}
	if m.CompletedAt > 0 {
		ti.CompletedAt = time.Unix(m.CompletedAt, 0)
	}
	ti.ChainPending = len(m.Chain)
	ti.GroupID = m.GroupID
	return ti
}

// RunTask promotes a scheduled/retry/archived task so it runs immediately.
func (i *Inspector) RunTask(ctx context.Context, qname, taskID string) error {
	return i.rdb.RunTask(ctx, qname, taskID)
}

// DeleteTask removes a scheduled/retry/archived task and releases its unique lock.
func (i *Inspector) DeleteTask(ctx context.Context, qname, taskID string) error {
	return i.rdb.DeleteTask(ctx, qname, taskID)
}
