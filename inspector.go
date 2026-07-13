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
	Paused    bool
}

// Queues lists all known queues with their per-state counts.
func (i *Inspector) Queues(ctx context.Context) ([]*QueueInfo, error) {
	names, err := i.rdb.Queues(ctx)
	if err != nil {
		return nil, err
	}
	pausedNames, err := i.rdb.PausedQueues(ctx)
	if err != nil {
		return nil, err
	}
	paused := make(map[string]bool, len(pausedNames))
	for _, name := range pausedNames {
		paused[name] = true
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
			Completed: st.Completed, Paused: paused[st.Queue],
		})
	}
	return infos, nil
}

// PauseQueue stops servers from consuming the queue (within about one second).
// Enqueueing, forwarding and recovery continue — work accumulates as pending.
func (i *Inspector) PauseQueue(ctx context.Context, qname string) error {
	return i.rdb.PauseQueue(ctx, qname)
}

// ResumeQueue lifts a pause; consumption restarts within about one second.
func (i *Inspector) ResumeQueue(ctx context.Context, qname string) error {
	return i.rdb.ResumeQueue(ctx, qname)
}

// PausedQueues lists currently paused queue names.
func (i *Inspector) PausedQueues(ctx context.Context) ([]string, error) {
	return i.rdb.PausedQueues(ctx)
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
	ti.ChainIndex = m.ChainIndex
	if len(m.Chain) > 0 {
		kinds := make([]string, len(m.Chain))
		for i, l := range m.Chain {
			kinds[i] = l.Kind
		}
		ti.ChainNext = kinds
	}
	ti.GroupID = m.GroupID
	ti.GroupQueue = m.GroupQueue
	ti.HasResult = len(m.Result) > 0
	ti.ResultSize = len(m.Result)
	return ti
}

// GroupMembers returns the IDs of a group's not-yet-succeeded members. cbQueue
// is the callback queue (a member TaskInfo carries it as GroupQueue).
func (i *Inspector) GroupMembers(ctx context.Context, cbQueue, groupID string) ([]string, error) {
	return i.rdb.GroupMemberIDs(ctx, cbQueue, groupID)
}

// SchedulerStatus reports the current scheduler leader and every schedule that
// has fired at least once (registered-but-never-fired schedules are invisible;
// a future schedule registry will list them).
type SchedulerStatus struct {
	LeaderID  string
	Schedules []ScheduleInfo
}

// ScheduleInfo is one schedule's registry entry merged with its fire history.
type ScheduleInfo struct {
	ID        string
	Kind      string // "" for fire-history-only entries (pre-registry leftovers)
	Queue     string
	Spec      string    // "@every 30s" or a 5-field cron expression
	LastFired time.Time // zero if the schedule never fired
	LastSeen  time.Time // zero for fire-history-only entries
	Stale     bool      // registry entry not refreshed within staleAfter
}

// staleAfter marks a registry entry stale when no scheduler has refreshed it
// this long (running schedulers touch entries every LeaderTTL/2).
const staleAfter = time.Minute

// SchedulerStatus returns the scheduler's leader and every known schedule:
// the registry (registered schedules, fired or not) merged by ID with the
// fire-history keys (which may also hold pre-registry leftovers).
func (i *Inspector) SchedulerStatus(ctx context.Context) (*SchedulerStatus, error) {
	leader, err := i.rdb.LeaderID(ctx)
	if err != nil {
		return nil, err
	}
	registry, err := i.rdb.ListSchedules(ctx)
	if err != nil {
		return nil, err
	}
	fired, err := i.rdb.ScanSchedules(ctx)
	if err != nil {
		return nil, err
	}
	firedAt := make(map[string]int64, len(fired))
	for _, f := range fired {
		firedAt[f.ID] = f.LastFired
	}

	st := &SchedulerStatus{LeaderID: leader, Schedules: make([]ScheduleInfo, 0, len(registry)+len(fired))}
	seen := make(map[string]bool, len(registry))
	for _, m := range registry {
		seen[m.ID] = true
		info := ScheduleInfo{
			ID: m.ID, Kind: m.Kind, Queue: m.Queue, Spec: m.Spec,
			LastSeen: time.Unix(m.LastSeen, 0),
			Stale:    time.Since(time.Unix(m.LastSeen, 0)) > staleAfter,
		}
		if ts, ok := firedAt[m.ID]; ok {
			info.LastFired = time.Unix(ts, 0)
		}
		st.Schedules = append(st.Schedules, info)
	}
	for _, f := range fired { // history-only leftovers keep working
		if !seen[f.ID] {
			st.Schedules = append(st.Schedules, ScheduleInfo{ID: f.ID, LastFired: time.Unix(f.LastFired, 0)})
		}
	}
	return st, nil
}

// RunTask promotes a scheduled/retry/archived task so it runs immediately.
func (i *Inspector) RunTask(ctx context.Context, qname, taskID string) error {
	return i.rdb.RunTask(ctx, qname, taskID)
}

// DeleteTask removes a scheduled/retry/archived task and releases its unique lock.
func (i *Inspector) DeleteTask(ctx context.Context, qname, taskID string) error {
	return i.rdb.DeleteTask(ctx, qname, taskID)
}
