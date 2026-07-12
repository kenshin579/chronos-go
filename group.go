package chronos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/kenshin579/chronos-go/internal/base"
)

// Group builds a parallel fan-out: every member is enqueued at once, and when
// ALL members have succeeded, the callback task is enqueued exactly once while
// its record exists. A member that exhausts its retries parks the group:
// re-run its dead-letter (Inspector/CLI RunTask) and, once it succeeds, the
// group resumes — the same stop-and-resume rule chains follow. Abandoned
// groups (a member deleted and never re-run) expire after rdb.GroupTTL.
//
// Handlers must be idempotent, as everywhere in chronos-go.
type Group struct {
	members []struct {
		args TaskArgs
		opts []Option
	}
	callback     TaskArgs
	callbackOpts []Option
	hasCallback  bool
}

// GroupInfo describes an enqueued group.
type GroupInfo struct {
	GroupID    string   // the group's identity
	MemberIDs  []string // deterministic member task IDs ("<groupID>:m<i>")
	CallbackID string   // the callback's task ID ("<groupID>:cb")
}

// NewGroup returns an empty group builder.
func NewGroup() *Group { return &Group{} }

// Add appends a member task. Members run in parallel on their own queues with
// their own options; WithTaskID and WithUnique are rejected at Enqueue time.
func (g *Group) Add(args TaskArgs, opts ...Option) *Group {
	g.members = append(g.members, struct {
		args TaskArgs
		opts []Option
	}{args, opts})
	return g
}

// OnComplete sets the callback task, enqueued once every member has succeeded.
// WithProcessIn delays it relative to the group's completion; WithProcessAt,
// WithTaskID and WithUnique are rejected.
func (g *Group) OnComplete(args TaskArgs, opts ...Option) *Group {
	g.callback = args
	g.callbackOpts = opts
	g.hasCallback = true
	return g
}

// Enqueue creates the group's pending-member record first (so a partially
// failed enqueue can never fire the callback early), then enqueues every
// member. Members enqueue sequentially and non-atomically: on error, already
// enqueued members will still run, and the group simply never completes (its
// record expires after rdb.GroupTTL).
func (g *Group) Enqueue(ctx context.Context, c *Client) (*GroupInfo, error) {
	if len(g.members) == 0 {
		return nil, errors.New("chronos: group needs at least one member")
	}
	if !g.hasCallback {
		return nil, errors.New("chronos: group needs a callback (OnComplete)")
	}

	// Callback snapshot (same rules as a chain tail link: relative delay only).
	cbLink, err := snapshotChainLink(g.callback, g.callbackOpts, true)
	if err != nil {
		return nil, fmt.Errorf("group callback: %w", err)
	}

	groupID := uuid.NewString()
	memberIDs := make([]string, len(g.members))
	for i := range g.members {
		memberIDs[i] = fmt.Sprintf("%s:m%d", groupID, i)
	}

	// 1) Register the pending-member SET before any member can possibly finish.
	if err := c.rdb.CreateGroup(ctx, cbLink.Queue, groupID, memberIDs); err != nil {
		return nil, err
	}

	// 2) Enqueue the members.
	for i, m := range g.members {
		options, err := resolveChainOptions(m.opts)
		if err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
		payload, err := encodeArgs(m.args)
		if err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
		msg := &base.TaskMessage{
			ID:            memberIDs[i],
			Kind:          m.args.Kind(),
			Payload:       payload,
			Queue:         options.queue,
			MaxRetry:      options.maxRetry,
			NoArchive:     options.noArchive,
			Retention:     int64(options.retention / time.Second),
			GroupID:       groupID,
			GroupQueue:    cbLink.Queue,
			GroupCallback: &cbLink,
		}
		if err := dispatchMessage(ctx, c, msg, options); err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
	}

	return &GroupInfo{
		GroupID:    groupID,
		MemberIDs:  memberIDs,
		CallbackID: groupID + ":cb",
	}, nil
}
