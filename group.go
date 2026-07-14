package chronos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/rdb"
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
	members      []groupMember
	callback     TaskArgs
	callbackOpts []Option
	hasCallback  bool
}

// groupMember is one member: a single task (isChain false) or a chain
// (isChain true, chain may still be nil — validated at Enqueue).
type groupMember struct {
	args    TaskArgs
	opts    []Option
	chain   *Chain
	isChain bool
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
	g.members = append(g.members, groupMember{args: args, opts: opts})
	return g
}

// AddChain appends a chain member: its links run in sequence, and the chain's
// FINAL link reports the member's completion to this group (its result becomes
// this member's GroupResults entry). The chain may not contain a ThenGroup
// stage (one-level nesting only) and its links may not use WithUnique/
// WithTaskID or WithDeadLetterDiscard.
func (g *Group) AddChain(ch *Chain) *Group {
	g.members = append(g.members, groupMember{chain: ch, isChain: true})
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

// Enqueue validates every member up front, creates the group's pending-member
// record (so a partially failed enqueue can never fire the callback early),
// then enqueues every member. Enqueueing is sequential and non-atomic only
// against infrastructure failures: a network error midway leaves already
// enqueued members running and the group incomplete (its record expires after
// rdb.GroupTTL).
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

	// Validate and snapshot every member BEFORE creating any Redis state, so a
	// deterministic builder error cannot leave a half-enqueued, stranded group.
	type pendingMember struct {
		msg     *base.TaskMessage
		options enqueueOptions
	}
	pending := make([]pendingMember, 0, len(g.members))
	for i, m := range g.members {
		memberSlot := memberIDs[i] // "<groupID>:m<i>"
		if m.isChain {
			msg, options, err := m.chain.snapshotForMember(memberSlot)
			if err != nil {
				return nil, fmt.Errorf("group member %d: %w", i, err)
			}
			// flat·ThenGroup 멤버 경로와 동일한 상한: 멤버 지연이 GroupTTL을 넘으면
			// pending SET이 멤버 완료 보고 전에 만료되어 콜백이 조용히 미발화한다.
			// 체인 멤버는 마지막 링크까지 진행한 뒤에야 그룹에 보고하고(AddChain 계약),
			// pending SET TTL은 체인 링크 진행 중엔 갱신되지 않으므로 첫 링크뿐 아니라
			// 모든 링크 지연의 합이 SET 생성~보고 창을 늘린다 — 합으로 검사한다.
			memberDelay := time.Duration(0)
			if !options.processAt.IsZero() {
				memberDelay = time.Until(options.processAt)
			}
			for _, l := range msg.Chain {
				if l.Delay > 0 {
					memberDelay += time.Duration(l.Delay) * time.Second
				}
			}
			if memberDelay > rdb.GroupTTL {
				return nil, fmt.Errorf("group member %d: chain link delays exceed the group TTL (%v)", i, rdb.GroupTTL)
			}
			// 그룹 보고 필드: 마지막 링크까지 enqueueNext가 전파, 마지막 링크가 보고.
			msg.GroupID = groupID
			msg.GroupQueue = cbLink.Queue
			msg.GroupCallback = &cbLink
			msg.GroupIndex = i
			msg.GroupSize = len(g.members)
			msg.GroupMemberID = memberSlot
			pending = append(pending, pendingMember{msg: msg, options: options})
			continue
		}
		// --- 단일 태스크 경로 (m.args/m.opts) ---
		options, err := resolveChainOptions(m.opts)
		if err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
		if options.noArchive {
			return nil, fmt.Errorf("group member %d: WithDeadLetterDiscard would strand the group (no dead-letter left to re-run)", i)
		}
		if !options.processAt.IsZero() && time.Until(options.processAt) > rdb.GroupTTL {
			return nil, fmt.Errorf("group member %d: WithProcessIn/WithProcessAt exceeds the group TTL (%v)", i, rdb.GroupTTL)
		}
		payload, err := encodeArgs(m.args)
		if err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
		pending = append(pending, pendingMember{
			msg: &base.TaskMessage{
				ID:            memberSlot,
				Kind:          m.args.Kind(),
				Payload:       payload,
				Queue:         options.queue,
				MaxRetry:      options.maxRetry,
				Retention:     int64(options.retention / time.Second),
				GroupID:       groupID,
				GroupQueue:    cbLink.Queue,
				GroupCallback: &cbLink,
				GroupIndex:    i,
				GroupSize:     len(g.members),
			},
			options: options,
		})
	}

	// 1) Register the pending-member SET before any member can possibly finish.
	if err := c.rdb.CreateGroup(ctx, cbLink.Queue, groupID, memberIDs); err != nil {
		return nil, err
	}

	// 2) Enqueue the members (sequential, non-atomic: a network failure midway
	// leaves earlier members running and the group incomplete — its record
	// expires after rdb.GroupTTL and the callback never fires).
	for i, p := range pending {
		if err := dispatchMessage(ctx, c, p.msg, p.options); err != nil {
			return nil, fmt.Errorf("group member %d: %w", i, err)
		}
	}

	return &GroupInfo{
		GroupID:    groupID,
		MemberIDs:  memberIDs,
		CallbackID: groupID + ":cb",
	}, nil
}
