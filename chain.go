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

// Chain builds a sequence of tasks in which each link is enqueued only after
// the previous one succeeds. A link that exhausts its retries stops the chain;
// re-running the dead-lettered link (Inspector/CLI RunTask) resumes it, because
// every link carries its remaining tail inside its own message.
//
// Handlers must be idempotent, as everywhere in chronos-go: a redelivered link
// may run its handler more than once. Its successor is enqueued at most once
// while that successor still exists; if a predecessor is redelivered after its
// successor already finished and was not retained, the successor can be
// recreated — the usual at-least-once caveat. Per-link WithRetention keeps the
// successor's record around and closes that window for its duration.
type Chain struct {
	stages []chainStage
}

// chainStage is one step of a chain: a single task (isGroup false) or a
// parallel group stage.
type chainStage struct {
	args    TaskArgs
	opts    []Option
	group   *Group // parallel stage's group (args/opts unused)
	isGroup bool   // set by ThenGroup, even for a nil group (validated at snapshot)
}

// NewChain returns an empty chain builder.
func NewChain() *Chain { return &Chain{} }

// Then appends a link. opts accepts the usual per-task options (WithQueue,
// WithMaxRetry, WithRetention, WithProcessIn, ...); WithTaskID and WithUnique
// are rejected at Enqueue time (the chain owns task IDs, and unique dedup
// inside chains is not supported).
func (ch *Chain) Then(args TaskArgs, opts ...Option) *Chain {
	ch.stages = append(ch.stages, chainStage{args: args, opts: opts})
	return ch
}

// ThenGroup appends a parallel stage: every member of g runs concurrently
// (each receiving the previous stage's result via PrevResult) and g's
// OnComplete callback fans the member results in (GroupResults) before the
// chain continues with the callback's own result. g must not be reused or
// mutated afterwards. A group cannot be the chain's first stage — use
// NewGroup directly when no preceding step exists.
func (ch *Chain) ThenGroup(g *Group) *Chain {
	ch.stages = append(ch.stages, chainStage{group: g, isGroup: true})
	return ch
}

// Enqueue makes the first link available for processing and returns its
// TaskInfo. Later links run only as their predecessors succeed.
func (ch *Chain) Enqueue(ctx context.Context, c *Client) (*TaskInfo, error) {
	if len(ch.stages) == 0 {
		return nil, errors.New("chronos: empty chain")
	}
	if ch.stages[0].isGroup {
		return nil, errors.New("chronos: a group cannot be the chain's first stage — start with Then, or use NewGroup directly")
	}

	chainID := uuid.NewString()

	// Snapshot stages 1..n-1 as the first link's tail.
	tail, err := ch.snapshotTail()
	if err != nil {
		return nil, err
	}

	// First link: resolve options like Enqueue does, with chain-owned identity.
	first := ch.stages[0]
	options, err := resolveChainOptions(first.opts)
	if err != nil {
		return nil, fmt.Errorf("chain link 0: %w", err)
	}
	if len(ch.stages) > 1 && options.noArchive {
		return nil, fmt.Errorf("chain link 0: %w", errNoArchiveMidChain)
	}
	payload, err := encodeArgs(first.args)
	if err != nil {
		return nil, fmt.Errorf("chain link 0: %w", err)
	}
	msg := &base.TaskMessage{
		ID:         chainID + ":0",
		Kind:       first.args.Kind(),
		Payload:    payload,
		Queue:      options.queue,
		MaxRetry:   options.maxRetry,
		NoArchive:  options.noArchive,
		Retention:  int64(options.retention / time.Second),
		Chain:      tail,
		ChainID:    chainID,
		ChainIndex: 0,
	}
	if err := dispatchMessage(ctx, c, msg, options); err != nil {
		return nil, err
	}
	return &TaskInfo{ID: msg.ID, Kind: msg.Kind, Queue: msg.Queue, ChainPending: len(tail)}, nil
}

// snapshotTail freezes stages 1..n-1 into ChainLinks (test seam).
func (ch *Chain) snapshotTail() ([]base.ChainLink, error) {
	tail := make([]base.ChainLink, 0, len(ch.stages)-1)
	for i := 1; i < len(ch.stages); i++ {
		link, err := ch.snapshotStage(i)
		if err != nil {
			return nil, err
		}
		tail = append(tail, link)
	}
	return tail, nil
}

// hasGroupStage reports whether any stage is a parallel (ThenGroup) stage.
func (ch *Chain) hasGroupStage() bool {
	for _, st := range ch.stages {
		if st.isGroup {
			return true
		}
	}
	return false
}

// snapshotForMember builds the first-link message of a chain used as a group
// member — same shape as Enqueue produces, but returned (not dispatched) so the
// caller can attach group-reporting fields. chainID is the member slot ID; the
// first link's own ID becomes "<chainID>:0". Rejects a ThenGroup stage anywhere
// and any discard link (a discarded member link would strand the group).
func (ch *Chain) snapshotForMember(chainID string) (*base.TaskMessage, enqueueOptions, error) {
	if ch == nil {
		return nil, enqueueOptions{}, errors.New("chronos: nil chain member")
	}
	if len(ch.stages) == 0 {
		return nil, enqueueOptions{}, errors.New("chronos: empty chain member")
	}
	if ch.hasGroupStage() {
		return nil, enqueueOptions{}, errors.New("chronos: a group member chain cannot contain a parallel stage (ThenGroup) — recursive nesting beyond one level is not supported")
	}
	tail, err := ch.snapshotTail()
	if err != nil {
		return nil, enqueueOptions{}, err
	}
	first := ch.stages[0]
	options, err := resolveChainOptions(first.opts)
	if err != nil {
		return nil, enqueueOptions{}, fmt.Errorf("chain member link 0: %w", err)
	}
	if options.noArchive {
		return nil, enqueueOptions{}, errors.New("chronos: a group member chain link cannot discard (WithDeadLetterDiscard) — it would strand the group")
	}
	for _, l := range tail {
		if l.NoArchive {
			return nil, enqueueOptions{}, errors.New("chronos: a group member chain link cannot discard (WithDeadLetterDiscard) — it would strand the group")
		}
	}
	payload, err := encodeArgs(first.args)
	if err != nil {
		return nil, enqueueOptions{}, fmt.Errorf("chain member link 0: %w", err)
	}
	msg := &base.TaskMessage{
		ID:         chainID + ":0",
		Kind:       first.args.Kind(),
		Payload:    payload,
		Queue:      options.queue,
		MaxRetry:   options.maxRetry,
		Retention:  int64(options.retention / time.Second),
		Chain:      tail,
		ChainID:    chainID,
		ChainIndex: 0,
	}
	return msg, options, nil
}

// snapshotStage freezes stage i (single task or group) into a ChainLink.
func (ch *Chain) snapshotStage(i int) (base.ChainLink, error) {
	st := ch.stages[i]
	isLast := i == len(ch.stages)-1
	if !st.isGroup {
		link, err := snapshotChainLink(st.args, st.opts, isLast)
		if err != nil {
			return base.ChainLink{}, fmt.Errorf("chain link %d: %w", i, err)
		}
		return link, nil
	}
	return snapshotGroupStage(st.group, i, isLast)
}

// snapshotGroupStage freezes a parallel stage: the link's own task fields
// describe the fan-in callback; Group holds the members.
func snapshotGroupStage(g *Group, i int, isLast bool) (base.ChainLink, error) {
	if g == nil {
		return base.ChainLink{}, fmt.Errorf("chain stage %d: nil group", i)
	}
	if len(g.members) == 0 {
		return base.ChainLink{}, fmt.Errorf("chain stage %d: group needs at least one member", i)
	}
	if !g.hasCallback {
		return base.ChainLink{}, fmt.Errorf("chain stage %d: group needs a callback (OnComplete)", i)
	}
	// 콜백: 마지막 스테이지의 콜백만 noArchive 허용(단일 링크 규칙과 동일).
	link, err := snapshotChainLink(g.callback, g.callbackOpts, isLast)
	if err != nil {
		return base.ChainLink{}, fmt.Errorf("chain stage %d callback: %w", i, err)
	}
	link.Group = make([]base.GroupMemberLink, 0, len(g.members))
	for j, m := range g.members {
		if m.isChain {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: a group used as a chain stage cannot have chain members yet", i, j)
		}
		o, err := resolveChainOptions(m.opts)
		if err != nil {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: %w", i, j, err)
		}
		if o.noArchive {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: WithDeadLetterDiscard would strand the group (no dead-letter left to re-run)", i, j)
		}
		if o.processAtAbsolute {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: WithProcessAt cannot be used on a group member (its delay is relative; use WithProcessIn)", i, j)
		}
		payload, err := encodeArgs(m.args)
		if err != nil {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: %w", i, j, err)
		}
		delay := captureRelativeDelay(o)
		// 독립 Group.Enqueue와 동일한 상한: 멤버 지연이 GroupTTL을 넘으면 pending
		// SET이 멤버 실행 전에 만료되어 콜백이 영영 뜨지 않는다(조용한 좌초).
		if delay > int64(rdb.GroupTTL/time.Second) {
			return base.ChainLink{}, fmt.Errorf("chain stage %d member %d: WithProcessIn exceeds the group TTL (%v)", i, j, rdb.GroupTTL)
		}
		link.Group = append(link.Group, base.GroupMemberLink{
			Kind:      m.args.Kind(),
			Payload:   payload,
			Queue:     o.queue,
			MaxRetry:  o.maxRetry,
			Retention: int64(o.retention / time.Second),
			Delay:     delay,
		})
	}
	return link, nil
}

// errNoArchiveMidChain: discarding a dead-lettered mid-link would delete the
// chain's remaining tail with it, so there would be nothing left to resume.
var errNoArchiveMidChain = errors.New("chronos: WithDeadLetterDiscard is only allowed on the last chain link (discarding a mid-link would delete the chain's remaining tail)")

// resolveChainOptions applies opts and rejects the ones a chain cannot honor.
func resolveChainOptions(opts []Option) (enqueueOptions, error) {
	o := enqueueOptions{queue: DefaultQueue, maxRetry: DefaultMaxRetry}
	for _, opt := range opts {
		opt.apply(&o)
	}
	if o.taskID != "" {
		return o, errors.New("chronos: WithTaskID cannot be used inside a chain (the chain owns task IDs)")
	}
	if o.uniqueTTL > 0 {
		return o, errors.New("chronos: WithUnique is not supported inside a chain")
	}
	return o, nil
}

// snapshotChainLink freezes a successor's enqueue parameters into a ChainLink.
func snapshotChainLink(args TaskArgs, opts []Option, isLast bool) (base.ChainLink, error) {
	o, err := resolveChainOptions(opts)
	if err != nil {
		return base.ChainLink{}, err
	}
	if o.processAtAbsolute {
		return base.ChainLink{}, errors.New("chronos: WithProcessAt cannot be used on a chain link after the first (its delay is relative to the predecessor; use WithProcessIn)")
	}
	if o.noArchive && !isLast {
		return base.ChainLink{}, errNoArchiveMidChain
	}
	payload, err := encodeArgs(args)
	if err != nil {
		return base.ChainLink{}, err
	}
	return base.ChainLink{
		Kind:      args.Kind(),
		Payload:   payload,
		Queue:     o.queue,
		MaxRetry:  o.maxRetry,
		NoArchive: o.noArchive,
		Retention: int64(o.retention / time.Second),
		Delay:     captureRelativeDelay(o),
	}, nil
}

// captureRelativeDelay converts a WithProcessIn-stored absolute time back into
// the intended relative delay in whole seconds (we are still inside the builder
// call, so the drift is tiny). Sub-second remainders round to the nearest
// second, and any positive delay is at least 1s so it is never silently lost.
func captureRelativeDelay(o enqueueOptions) int64 {
	if o.processAt.IsZero() {
		return 0
	}
	d := time.Until(o.processAt)
	if d <= 0 {
		return 0
	}
	delay := int64((d + time.Second/2) / time.Second)
	if delay == 0 {
		delay = 1
	}
	return delay
}
