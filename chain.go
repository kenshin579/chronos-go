package chronos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/kenshin579/chronos-go/internal/base"
)

// Chain builds a sequence of tasks in which each link is enqueued only after
// the previous one succeeds. A link that exhausts its retries stops the chain;
// re-running the dead-lettered link (Inspector/CLI RunTask) resumes it, because
// every link carries its remaining tail inside its own message.
//
// Handlers must be idempotent, as everywhere in chronos-go: a redelivered link
// may run its handler more than once, though its successor is enqueued at most
// once (deterministic successor IDs + create-if-absent).
type Chain struct {
	links []struct {
		args TaskArgs
		opts []Option
	}
}

// NewChain returns an empty chain builder.
func NewChain() *Chain { return &Chain{} }

// Then appends a link. opts accepts the usual per-task options (WithQueue,
// WithMaxRetry, WithRetention, WithProcessIn, ...); WithTaskID and WithUnique
// are rejected at Enqueue time (the chain owns task IDs, and unique dedup
// inside chains is not supported).
func (ch *Chain) Then(args TaskArgs, opts ...Option) *Chain {
	ch.links = append(ch.links, struct {
		args TaskArgs
		opts []Option
	}{args, opts})
	return ch
}

// Enqueue makes the first link available for processing and returns its
// TaskInfo. Later links run only as their predecessors succeed.
func (ch *Chain) Enqueue(ctx context.Context, c *Client) (*TaskInfo, error) {
	if len(ch.links) == 0 {
		return nil, errors.New("chronos: empty chain")
	}

	chainID := uuid.NewString()

	// Snapshot links 1..n-1 as the first link's tail.
	tail := make([]base.ChainLink, 0, len(ch.links)-1)
	for i := 1; i < len(ch.links); i++ {
		link, err := snapshotChainLink(ch.links[i].args, ch.links[i].opts)
		if err != nil {
			return nil, fmt.Errorf("chain link %d: %w", i, err)
		}
		tail = append(tail, link)
	}

	// First link: resolve options like Enqueue does, with chain-owned identity.
	first := ch.links[0]
	options, err := resolveChainOptions(first.opts)
	if err != nil {
		return nil, fmt.Errorf("chain link 0: %w", err)
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
	return &TaskInfo{ID: msg.ID, Kind: msg.Kind, Queue: msg.Queue}, nil
}

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
func snapshotChainLink(args TaskArgs, opts []Option) (base.ChainLink, error) {
	o, err := resolveChainOptions(opts)
	if err != nil {
		return base.ChainLink{}, err
	}
	payload, err := encodeArgs(args)
	if err != nil {
		return base.ChainLink{}, err
	}
	var delay int64
	if !o.processAt.IsZero() {
		// WithProcessIn stored an absolute time; capture the intended relative
		// delay (we are still inside the builder call, so the drift is tiny).
		if d := time.Until(o.processAt); d > 0 {
			delay = int64(d / time.Second)
			if delay == 0 {
				delay = 1
			}
		}
	}
	return base.ChainLink{
		Kind:      args.Kind(),
		Payload:   payload,
		Queue:     o.queue,
		MaxRetry:  o.maxRetry,
		NoArchive: o.noArchive,
		Retention: int64(o.retention / time.Second),
		Delay:     delay,
	}, nil
}
