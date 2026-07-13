package chronos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kenshin579/chronos-go/internal/base"
)

// internalHandler is the type-erased handler stored in the Mux. Each one
// decodes the payload into a concrete args type before calling the user's
// typed handler.
type internalHandler func(ctx context.Context, msg *base.TaskMessage) error

// Mux routes tasks to handlers by their Kind.
type Mux struct {
	handlers map[string]internalHandler
}

// NewMux returns an empty Mux.
func NewMux() *Mux {
	return &Mux{handlers: make(map[string]internalHandler)}
}

// AddHandler registers a strongly-typed handler for tasks of type T. The Kind
// is read from the zero value of T, so T's Kind method must use a value
// receiver. Registering two handlers for the same Kind panics.
func AddHandler[T TaskArgs](mux *Mux, fn func(ctx context.Context, task *Task[T]) error) {
	var zero T
	kind := zero.Kind()
	if _, exists := mux.handlers[kind]; exists {
		panic(fmt.Sprintf("chronos: handler already registered for kind %q", kind))
	}
	mux.handlers[kind] = func(ctx context.Context, msg *base.TaskMessage) error {
		args, err := decodeArgs[T](msg.Payload)
		if err != nil {
			return fmt.Errorf("chronos: decode payload for kind %q: %w", kind, err)
		}
		return fn(ctx, newTask[T](args, msg))
	}
}

// MaxResultSize bounds a handler result's JSON encoding. Larger results
// dead-letter the task without retry (the same value would be produced
// again) — pass a reference (object-store path, row ID) instead.
const MaxResultSize = 1 << 20

// ErrResultTooLarge marks a handler result that exceeds MaxResultSize.
var ErrResultTooLarge = errors.New("chronos: result exceeds MaxResultSize")

// AddHandlerR registers a handler whose success return value becomes the
// task's result: it is relayed to the next chain link (read with PrevResult)
// and collected for the group callback (read with GroupResults). The result
// is marshalled as JSON. Kind rules and duplicate-registration panics match
// AddHandler.
func AddHandlerR[T TaskArgs, R any](mux *Mux, fn func(ctx context.Context, task *Task[T]) (R, error)) {
	var zero T
	kind := zero.Kind()
	if _, exists := mux.handlers[kind]; exists {
		panic(fmt.Sprintf("chronos: handler already registered for kind %q", kind))
	}
	mux.handlers[kind] = func(ctx context.Context, msg *base.TaskMessage) error {
		args, err := decodeArgs[T](msg.Payload)
		if err != nil {
			return fmt.Errorf("chronos: decode payload for kind %q: %w", kind, err)
		}
		res, err := fn(ctx, newTask[T](args, msg))
		if err != nil {
			return err
		}
		b, err := json.Marshal(res)
		if err != nil {
			return SkipRetry(fmt.Errorf("chronos: marshal result for kind %q: %w", kind, err))
		}
		if len(b) > MaxResultSize {
			return SkipRetry(fmt.Errorf("chronos: kind %q result is %d bytes: %w", kind, len(b), ErrResultTooLarge))
		}
		msg.Result = b
		return nil
	}
}

// newTask builds the typed task handed to handlers, carrying workflow inputs.
func newTask[T TaskArgs](args T, msg *base.TaskMessage) *Task[T] {
	return &Task[T]{
		Args:         args,
		id:           msg.ID,
		queue:        msg.Queue,
		prevResult:   msg.PrevResult,
		groupResults: msg.GroupResults,
	}
}

// dispatch routes a message to its registered handler.
func (mux *Mux) dispatch(ctx context.Context, msg *base.TaskMessage) error {
	h, ok := mux.handlers[msg.Kind]
	if !ok {
		return fmt.Errorf("chronos: no handler registered for kind %q", msg.Kind)
	}
	return h(ctx, msg)
}
