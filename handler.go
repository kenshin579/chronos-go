package chronos

import (
	"context"
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
		return fn(ctx, &Task[T]{Args: args, id: msg.ID, queue: msg.Queue})
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
