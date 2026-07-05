package chronos

import (
	"context"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
)

func TestMux_RoutesToTypedHandler(t *testing.T) {
	mux := NewMux()

	var gotUser string
	AddHandler(mux, func(ctx context.Context, task *Task[emailArgs]) error {
		gotUser = task.Args.UserID
		return nil
	})

	payload, err := encodeArgs(emailArgs{UserID: "u42"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	msg := &base.TaskMessage{ID: "t1", Kind: "email:send", Payload: payload, Queue: "default"}

	if err := mux.dispatch(context.Background(), msg); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if gotUser != "u42" {
		t.Errorf("handler received user %q, want u42", gotUser)
	}
}

func TestMux_UnknownKindErrors(t *testing.T) {
	mux := NewMux()
	msg := &base.TaskMessage{ID: "t1", Kind: "nope", Payload: []byte("{}"), Queue: "default"}
	if err := mux.dispatch(context.Background(), msg); err == nil {
		t.Error("expected error for unregistered kind")
	}
}
