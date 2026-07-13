package chronos

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kenshin579/chronos-go/internal/base"
)

type resArgs struct {
	N int `json:"n"`
}

func (resArgs) Kind() string { return "res:job" }

type resOut struct {
	Doubled int    `json:"doubled"`
	Big     string `json:"big,omitempty"`
}

func dispatchMsg(t *testing.T, mux *Mux, msg *base.TaskMessage) error {
	t.Helper()
	return mux.dispatch(context.Background(), msg)
}

func TestAddHandlerR_SetsResultOnSuccess(t *testing.T) {
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[resArgs]) (resOut, error) {
		return resOut{Doubled: task.Args.N * 2}, nil
	})
	msg := &base.TaskMessage{ID: "t1", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":21}`)}
	if err := dispatchMsg(t, mux, msg); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if string(msg.Result) != `{"doubled":42}` {
		t.Errorf("result = %s", msg.Result)
	}
}

func TestAddHandlerR_ErrorLeavesNoResult(t *testing.T) {
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[resArgs]) (resOut, error) {
		return resOut{Doubled: 99}, errors.New("boom")
	})
	msg := &base.TaskMessage{ID: "t1", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`)}
	if err := dispatchMsg(t, mux, msg); err == nil {
		t.Fatal("want error")
	}
	if msg.Result != nil {
		t.Errorf("failed handler must not set a result: %s", msg.Result)
	}
}

func TestAddHandlerR_OversizeResultSkipsRetry(t *testing.T) {
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[resArgs]) (resOut, error) {
		return resOut{Big: strings.Repeat("x", MaxResultSize)}, nil
	})
	msg := &base.TaskMessage{ID: "t1", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`)}
	err := dispatchMsg(t, mux, msg)
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("want ErrResultTooLarge, got %v", err)
	}
	if !asSkipRetry(err) {
		t.Error("oversize result must be non-retryable")
	}
	if msg.Result != nil {
		t.Error("oversize result must not be stored")
	}
}

func TestPrevResult(t *testing.T) {
	mux := NewMux()
	var got resOut
	var gotErr error
	AddHandler(mux, func(ctx context.Context, task *Task[resArgs]) error {
		got, gotErr = PrevResult[resOut](task)
		return nil
	})
	msg := &base.TaskMessage{ID: "t2", Kind: "res:job", Queue: "q",
		Payload: []byte(`{"n":1}`), PrevResult: []byte(`{"doubled":42}`)}
	if err := dispatchMsg(t, mux, msg); err != nil {
		t.Fatal(err)
	}
	if gotErr != nil || got.Doubled != 42 {
		t.Errorf("prev result = %+v err=%v", got, gotErr)
	}
	// 결과 없는 선행 → ErrNoResult.
	msg2 := &base.TaskMessage{ID: "t3", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`)}
	if err := dispatchMsg(t, mux, msg2); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(gotErr, ErrNoResult) {
		t.Errorf("want ErrNoResult, got %v", gotErr)
	}
}

func TestGroupResultsAccessors(t *testing.T) {
	mux := NewMux()
	var typed []resOut
	var typedErr error
	var raw [][]byte
	AddHandler(mux, func(ctx context.Context, task *Task[resArgs]) error {
		typed, typedErr = GroupResults[resOut](task)
		raw = task.RawGroupResults()
		return nil
	})
	// 동질 그룹: 전부 디코딩.
	msg := &base.TaskMessage{ID: "cb", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`),
		GroupResults: [][]byte{[]byte(`{"doubled":2}`), []byte(`{"doubled":4}`)}}
	if err := dispatchMsg(t, mux, msg); err != nil {
		t.Fatal(err)
	}
	if typedErr != nil || len(typed) != 2 || typed[0].Doubled != 2 || typed[1].Doubled != 4 {
		t.Errorf("typed = %+v err=%v", typed, typedErr)
	}
	if len(raw) != 2 {
		t.Errorf("raw = %v", raw)
	}
	// nil 멤버 포함 → 타입드는 ErrNoResult, raw는 위치 보존.
	msg2 := &base.TaskMessage{ID: "cb2", Kind: "res:job", Queue: "q", Payload: []byte(`{"n":1}`),
		GroupResults: [][]byte{[]byte(`{"doubled":2}`), nil}}
	if err := dispatchMsg(t, mux, msg2); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(typedErr, ErrNoResult) {
		t.Errorf("want ErrNoResult for nil member, got %v", typedErr)
	}
	if len(raw) != 2 || raw[1] != nil {
		t.Errorf("raw must keep positions: %v", raw)
	}
}
