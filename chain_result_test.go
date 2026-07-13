package chronos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

type relayArgs struct {
	Step int `json:"step"`
}

func (relayArgs) Kind() string { return "relay:step" }

type relayOut struct {
	Sum int `json:"sum"`
}

// 2링크 체인: 링크0의 결과가 링크1의 PrevResult로 도착하고, 링크1이 1회
// 실패(재시도)해도 보존되는지 확인.
func TestChain_RelaysResultToSuccessor(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	var got atomic.Int64 // 링크1이 수신한 PrevResult.Sum
	var tries atomic.Int64
	mux := NewMux()
	AddHandlerR(mux, func(ctx context.Context, task *Task[relayArgs]) (relayOut, error) {
		return relayOut{Sum: task.Args.Step + 41}, nil
	})
	// 두 번째 링크는 다른 Kind로 — 첫 시도 실패 후 재시도에서도 PrevResult 확인.
	AddHandler(mux, func(ctx context.Context, task *Task[relayCheckArgs]) error {
		out, err := PrevResult[relayOut](task)
		if err != nil {
			t.Errorf("prev result: %v", err)
			return nil
		}
		if tries.Add(1) == 1 {
			return errors.New("transient")
		}
		got.Store(int64(out.Sum))
		return nil
	})

	srv := NewServer(client, ServerConfig{Queues: map[string]int{"relay": 1}, Concurrency: 2,
		RetryDelayFunc: func(n int, err error) time.Duration { return 100 * time.Millisecond }})
	if err := srv.Start(ctx, mux); err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown(context.Background())

	c := NewClient(client)
	_, err := NewChain().
		Then(relayArgs{Step: 1}, WithQueue("relay")).
		Then(relayCheckArgs{}, WithQueue("relay")).
		Enqueue(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for got.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got.Load() != 42 {
		t.Fatalf("successor got %d, want 42 (tries=%d)", got.Load(), tries.Load())
	}
	if tries.Load() != 2 {
		t.Errorf("expected exactly one retry, tries=%d", tries.Load())
	}
}

type relayCheckArgs struct{}

func (relayCheckArgs) Kind() string { return "relay:check" }
