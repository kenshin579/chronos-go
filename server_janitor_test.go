package chronos

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kenshin579/chronos-go/internal/base"
	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestServer_JanitorCleansExpiredArchived(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	// Seed an archived task that died 1h ago.
	msg := &base.TaskMessage{ID: "old", Kind: "k", Payload: []byte("{}"), Queue: "default", State: base.StateArchived}
	enc, _ := base.EncodeMessage(msg)
	client.HSet(ctx, base.TaskKey("default", "old"), "msg", enc, "state", int(base.StateArchived))
	client.ZAdd(ctx, base.ArchivedKey("default"), redis.Z{Score: float64(time.Now().Add(-time.Hour).Unix()), Member: "old"})

	srv := NewServer(client, ServerConfig{
		Queues:            map[string]int{"default": 1},
		Concurrency:       2,
		ArchivedRetention: 1 * time.Minute,        // died >1m ago → expired
		JanitorInterval:   100 * time.Millisecond, // clean fast for the test
	})
	if err := srv.Start(ctx, NewMux()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Shutdown(context.Background())

	eventually(t, 5*time.Second, func() bool {
		card, _ := client.ZCard(ctx, base.ArchivedKey("default")).Result()
		return card == 0
	}, "janitor should delete the expired archived task")
}
