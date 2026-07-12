package chronos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kenshin579/chronos-go/internal/testutil"
)

func TestScheduler_RegistersSchedulesAndTouches(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()

	s := NewScheduler(client, SchedulerConfig{LeaderTTL: time.Second})
	if err := RegisterInterval(s, time.Hour, emailArgs{UserID: "reg"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer s.Shutdown(context.Background())

	insp := NewInspector(client)
	// 등록 직후: 미발화여도 레지스트리로 노출.
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := insp.SchedulerStatus(ctx)
		if err == nil && len(st.Schedules) == 1 {
			sc := st.Schedules[0]
			if sc.Kind != "email:send" || sc.Queue != "default" || sc.Spec != "@every 1h0m0s" {
				t.Fatalf("schedule meta wrong: %+v", sc)
			}
			if sc.Stale {
				t.Fatal("fresh schedule marked stale")
			}
			if sc.LastSeen.IsZero() {
				t.Fatal("LastSeen zero")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("schedule never appeared in registry")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSchedulerStatus_StaleDetection(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()
	// 레지스트리에 오래된 엔트리를 직접 심는다.
	old := time.Now().Add(-10 * time.Minute).Unix()
	client.HSet(ctx, "chronos:schedules", "ghost#1",
		fmt.Sprintf(`{"kind":"ghost:job","queue":"default","spec":"@every 1s","registered_at":%d,"last_seen":%d}`, old, old))
	insp := NewInspector(client)
	st, err := insp.SchedulerStatus(ctx)
	if err != nil || len(st.Schedules) != 1 {
		t.Fatalf("status: %+v err=%v", st, err)
	}
	if !st.Schedules[0].Stale {
		t.Error("old entry not marked stale")
	}
}

func TestSchedulerStatus_MergesHistoryOnlyEntries(t *testing.T) {
	client := testutil.NewRedis(t)
	ctx := context.Background()
	// 레지스트리 없이 발화 이력만 있는 구버전 잔재.
	client.Set(ctx, "chronos:sched:legacy#9:last", 1700000000, 0)
	insp := NewInspector(client)
	st, err := insp.SchedulerStatus(ctx)
	if err != nil || len(st.Schedules) != 1 {
		t.Fatalf("status: %+v err=%v", st, err)
	}
	sc := st.Schedules[0]
	if sc.ID != "legacy#9" || sc.LastFired.Unix() != 1700000000 || sc.Kind != "" || sc.Stale {
		t.Errorf("history-only entry wrong: %+v", sc)
	}
}
