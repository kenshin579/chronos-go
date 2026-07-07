package chronos

import (
	"testing"
	"time"
)

// everySecond is a trivial schedule: next trigger is the next whole second after t.
type everySecond struct{}

func (everySecond) Next(t time.Time) time.Time {
	return t.Truncate(time.Second).Add(time.Second)
}

func TestComputeFires_NormalTick_FiresOnce(t *testing.T) {
	sched := everySecond{}
	last := time.Unix(100, 0)
	now := time.Unix(101, 0) // exactly one trigger (101) is due
	fires, newLast := computeFires(sched, last, now, MisfireSkip)
	if len(fires) != 1 || !fires[0].Equal(time.Unix(101, 0)) {
		t.Fatalf("fires = %v, want [101]", fires)
	}
	if !newLast.Equal(time.Unix(101, 0)) {
		t.Errorf("newLast = %v, want 101", newLast)
	}
}

func TestComputeFires_NotDue_NoFire(t *testing.T) {
	fires, newLast := computeFires(everySecond{}, time.Unix(101, 0), time.Unix(101, 0), MisfireSkip)
	if len(fires) != 0 {
		t.Errorf("fires = %v, want none", fires)
	}
	if !newLast.Equal(time.Unix(101, 0)) {
		t.Errorf("newLast = %v, want unchanged 101", newLast)
	}
}

func TestComputeFires_Gap_Skip_FiresLatestOnly(t *testing.T) {
	// last=100, now=105 → triggers 101,102,103,104,105 missed. Skip fires none of
	// the missed ones and fast-forwards lastFired to the latest trigger (105).
	fires, newLast := computeFires(everySecond{}, time.Unix(100, 0), time.Unix(105, 0), MisfireSkip)
	if len(fires) != 0 {
		t.Errorf("Skip should not fire missed triggers, got %v", fires)
	}
	if !newLast.Equal(time.Unix(105, 0)) {
		t.Errorf("newLast = %v, want 105 (fast-forwarded)", newLast)
	}
}

func TestComputeFires_Gap_FireOnce_FiresLatestOnce(t *testing.T) {
	// Same gap; FireOnce catches up with exactly one fire (for the latest trigger).
	fires, newLast := computeFires(everySecond{}, time.Unix(100, 0), time.Unix(105, 0), MisfireFireOnce)
	if len(fires) != 1 || !fires[0].Equal(time.Unix(105, 0)) {
		t.Fatalf("FireOnce fires = %v, want [105]", fires)
	}
	if !newLast.Equal(time.Unix(105, 0)) {
		t.Errorf("newLast = %v, want 105", newLast)
	}
}
