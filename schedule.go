package chronos

import "time"

// MisfirePolicy decides what happens when a schedule's triggers were missed
// (e.g. during a leader-election gap or downtime).
type MisfirePolicy int

const (
	// MisfireSkip (default) discards missed triggers and resumes on schedule.
	MisfireSkip MisfirePolicy = iota
	// MisfireFireOnce fires exactly one catch-up trigger if any were missed.
	MisfireFireOnce
)

// cronSchedule is the minimal schedule interface computeFires needs. It matches
// robfig/cron's Schedule (Next returns the first activation time after t).
type cronSchedule interface {
	Next(time.Time) time.Time
}

// computeFires determines which trigger times to enqueue given the last fired
// time, the current time, and the misfire policy. It returns the trigger times
// to enqueue (0, or 1) and the new lastFired to persist.
//
//   - Nothing due (next trigger after now): no fire, lastFired unchanged.
//   - Exactly one trigger due (normal tick): fire it.
//   - Multiple triggers elapsed (a gap): Skip fires none and fast-forwards to the
//     latest elapsed trigger; FireOnce fires once (for the latest) then resumes.
//
// It never returns more than one trigger — chronos does not replay every missed
// tick (that risks a flood); MisfireRunAll is intentionally unsupported.
func computeFires(s cronSchedule, lastFired, now time.Time, policy MisfirePolicy) (fires []time.Time, newLastFired time.Time) {
	next := s.Next(lastFired)
	if next.After(now) {
		return nil, lastFired // nothing due yet
	}

	// Find the latest trigger that is <= now.
	latest := next
	for {
		n := s.Next(latest)
		if n.After(now) {
			break
		}
		latest = n
	}

	gap := latest.After(next) // more than one trigger elapsed
	switch {
	case !gap:
		return []time.Time{next}, next // normal single tick
	case policy == MisfireFireOnce:
		return []time.Time{latest}, latest // one catch-up
	default: // MisfireSkip
		return nil, latest // discard missed, fast-forward
	}
}
