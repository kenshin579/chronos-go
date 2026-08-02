package base

import "testing"

// TestDefaultKeysUnchanged is the golden test: an existing deployment that does
// not opt into a namespace must see byte-identical keys. Every literal here is
// the layout chronos-go shipped before prefixes existed.
func TestDefaultKeysUnchanged(t *testing.T) {
	tests := []struct{ got, want string }{
		{QueueKeyPrefix("default"), "chronos:{default}:"},
		{StreamKey("default"), "chronos:{default}:stream"},
		{TaskKey("default", "abc"), "chronos:{default}:t:abc"},
		{TaskKeyPrefix("default"), "chronos:{default}:t:"},
		{RetryKey("default"), "chronos:{default}:retry"},
		{ArchivedKey("default"), "chronos:{default}:archived"},
		{CompletedKey("default"), "chronos:{default}:completed"},
		{ScheduledKey("default"), "chronos:{default}:scheduled"},
		{UniqueKey("default", "k:deadbeef"), "chronos:{default}:unique:k:deadbeef"},
		{GroupKey("cbq", "g1"), "chronos:{cbq}:group:g1"},
		{GroupResultKey("cbq", "g1"), "chronos:{cbq}:groupresult:g1"},
		{PeriodicDedupKey("default", "s1:100"), "chronos:{default}:pdedup:s1:100"},
		{QueuesKey(), "chronos:queues"},
		{PausedKey(), "chronos:paused"},
		{SchedulesKey(), "chronos:schedules"},
		{LeaderKey(), "chronos:leader"},
		{LeaderResignChannel(), "chronos:leader:resign"},
		{ScheduleLastFiredKey("s1"), "chronos:sched:s1:last"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("got %q, want %q", tt.got, tt.want)
		}
	}
}

// TestCustomPrefix asserts the prefix reaches every builder, including the
// global ones — those are the keys that make two applications collide.
func TestCustomPrefix(t *testing.T) {
	k := NewKeys("myapp")
	tests := []struct{ got, want string }{
		{k.QueueKeyPrefix("default"), "myapp:{default}:"},
		{k.StreamKey("default"), "myapp:{default}:stream"},
		{k.TaskKey("default", "abc"), "myapp:{default}:t:abc"},
		{k.TaskKeyPrefix("default"), "myapp:{default}:t:"},
		{k.RetryKey("default"), "myapp:{default}:retry"},
		{k.ArchivedKey("default"), "myapp:{default}:archived"},
		{k.CompletedKey("default"), "myapp:{default}:completed"},
		{k.ScheduledKey("default"), "myapp:{default}:scheduled"},
		{k.UniqueKey("default", "k:deadbeef"), "myapp:{default}:unique:k:deadbeef"},
		{k.GroupKey("cbq", "g1"), "myapp:{cbq}:group:g1"},
		{k.GroupResultKey("cbq", "g1"), "myapp:{cbq}:groupresult:g1"},
		{k.PeriodicDedupKey("default", "s1:100"), "myapp:{default}:pdedup:s1:100"},
		{k.QueuesKey(), "myapp:queues"},
		{k.PausedKey(), "myapp:paused"},
		{k.SchedulesKey(), "myapp:schedules"},
		{k.LeaderKey(), "myapp:leader"},
		{k.LeaderResignChannel(), "myapp:leader:resign"},
		{k.ScheduleLastFiredKey("s1"), "myapp:sched:s1:last"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("got %q, want %q", tt.got, tt.want)
		}
	}
}

// TestTaskKeyPrefixInvariant guards the contract forwardCmd and trimArchivedCmd
// depend on: both receive TaskKeyPrefix as ARGV and concatenate a task ID onto
// it inside Lua. If this ever diverges from TaskKey, those scripts write to keys
// nothing else reads.
func TestTaskKeyPrefixInvariant(t *testing.T) {
	for _, k := range []Keys{DefaultKeys, NewKeys("myapp"), NewKeys("chronos:inspireme")} {
		if k.TaskKeyPrefix("default")+"abc" != k.TaskKey("default", "abc") {
			t.Errorf("prefix %q: TaskKeyPrefix + id must equal TaskKey", k.Prefix())
		}
	}
}

// TestKeysNormalizePrefix asserts the constructor normalizes rather than
// storing the raw string.
func TestKeysNormalizePrefix(t *testing.T) {
	if got := NewKeys("myapp:").StreamKey("q"); got != "myapp:{q}:stream" {
		t.Errorf("trailing colon not normalized: %q", got)
	}
}
