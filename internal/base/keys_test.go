package base

import "testing"

func TestKeyBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"queue prefix", QueueKeyPrefix("default"), "chronos:{default}:"},
		{"stream", StreamKey("default"), "chronos:{default}:stream"},
		{"task", TaskKey("default", "abc"), "chronos:{default}:t:abc"},
		{"queues set", QueuesKey(), "chronos:queues"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}

func TestRetryAndArchivedAndTaskPrefixKeys(t *testing.T) {
	if got := RetryKey("default"); got != "chronos:{default}:retry" {
		t.Errorf("RetryKey = %q", got)
	}
	if got := ArchivedKey("default"); got != "chronos:{default}:archived" {
		t.Errorf("ArchivedKey = %q", got)
	}
	if got := TaskKeyPrefix("default"); got != "chronos:{default}:t:" {
		t.Errorf("TaskKeyPrefix = %q", got)
	}
	// TaskKey는 prefix + id와 일치해야 한다(forward Lua가 prefix로 키를 조립하므로).
	if TaskKeyPrefix("default")+"abc" != TaskKey("default", "abc") {
		t.Error("TaskKeyPrefix + id must equal TaskKey")
	}
}

func TestScheduledAndUniqueKeys(t *testing.T) {
	if got := ScheduledKey("default"); got != "chronos:{default}:scheduled" {
		t.Errorf("ScheduledKey = %q", got)
	}
	if got := UniqueKey("default", "email:send:abc"); got != "chronos:{default}:unique:email:send:abc" {
		t.Errorf("UniqueKey = %q", got)
	}
}
