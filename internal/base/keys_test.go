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
