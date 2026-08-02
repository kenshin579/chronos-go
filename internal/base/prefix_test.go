package base

import "testing"

func TestNormalizePrefix(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"chronos", "chronos"},
		{"inspireme", "inspireme"},
		{"myapp:", "myapp"},                        // trailing colon trimmed
		{"myapp:::", "myapp"},                      // repeated trailing colons trimmed
		{":myapp", "myapp"},                        // leading colon trimmed — would yield ":myapp:{q}:stream"
		{":myapp:", "myapp"},                       // both ends trimmed
		{"chronos:inspireme", "chronos:inspireme"}, // interior colon kept
	}
	for _, tt := range tests {
		if got := NormalizePrefix(tt.in); got != tt.want {
			t.Errorf("NormalizePrefix(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNormalizePrefixPanics(t *testing.T) {
	bad := []struct {
		in     string
		reason string
	}{
		{"", "empty"},
		{":", "empty after trimming"},
		{"my{app", "opening brace corrupts the hash tag"},
		{"my}app", "closing brace corrupts the hash tag"},
		{"my*app", "glob metacharacter corrupts SCAN patterns"},
		{"my?app", "glob metacharacter corrupts SCAN patterns"},
		{"my[app", "glob metacharacter corrupts SCAN patterns"},
		{"my]app", "glob metacharacter corrupts SCAN patterns"},
		{"my app", "whitespace"},
		{"my\tapp", "whitespace"},
		{"my\napp", "whitespace"},
		{"my\x00app", "control character"},
	}
	for _, tt := range bad {
		t.Run(tt.in, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("NormalizePrefix(%q) did not panic (%s)", tt.in, tt.reason)
				}
			}()
			NormalizePrefix(tt.in)
		})
	}
}
