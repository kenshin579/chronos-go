package base

import "testing"

func TestUniqueSuffix_StableAndPayloadSensitive(t *testing.T) {
	a := UniqueSuffix("email:send", []byte(`{"to":"x"}`))
	b := UniqueSuffix("email:send", []byte(`{"to":"x"}`))
	c := UniqueSuffix("email:send", []byte(`{"to":"y"}`))

	if a != b {
		t.Errorf("same kind+payload must hash equal: %q vs %q", a, b)
	}
	if a == c {
		t.Error("different payload must hash differently")
	}
	// Suffix starts with the kind for readability/debuggability.
	if len(a) <= len("email:send:") || a[:len("email:send:")] != "email:send:" {
		t.Errorf("suffix should start with %q, got %q", "email:send:", a)
	}
}
