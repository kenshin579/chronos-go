package base

import (
	"fmt"
	"strings"
	"unicode"
)

// DefaultPrefix is the key prefix used when none is configured. It preserves
// the key layout chronos-go has always written, so an existing deployment that
// does not opt into a namespace sees byte-identical keys.
const DefaultPrefix = "chronos"

// NormalizePrefix validates a key prefix and returns it with trailing colons
// removed, so "myapp" and "myapp:" behave identically.
//
// It panics on an invalid prefix rather than returning an error. A prefix is
// almost always a compile-time constant, and every rejected character causes a
// silent failure rather than a loud one — braces collapse every queue onto a
// single cluster slot, and glob metacharacters corrupt the SCAN pattern
// ScanSchedules derives from ScheduleLastFiredKey. Failing at startup is
// strictly safer than misrouting every key. This matches AddHandler, which
// panics on a duplicate Kind for the same reason.
func NormalizePrefix(prefix string) string {
	p := strings.TrimRight(prefix, ":")
	if p == "" {
		panic(fmt.Sprintf("chronos: key prefix %q is empty", prefix))
	}
	for _, r := range p {
		switch {
		case r == '{' || r == '}':
			// The hash tag is what keeps a queue's keys in one cluster slot.
			// A brace in the prefix moves or fixes the tag: "my{app}:{q}:stream"
			// tags on "app" for every queue, collapsing the whole deployment
			// onto one slot with no error.
			panic(fmt.Sprintf("chronos: key prefix %q must not contain braces", prefix))
		case r == '*' || r == '?' || r == '[' || r == ']':
			// ScanSchedules builds a SCAN pattern from the key builder; a glob
			// metacharacter here silently matches the wrong keys.
			panic(fmt.Sprintf("chronos: key prefix %q must not contain glob metacharacters", prefix))
		case unicode.IsSpace(r) || unicode.IsControl(r):
			panic(fmt.Sprintf("chronos: key prefix %q must not contain whitespace or control characters", prefix))
		}
	}
	return p
}
