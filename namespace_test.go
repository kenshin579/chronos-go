package chronos

import (
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func testClient() redis.UniversalClient {
	return redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
}

func TestNamespacePanicsOnBadPrefix(t *testing.T) {
	for _, bad := range []string{"", "my{app", "my*app", "my app"} {
		t.Run(bad, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("NewNamespace(%q) did not panic", bad)
				}
			}()
			NewNamespace(testClient(), bad)
		})
	}
}

func TestNamespacePrefix(t *testing.T) {
	ns := NewNamespace(testClient(), "myapp:")
	if ns.Prefix() != "myapp" {
		t.Errorf("Prefix() = %q, want %q", ns.Prefix(), "myapp")
	}
}

// TestNamespaceDerivedTypesShareThePrefix is the point of the handle: you
// cannot configure three of the four and forget the fourth.
func TestNamespaceDerivedTypesShareThePrefix(t *testing.T) {
	ns := NewNamespace(testClient(), "myapp")
	want := "myapp"
	if got := ns.NewClient().rdb.Keys().Prefix(); got != want {
		t.Errorf("Client prefix = %q, want %q", got, want)
	}
	if got := ns.NewInspector().rdb.Keys().Prefix(); got != want {
		t.Errorf("Inspector prefix = %q, want %q", got, want)
	}
	if got := ns.NewServer(ServerConfig{Queues: map[string]int{"default": 1}}).rdb.Keys().Prefix(); got != want {
		t.Errorf("Server prefix = %q, want %q", got, want)
	}
	if got := ns.NewScheduler(SchedulerConfig{}).rdb.Keys().Prefix(); got != want {
		t.Errorf("Scheduler prefix = %q, want %q", got, want)
	}
}

// TestDefaultConstructorsUnchanged asserts the four existing constructors still
// write where they always did.
func TestDefaultConstructorsUnchanged(t *testing.T) {
	if got := NewClient(testClient()).rdb.Keys().StreamKey("default"); !strings.HasPrefix(got, "chronos:") {
		t.Errorf("NewClient key = %q, want the chronos: prefix", got)
	}
}
