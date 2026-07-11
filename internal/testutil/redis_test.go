package testutil

import (
	"context"
	"testing"
)

func TestNewRedis_PingSucceeds(t *testing.T) {
	client := NewRedis(t)
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestNewClusterRedis_SkipsWithoutEnv(t *testing.T) {
	t.Setenv("REDIS_CLUSTER_ADDRS", "")
	inner := &testing.T{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// NewClusterRedis must call t.Skip (runtime.Goexit) when env is unset.
		NewClusterRedis(inner)
		t.Error("NewClusterRedis returned instead of skipping")
	}()
	<-done
	if !inner.Skipped() {
		t.Error("expected inner test to be skipped")
	}
}
