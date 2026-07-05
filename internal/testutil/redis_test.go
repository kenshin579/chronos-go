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
