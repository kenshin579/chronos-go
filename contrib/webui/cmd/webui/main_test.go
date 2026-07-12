package main

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestBuildClient_ModesAndErrors(t *testing.T) {
	// 기본(둘 다 false) = standalone.
	c, err := buildClient(false, false, "127.0.0.1:6379", 3)
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	if _, ok := c.(*redis.Client); !ok {
		t.Errorf("default: got %T, want *redis.Client", c)
	}
	_ = c.Close()

	// cluster: 콤마 분리 + 공백/빈 조각 허용.
	cc, err := buildClient(false, true, " n1:7000 , n2:7001 ,", 0)
	if err != nil {
		t.Fatalf("cluster: %v", err)
	}
	if _, ok := cc.(*redis.ClusterClient); !ok {
		t.Fatalf("cluster: got %T, want *redis.ClusterClient", cc)
	}
	_ = cc.Close()

	// 상호 배타.
	if _, err := buildClient(true, true, "x:1", 0); err == nil {
		t.Error("standalone+cluster: want error")
	}
	// cluster + db != 0.
	if _, err := buildClient(false, true, "x:1", 15); err == nil {
		t.Error("cluster with db!=0: want error")
	}
	// 빈 주소 목록.
	if _, err := buildClient(false, true, " , ", 0); err == nil {
		t.Error("cluster with empty addrs: want error")
	}
}
