module github.com/kenshin579/chronos-go/benchmarks

go 1.26

replace github.com/kenshin579/chronos-go => ../

require (
	github.com/kenshin579/chronos-go v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.21.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)
