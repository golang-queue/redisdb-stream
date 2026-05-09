module example

go 1.25.0

require (
	github.com/golang-queue/queue v0.5.0
	github.com/golang-queue/redisdb-stream v0.3.1
)

require (
	github.com/appleboy/com v1.2.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/redis/go-redis/v9 v9.19.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/golang-queue/redisdb-stream => ../../
