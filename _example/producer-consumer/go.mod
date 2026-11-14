module example

go 1.26.10

require (
	github.com/appleboy/graceful v1.3.0
	github.com/golang-queue/queue v0.6.2
	github.com/golang-queue/redisdb-stream v0.4.0
)

require (
	github.com/appleboy/com v1.2.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jpillora/backoff v1.1.0 // indirect
	github.com/redis/go-redis/v9 v9.23.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
)

replace github.com/golang-queue/redisdb-stream => ../../
