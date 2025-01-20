module example

go 1.22

require (
	github.com/appleboy/graceful v1.1.1
	github.com/golang-queue/queue v0.3.0
	github.com/golang-queue/redisdb-stream v0.0.0-20220424021550-bac6de373624
)

require (
	github.com/appleboy/com v0.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/redis/go-redis/v9 v9.7.0 // indirect
)

replace github.com/golang-queue/redisdb-stream => ../../
