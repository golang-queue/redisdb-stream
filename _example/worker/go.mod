module example

go 1.16

require (
	github.com/golang-queue/queue v0.1.0
	github.com/golang-queue/redisdb-stream v0.0.0-20220424021550-bac6de373624
)

replace github.com/golang-queue/redisdb-stream => ../../
