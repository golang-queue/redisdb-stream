module example

go 1.16

require (
	github.com/appleboy/graceful v0.0.4
	github.com/golang-queue/queue v0.0.13-0.20220423074840-a2e1b18a04ae
	github.com/golang-queue/redisdb-stream v0.0.0-20220424021550-bac6de373624 // indirect
)

replace github.com/golang-queue/redisdb-stream => ../../
