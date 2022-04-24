package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/redisdb-stream"
)

type job struct {
	Message string
}

func (j *job) Bytes() []byte {
	b, err := json.Marshal(j)
	if err != nil {
		panic(err)
	}
	return b
}

func main() {
	taskN := 5

	// define the worker
	w := redisdb.NewWorker(
		redisdb.WithAddr("127.0.0.1:6379"),
		redisdb.WithStreamName("foobar"),
	)

	// define the queue
	q := queue.NewPool(
		0,
		queue.WithWorker(w),
	)

	// assign tasks in queue
	for i := 0; i < taskN; i++ {
		go func(i int) {
			if err := q.Queue(&job{
				Message: fmt.Sprintf("handle the job: %d", i+1),
			}); err != nil {
				log.Fatal(err)
			}
		}(i)
	}

	time.Sleep(2 * time.Second)
	// shutdown the service and notify all the worker
	q.Release()
}
