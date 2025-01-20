package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
	redisdb "github.com/golang-queue/redisdb-stream"
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
	taskN := 100
	rets := make(chan string, taskN)

	// define the worker
	w := redisdb.NewWorker(
		redisdb.WithAddr("127.0.0.1:6379"),
		redisdb.WithStreamName("foobar"),
		redisdb.WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			var v job
			if err := json.Unmarshal(m.Payload(), &v); err != nil {
				return err
			}
			rets <- v.Message
			return nil
		}),
	)

	// define the queue
	q := queue.NewPool(
		5,
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

	// wait until all tasks done
	for i := 0; i < taskN; i++ {
		fmt.Println("message:", <-rets)
		time.Sleep(50 * time.Millisecond)
	}

	// shutdown the service and notify all the worker
	q.Release()
}
