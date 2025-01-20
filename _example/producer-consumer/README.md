# Producer-Consumer

This example demonstrates how to use the `Producer` and `Consumer` classes to create a simple producer-consumer system.

## Producer

The producer is responsible for creating tasks and adding them to the queue. Below is an example of a producer implementation:

```go
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
```

## Consumer

The consumer is responsible for processing the tasks. Below is an example of a consumer implementation:

```go
package main

import (
  "context"
  "encoding/json"
  "fmt"
  "time"

  "github.com/appleboy/graceful"
  "github.com/golang-queue/queue"
  "github.com/golang-queue/queue/core"
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
  taskN := 10000
  rets := make(chan string, taskN)

  m := graceful.NewManager()

  // define the worker
  w := redisdb.NewWorker(
    redisdb.WithAddr("127.0.0.1:6379"),
    redisdb.WithStreamName("foobar"),
    redisdb.WithGroup("foobar"),
    redisdb.WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
      var v job
      if err := json.Unmarshal(m.Payload(), &v); err != nil {
        return err
      }
      rets <- v.Message
      time.Sleep(2 * time.Second)
      return nil
    }),
  )

  // define the queue
  q := queue.NewPool(
    1,
    queue.WithWorker(w),
  )

  m.AddRunningJob(func(ctx context.Context) error {
    for {
      select {
      case <-ctx.Done():
        select {
        case m := <-rets:
          fmt.Println("message:", m)
        default:
        }
        return nil
      case m := <-rets:
        fmt.Println("message:", m)
        time.Sleep(50 * time.Millisecond)
      }
    }
  })

  m.AddShutdownJob(func() error {
    // shutdown the service and notify all the worker
    q.Release()
    return nil
  })

  <-m.Done()
}
```

This example demonstrates how to set up a producer-consumer system using the `Producer` and `Consumer` classes. The producer creates tasks and adds them to the queue, while the consumer processes the tasks from the queue.
