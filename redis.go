package redisdb

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"

	"github.com/go-redis/redis/v9"
)

var _ core.Worker = (*Worker)(nil)

const blockTime = 60000

// Worker for Redis
type Worker struct {
	// redis config
	rdb       redis.Cmdable
	tasks     chan redis.XMessage
	stopFlag  int32
	stopOnce  sync.Once
	startOnce sync.Once
	stop      chan struct{}
	exit      chan struct{}
	opts      options
}

// NewWorker for struc
func NewWorker(opts ...Option) *Worker {
	var err error
	w := &Worker{
		opts:  newOptions(opts...),
		stop:  make(chan struct{}),
		exit:  make(chan struct{}),
		tasks: make(chan redis.XMessage),
	}

	if w.opts.connectionString != "" {
		options, err := redis.ParseURL(w.opts.connectionString)
		if err != nil {
			w.opts.logger.Fatal(err)
		}
		w.rdb = redis.NewClient(options)
	} else if w.opts.addr != "" {
		if w.opts.cluster {
			w.rdb = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    strings.Split(w.opts.addr, ","),
				Password: w.opts.password,
			})
		} else {
			options := &redis.Options{
				Addr:     w.opts.addr,
				Password: w.opts.password,
				DB:       w.opts.db,
			}
			w.rdb = redis.NewClient(options)
		}
	}

	_, err = w.rdb.Ping(context.Background()).Result()
	if err != nil {
		w.opts.logger.Fatal(err)
	}

	return w
}

func (w *Worker) startConsumer() {
	w.startOnce.Do(func() {
		if err := w.rdb.XGroupCreateMkStream(
			context.Background(),
			w.opts.streamName,
			w.opts.group,
			"$",
		).Err(); err != nil {
			w.opts.logger.Error(err)
		}

		go w.fetchTask()
	})
}

func (w *Worker) fetchTask() {
	for {
		select {
		case <-w.stop:
			return
		default:
		}

		ctx := context.Background()
		data, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    w.opts.group,
			Consumer: w.opts.consumer,
			Streams:  []string{w.opts.streamName, ">"},
			// count is number of entries we want to read from redis
			Count: 1,
			// we use the block command to make sure if no entry is found we wait
			// until an entry is found
			Block: blockTime,
		}).Result()
		if err != nil {
			w.opts.logger.Errorf("error while reading from redis %v",err)
			continue
		}
		// we have received the data we should loop it and queue the messages
		// so that our tasks can start processing
		for _, result := range data {
			for _, message := range result.Messages {
				select {
				case w.tasks <- message:
					if err := w.rdb.XAck(ctx, w.opts.streamName, w.opts.group, message.ID).Err(); err != nil {
						w.opts.logger.Errorf("can't ack message: %s", message.ID)
					}
				case <-w.stop:
					// Todo: re-queue the task
					w.opts.logger.Info("re-queue the task: ", message.ID)
					if err := w.queue(message.Values); err != nil {
						w.opts.logger.Error("error to re-queue the task: ", message.ID)
					}
					close(w.exit)
					return
				}
			}
		}
	}
}

func (w *Worker) handle(job *queue.Job) error {
	// create channel with buffer size 1 to avoid goroutine leak
	done := make(chan error, 1)
	panicChan := make(chan interface{}, 1)
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), job.Timeout)
	defer func() {
		cancel()
	}()

	// run the job
	go func() {
		// handle panic issue
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		// run custom process function
		done <- w.opts.runFunc(ctx, job)
	}()

	select {
	case p := <-panicChan:
		panic(p)
	case <-ctx.Done(): // timeout reached
		return ctx.Err()
	case <-w.stop: // shutdown service
		// cancel job
		cancel()

		leftTime := job.Timeout - time.Since(startTime)
		// wait job
		select {
		case <-time.After(leftTime):
			return context.DeadlineExceeded
		case err := <-done: // job finish
			return err
		case p := <-panicChan:
			panic(p)
		}
	case err := <-done: // job finish
		return err
	}
}

// Shutdown worker
func (w *Worker) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return queue.ErrQueueShutdown
	}

	w.stopOnce.Do(func() {
		close(w.stop)

		// wait requeue
		select {
		case <-w.exit:
		case <-time.After(200 * time.Millisecond):
		}

		switch v := w.rdb.(type) {
		case *redis.Client:
			v.Close()
		case *redis.ClusterClient:
			v.Close()
		}
		close(w.tasks)
	})
	return nil
}

func (w *Worker) queue(data interface{}) error {
	ctx := context.Background()

	// Publish a message.
	err := w.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: w.opts.streamName,
		MaxLen: w.opts.maxLength,
		Values: data,
	}).Err()

	return err
}

// Queue send notification to queue
func (w *Worker) Queue(task core.QueuedMessage) error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return queue.ErrQueueShutdown
	}

	return w.queue(map[string]interface{}{"body": BytesToStr(task.Bytes())})
}

// Run start the worker
func (w *Worker) Run(task core.QueuedMessage) error {
	data, _ := task.(*queue.Job)

	if err := w.handle(data); err != nil {
		return err
	}

	return nil
}

// Request a new task
func (w *Worker) Request() (core.QueuedMessage, error) {
	clock := 0
	w.startConsumer()
loop:
	for {
		select {
		case task, ok := <-w.tasks:
			if !ok {
				return nil, queue.ErrQueueHasBeenClosed
			}
			var data queue.Job
			_ = json.Unmarshal(StrToBytes(task.Values["body"].(string)), &data)
			return &data, nil
		case <-time.After(1 * time.Second):
			if clock == 5 {
				break loop
			}
			clock += 1
		}
	}

	return nil, queue.ErrNoTaskInQueue
}
