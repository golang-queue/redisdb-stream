package redisdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
	"github.com/golang-queue/queue/job"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func setupRedisCluserContainer(ctx context.Context, t *testing.T) (testcontainers.Container, string) {
	req := testcontainers.ContainerRequest{
		Image: "vishnunair/docker-redis-cluster:latest",
		ExposedPorts: []string{
			"6379/tcp",
			"6380/tcp",
			"6381/tcp",
			"6382/tcp",
			"6383/tcp",
			"6384/tcp",
		},
		WaitingFor: wait.NewExecStrategy(
			[]string{"redis-cli", "-h", "localhost", "-p", "6379", "cluster", "info"},
		),
	}
	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	endpoint, err := redisC.Endpoint(ctx, "")
	require.NoError(t, err)

	return redisC, endpoint
}

func setupRedisContainer(ctx context.Context, t *testing.T) (testcontainers.Container, string) {
	req := testcontainers.ContainerRequest{
		Image:        "redis:6",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor: wait.NewExecStrategy(
			[]string{"redis-cli", "-h", "localhost", "-p", "6379", "ping"},
		),
	}
	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	endpoint, err := redisC.Endpoint(ctx, "")
	require.NoError(t, err)

	return redisC, endpoint
}

func TestWithRedis(t *testing.T) {
	ctx := context.Background()
	redisC, _ := setupRedisContainer(ctx, t)
	testcontainers.CleanupContainer(t, redisC)
}

type mockMessage struct {
	Message string
}

func (m mockMessage) Bytes() []byte {
	return []byte(m.Message)
}

func TestRedisDefaultFlow(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)

	m := &mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("test"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	assert.NoError(t, q.Queue(m))
	q.Start()
	time.Sleep(100 * time.Millisecond)
	q.Release()
}

func TestRedisShutdown(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("test2"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(1 * time.Second)
	q.Shutdown()
	// check shutdown once
	assert.Error(t, w.Shutdown())
	assert.Equal(t, queue.ErrQueueShutdown, w.Shutdown())
	q.Wait()
}

func TestCustomFuncAndWait(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := &mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("test3"),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			time.Sleep(500 * time.Millisecond)
			return nil
		}),
	)
	q := queue.NewPool(
		5,
		queue.WithWorker(w),
	)
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	time.Sleep(1000 * time.Millisecond)
	q.Release()
	// you will see the execute time > 1000ms
}

func TestRedisCluster(t *testing.T) {
	t.Skip()

	ctx := context.Background()
	redisC, _ := setupRedisCluserContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)

	masterPort, err := redisC.MappedPort(ctx, "6379")
	assert.NoError(t, err)

	slavePort, err := redisC.MappedPort(ctx, "6382")
	assert.NoError(t, err)

	hostIP, err := redisC.Host(ctx)
	assert.NoError(t, err)

	m := &mockMessage{
		Message: "foo",
	}

	masterName := fmt.Sprintf("%s:%s", hostIP, masterPort.Port())
	slaveName := fmt.Sprintf("%s:%s", hostIP, slavePort.Port())

	hosts := []string{masterName, slaveName}

	w := NewWorker(
		WithAddr(strings.Join(hosts, ",")),
		WithStreamName("testCluster"),
		WithCluster(),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			time.Sleep(500 * time.Millisecond)
			return nil
		}),
	)
	q := queue.NewPool(
		5,
		queue.WithWorker(w),
	)
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	time.Sleep(1000 * time.Millisecond)
	q.Release()
	// you will see the execute time > 1000ms
}

func TestEnqueueJobAfterShutdown(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(endpoint),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	q.Shutdown()
	// can't queue task after shutdown
	err = q.Queue(m)
	assert.Error(t, err)
	assert.Equal(t, queue.ErrQueueShutdown, err)
	q.Wait()
}

func TestJobReachTimeout(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("timeout"),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Payload()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, q.Queue(m, job.AllowOption{
		Timeout: job.Time(20 * time.Millisecond),
	}))
	time.Sleep(2 * time.Second)
	q.Shutdown()
	q.Wait()
}

func TestCancelJobAfterShutdown(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "test",
	}
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("cancel"),
		WithLogger(queue.NewLogger()),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Payload()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, q.Queue(m, job.AllowOption{
		Timeout: job.Time(3 * time.Second),
	}))
	time.Sleep(2 * time.Second)
	q.Shutdown()
	q.Wait()
}

func TestGoroutineLeak(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("GoroutineLeak"),
		WithLogger(queue.NewEmptyLogger()),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Payload()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
					log.Println("get data:", string(m.Payload()))
					time.Sleep(50 * time.Millisecond)
					return nil
				}
			}
		}),
	)
	q, err := queue.NewQueue(
		queue.WithLogger(queue.NewEmptyLogger()),
		queue.WithWorker(w),
		queue.WithWorkerCount(10),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 50; i++ {
		m.Message = fmt.Sprintf("foobar: %d", i+1)
		assert.NoError(t, q.Queue(m))
	}
	time.Sleep(1 * time.Second)
	q.Release()
	time.Sleep(1 * time.Second)
	fmt.Println("number of goroutines:", runtime.NumGoroutine())
}

func TestGoroutinePanic(t *testing.T) {
	ctx := context.Background()
	redisC, endpoint := setupRedisContainer(ctx, t)
	defer testcontainers.CleanupContainer(t, redisC)
	m := mockMessage{
		Message: "foo",
	}
	w := NewWorker(
		WithAddr(endpoint),
		WithStreamName("GoroutinePanic"),
		WithRunFunc(func(ctx context.Context, m core.TaskMessage) error {
			panic("missing something")
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	time.Sleep(200 * time.Millisecond)
	q.Shutdown()
	assert.Error(t, q.Queue(m))
	q.Wait()
}
