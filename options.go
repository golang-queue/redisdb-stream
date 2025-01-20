package redisdb

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/golang-queue/queue"
	"github.com/golang-queue/queue/core"
)

// Option for queue system
type Option func(*options)

type options struct {
	runFunc          func(context.Context, core.TaskMessage) error
	logger           queue.Logger
	addr             string
	db               int
	connectionString string
	username         string
	password         string
	streamName       string
	cluster          bool
	group            string
	consumer         string
	maxLength        int64
	blockTime        time.Duration
	tls              *tls.Config
}

// WithAddr setup the addr of redis
func WithAddr(addr string) Option {
	return func(w *options) {
		w.addr = addr
	}
}

// WithMaxLength setup the max length for publish messages
func WithMaxLength(m int64) Option {
	return func(w *options) {
		w.maxLength = m
	}
}

// WithBlockTime setup the block time for publish messages
// we use the block command to make sure if no entry is found we wait
// until an entry is found
func WithBlockTime(m time.Duration) Option {
	return func(w *options) {
		w.blockTime = m
	}
}

// WithPassword redis password
func WithDB(db int) Option {
	return func(w *options) {
		w.db = db
	}
}

// WithCluster redis cluster
func WithCluster() Option {
	return func(w *options) {
		w.cluster = true
	}
}

// WithStreamName Stream name
func WithStreamName(name string) Option {
	return func(w *options) {
		w.streamName = name
	}
}

// WithGroup group name
func WithGroup(name string) Option {
	return func(w *options) {
		w.group = name
	}
}

// WithConsumer consumer name
func WithConsumer(name string) Option {
	return func(w *options) {
		w.consumer = name
	}
}

// WithUsername redis username
// This is only used for redis cluster
func WithUsername(username string) Option {
	return func(w *options) {
		w.username = username
	}
}

// WithPassword redis password
func WithPassword(passwd string) Option {
	return func(w *options) {
		w.password = passwd
	}
}

// WithConnectionString redis connection string
func WithConnectionString(connectionString string) Option {
	return func(w *options) {
		w.connectionString = connectionString
	}
}

// WithRunFunc setup the run func of queue
func WithRunFunc(fn func(context.Context, core.TaskMessage) error) Option {
	return func(w *options) {
		w.runFunc = fn
	}
}

// WithLogger set custom logger
func WithLogger(l queue.Logger) Option {
	return func(w *options) {
		w.logger = l
	}
}

// WithTLS returns an Option that configures the use of TLS for the connection.
// It sets the minimum TLS version to TLS 1.2.
func WithTLS() Option {
	return func(w *options) {
		w.tls = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}
}

// WithSkipTLSVerify returns an Option that configures the TLS settings to skip
// verification of the server's certificate. This is useful for connecting to
// servers with self-signed certificates or when certificate verification is
// not required. Use this option with caution as it makes the connection
// susceptible to man-in-the-middle attacks.
func WithSkipTLSVerify() Option {
	return func(w *options) {
		if w.tls == nil {
			w.tls = &tls.Config{
				InsecureSkipVerify: true, //nolint: gosec

			}
			return
		}
		w.tls.InsecureSkipVerify = true
	}
}

func newOptions(opts ...Option) options {
	defaultOpts := options{
		streamName: "golang-queue",
		group:      "golang-queue",
		consumer:   "golang-queue",
		logger:     queue.NewLogger(),
		runFunc: func(context.Context, core.TaskMessage) error {
			return nil
		},
		blockTime: 60 * time.Second,
	}

	// Loop through each option
	for _, opt := range opts {
		// Call the option giving the instantiated
		opt(&defaultOpts)
	}

	return defaultOpts
}
