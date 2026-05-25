package producer

import (
	"time"

	"github.com/stellhub/stellflow-go-sdk/observability"
)

// Partitioner selects a partition for a record when Record.Partition is not set.
type Partitioner func(record Record, partitions []int32) (int32, error)

// OrderingStrategy controls how the producer protects per-partition ordering.
type OrderingStrategy int

const (
	// OrderingPerPartition serializes batches for the same topic partition.
	OrderingPerPartition OrderingStrategy = iota
	// OrderingNone allows batches to be sent as soon as broker in-flight capacity is available.
	OrderingNone
)

// Options configures producer batching and partitioning.
type Options struct {
	Acks                          int16
	TimeoutMs                     int32
	DeliveryTimeout               time.Duration
	RequestTimeout                time.Duration
	RetryMaxAttempts              int
	RetryBackoff                  time.Duration
	MaxInFlight                   int
	Ordering                      OrderingStrategy
	BatchSize                     int
	BatchBytes                    int
	Linger                        time.Duration
	QueueSize                     int
	Partitioner                   Partitioner
	DisableAutoCreateTopics       bool
	AutoCreateTopicPartitionCount int32
	Idempotent                    bool
	ProducerID                    int64
	ProducerEpoch                 int16
	TransactionalID               string
	Observability                 observability.Options
}

func normalizeOptions(options Options) Options {
	if options.Acks == 0 {
		options.Acks = -1
	}
	if options.TimeoutMs == 0 {
		options.TimeoutMs = 30000
	}
	if options.DeliveryTimeout <= 0 {
		options.DeliveryTimeout = 2 * time.Minute
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = 30 * time.Second
	}
	if options.RetryMaxAttempts <= 0 {
		options.RetryMaxAttempts = 2
	}
	if options.RetryBackoff <= 0 {
		options.RetryBackoff = 100 * time.Millisecond
	}
	if options.MaxInFlight <= 0 {
		options.MaxInFlight = 5
	}
	if options.TransactionalID != "" {
		options.Idempotent = true
	}
	if options.Idempotent && options.Ordering == OrderingNone {
		options.Ordering = OrderingPerPartition
	}
	if options.Idempotent && options.ProducerID == 0 && options.ProducerEpoch == 0 {
		options.ProducerID = -1
		options.ProducerEpoch = -1
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 100
	}
	if options.BatchBytes <= 0 {
		options.BatchBytes = 1024 * 1024
	}
	if options.Linger <= 0 {
		options.Linger = 5 * time.Millisecond
	}
	if options.QueueSize <= 0 {
		options.QueueSize = 1024
	}
	if options.AutoCreateTopicPartitionCount <= 0 {
		options.AutoCreateTopicPartitionCount = 2
	}
	if !options.Idempotent {
		options.ProducerID = -1
		options.ProducerEpoch = -1
	}
	return options
}
