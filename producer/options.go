package producer

import (
	"time"

	"github.com/stellhub/stellflow-go-sdk/observability"
)

// Partitioner selects a partition for a record when Record.Partition is not set.
type Partitioner func(record Record, partitions []int32) (int32, error)

// Options configures producer batching and partitioning.
type Options struct {
	Acks          int16
	TimeoutMs     int32
	BatchSize     int
	BatchBytes    int
	Linger        time.Duration
	QueueSize     int
	Partitioner   Partitioner
	Observability observability.Options
}

func normalizeOptions(options Options) Options {
	if options.Acks == 0 {
		options.Acks = -1
	}
	if options.TimeoutMs == 0 {
		options.TimeoutMs = 30000
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
	return options
}
