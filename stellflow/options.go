package stellflow

import (
	"time"

	"github.com/stellhub/stellflow-go-sdk/consumer"
	"github.com/stellhub/stellflow-go-sdk/producer"
)

// Options configures the shared SDK client factory.
type Options struct {
	BootstrapServers []string
	ClientID         string
	MaxFrameLength   int
	RequestTimeout   time.Duration
	Retry            RetryOptions
	Producer         producer.Options
	Consumer         consumer.Options
}

// RetryOptions configures request retry and backoff.
type RetryOptions struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}
