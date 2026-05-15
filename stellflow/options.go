package stellflow

import (
	"time"

	"github.com/stellhub/stellflow-go-sdk/consumer"
)

// Options configures the shared SDK client factory.
type Options struct {
	BootstrapServers []string
	ClientID         string
	MaxFrameLength   int
	RequestTimeout   time.Duration
	Consumer         consumer.Options
}
