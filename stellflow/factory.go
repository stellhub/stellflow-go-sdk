package stellflow

import (
	"fmt"
	"time"

	"github.com/stellhub/stellflow-go-sdk/admin"
	"github.com/stellhub/stellflow-go-sdk/consumer"
	imetadata "github.com/stellhub/stellflow-go-sdk/internal/metadata"
	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/producer"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
)

// ClientFactory owns shared transport, protocol, and metadata infrastructure.
type ClientFactory struct {
	options  Options
	pool     *transport.Pool
	protocol *protocolclient.Client
	metadata *imetadata.Manager
}

// NewClientFactory creates a shared SDK factory.
func NewClientFactory(options Options) (*ClientFactory, error) {
	if len(options.BootstrapServers) == 0 {
		return nil, fmt.Errorf("bootstrap servers must not be empty")
	}
	if options.ClientID == "" {
		options.ClientID = "stellflow-go-sdk"
	}
	if options.RequestTimeout == 0 {
		options.RequestTimeout = 30 * time.Second
	}
	endpoints := make([]transport.Endpoint, 0, len(options.BootstrapServers))
	for _, value := range options.BootstrapServers {
		endpoint, err := transport.ParseEndpoint(value)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, endpoint)
	}
	pool := transport.NewPool(options.MaxFrameLength)
	protocolClient := protocolclient.NewWithOptions(pool, codec.DefaultRegistry(), options.ClientID, protocolclient.Options{
		RequestTimeout: options.RequestTimeout,
		Retry: protocolclient.RetryOptions{
			MaxAttempts:    options.Retry.MaxAttempts,
			InitialBackoff: options.Retry.InitialBackoff,
			MaxBackoff:     options.Retry.MaxBackoff,
		},
	})
	metadataManager := imetadata.New(protocolClient, endpoints)
	return &ClientFactory{
		options:  options,
		pool:     pool,
		protocol: protocolClient,
		metadata: metadataManager,
	}, nil
}

// NewAdmin creates an Admin client.
func (f *ClientFactory) NewAdmin() *admin.Client {
	return admin.New(f.protocol, f.metadata)
}

// NewProducer creates a Producer client.
func (f *ClientFactory) NewProducer() *producer.Client {
	return producer.New(f.protocol, f.metadata)
}

// NewConsumer creates a Consumer client.
func (f *ClientFactory) NewConsumer() *consumer.Client {
	return consumer.New(f.protocol, f.metadata, f.options.Consumer)
}

// Close closes shared transport resources.
func (f *ClientFactory) Close() error {
	return f.pool.Close()
}
