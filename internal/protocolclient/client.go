package protocolclient

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// Response is a decoded protocol response.
type Response struct {
	Header protocol.ResponseHeader
	Body   codec.ResponseBody
}

// RetryOptions configures request retry and backoff.
type RetryOptions struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// Options configures protocol client behavior.
type Options struct {
	RequestTimeout time.Duration
	Retry          RetryOptions
}

// Client sends Stellflow protocol requests through a transport pool.
type Client struct {
	pool     *transport.Pool
	registry *codec.Registry
	clientID string
	options  Options
	nextID   atomic.Int32
}

// New creates a protocol client.
func New(pool *transport.Pool, registry *codec.Registry, clientID string) *Client {
	return NewWithOptions(pool, registry, clientID, Options{})
}

// NewWithOptions creates a protocol client with explicit options.
func NewWithOptions(pool *transport.Pool, registry *codec.Registry, clientID string, options Options) *Client {
	if registry == nil {
		registry = codec.DefaultRegistry()
	}
	options = normalizeOptions(options)
	return &Client{pool: pool, registry: registry, clientID: clientID, options: options}
}

// Send encodes, sends, and decodes one request.
func (c *Client) Send(ctx context.Context, endpoint transport.Endpoint, apiKey protocol.ApiKey, apiVersion int16, body codec.RequestBody) (Response, error) {
	if c.pool == nil {
		return Response{}, errors.New("protocol client requires transport pool")
	}
	requestBody, err := c.registry.EncodeRequestBody(apiKey, apiVersion, body)
	if err != nil {
		return Response{}, err
	}
	clientID := c.clientID
	if c.options.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.options.RequestTimeout)
		defer cancel()
	}
	var lastErr error
	for attempt := 1; attempt <= c.options.Retry.MaxAttempts; attempt++ {
		header := protocol.RequestHeader{
			APIKey:        apiKey,
			APIVersion:    apiVersion,
			HeaderVersion: protocol.DefaultHeaderVersion,
			CorrelationID: c.nextCorrelationID(),
			ClientID:      &clientID,
		}
		conn, err := c.pool.Get(ctx, endpoint)
		if err != nil {
			lastErr = err
			if !c.canRetry(ctx, attempt, err) {
				return Response{}, err
			}
			if err := c.backoff(ctx, attempt); err != nil {
				return Response{}, err
			}
			continue
		}
		rawResponse, err := conn.Send(ctx, transport.Request{Header: header, Body: requestBody})
		if err != nil {
			if isTransportRetryable(err) {
				c.pool.Invalidate(endpoint, conn)
			}
			lastErr = err
			if !c.canRetry(ctx, attempt, err) {
				return Response{}, err
			}
			if err := c.backoff(ctx, attempt); err != nil {
				return Response{}, err
			}
			continue
		}
		if rawResponse.Header.ErrorCode != protocol.ErrorCodeNone {
			err := &protocol.ClientError{Code: rawResponse.Header.ErrorCode, Message: "request failed"}
			lastErr = err
			if !c.canRetry(ctx, attempt, err) {
				return Response{Header: rawResponse.Header}, err
			}
			if err := c.backoff(ctx, attempt); err != nil {
				return Response{}, err
			}
			continue
		}
		responseBody, err := c.registry.DecodeResponseBody(apiKey, apiVersion, rawResponse.Body)
		if err != nil {
			return Response{}, err
		}
		return Response{Header: rawResponse.Header, Body: responseBody}, nil
	}
	return Response{}, lastErr
}

// APIVersions sends ApiVersions to endpoint.
func (c *Client) APIVersions(ctx context.Context, endpoint transport.Endpoint) (message.APIVersionsResponseBody, error) {
	body := message.APIVersionsRequestBody{
		ClientSoftwareName:    stringPtr("stellflow-go-sdk"),
		ClientSoftwareVersion: stringPtr("0.0.1"),
		SupportedFeatures:     []string{},
	}
	response, err := c.Send(ctx, endpoint, protocol.ApiKeyAPIVersions, protocol.DefaultAPIVersion, body)
	if err != nil {
		return message.APIVersionsResponseBody{}, err
	}
	typed, ok := response.Body.(message.APIVersionsResponseBody)
	if !ok {
		return message.APIVersionsResponseBody{}, errors.New("unexpected ApiVersions response body")
	}
	return typed, nil
}

// Metadata sends Metadata to endpoint.
func (c *Client) Metadata(ctx context.Context, endpoint transport.Endpoint, topics []string) (message.MetadataResponseBody, error) {
	requestTopics := make([]message.MetadataTopicRequest, 0, len(topics))
	for _, topic := range topics {
		requestTopics = append(requestTopics, message.MetadataTopicRequest{Topic: topic})
	}
	body := message.MetadataRequestBody{Topics: requestTopics}
	response, err := c.Send(ctx, endpoint, protocol.ApiKeyMetadata, protocol.DefaultAPIVersion, body)
	if err != nil {
		return message.MetadataResponseBody{}, err
	}
	typed, ok := response.Body.(message.MetadataResponseBody)
	if !ok {
		return message.MetadataResponseBody{}, errors.New("unexpected Metadata response body")
	}
	return typed, nil
}

func (c *Client) nextCorrelationID() int32 {
	next := c.nextID.Add(1)
	if next <= 0 {
		c.nextID.Store(1)
		return 1
	}
	return next
}

func stringPtr(value string) *string {
	return &value
}

func normalizeOptions(options Options) Options {
	if options.Retry.MaxAttempts == 0 {
		options.Retry.MaxAttempts = 3
	}
	if options.Retry.MaxAttempts < 0 {
		options.Retry.MaxAttempts = 1
	}
	if options.Retry.InitialBackoff == 0 {
		options.Retry.InitialBackoff = 100 * time.Millisecond
	}
	if options.Retry.MaxBackoff == 0 {
		options.Retry.MaxBackoff = time.Second
	}
	if options.Retry.MaxBackoff < options.Retry.InitialBackoff {
		options.Retry.MaxBackoff = options.Retry.InitialBackoff
	}
	return options
}

func (c *Client) canRetry(ctx context.Context, attempt int, err error) bool {
	if attempt >= c.options.Retry.MaxAttempts {
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	return protocol.IsRetriable(err) || isTransportRetryable(err)
}

func (c *Client) backoff(ctx context.Context, attempt int) error {
	delay := c.backoffDelay(attempt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) backoffDelay(attempt int) time.Duration {
	delay := c.options.Retry.InitialBackoff
	for range attempt - 1 {
		delay *= 2
		if delay >= c.options.Retry.MaxBackoff {
			return c.options.Retry.MaxBackoff
		}
	}
	if delay > c.options.Retry.MaxBackoff {
		return c.options.Retry.MaxBackoff
	}
	return delay
}

func isTransportRetryable(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}
