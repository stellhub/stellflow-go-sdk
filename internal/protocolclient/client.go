package protocolclient

import (
	"context"
	"errors"
	"sync/atomic"

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

// Client sends Stellflow protocol requests through a transport pool.
type Client struct {
	pool     *transport.Pool
	registry *codec.Registry
	clientID string
	nextID   atomic.Int32
}

// New creates a protocol client.
func New(pool *transport.Pool, registry *codec.Registry, clientID string) *Client {
	if registry == nil {
		registry = codec.DefaultRegistry()
	}
	return &Client{pool: pool, registry: registry, clientID: clientID}
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
	header := protocol.RequestHeader{
		APIKey:        apiKey,
		APIVersion:    apiVersion,
		HeaderVersion: protocol.DefaultHeaderVersion,
		CorrelationID: c.nextCorrelationID(),
		ClientID:      &clientID,
	}
	conn, err := c.pool.Get(ctx, endpoint)
	if err != nil {
		return Response{}, err
	}
	rawResponse, err := conn.Send(ctx, transport.Request{Header: header, Body: requestBody})
	if err != nil {
		return Response{}, err
	}
	if rawResponse.Header.ErrorCode != protocol.ErrorCodeNone {
		return Response{Header: rawResponse.Header}, &protocol.ClientError{Code: rawResponse.Header.ErrorCode, Message: "request failed"}
	}
	responseBody, err := c.registry.DecodeResponseBody(apiKey, apiVersion, rawResponse.Body)
	if err != nil {
		return Response{}, err
	}
	return Response{Header: rawResponse.Header, Body: responseBody}, nil
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
