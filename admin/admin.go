package admin

import (
	"context"
	"errors"

	imetadata "github.com/stellhub/stellflow-go-sdk/internal/metadata"
	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/observability"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const routeRefreshAttempts = 2

// Options configures admin client behavior.
type Options struct {
	Observability observability.Options
}

// Client exposes administrative Stellflow APIs.
type Client struct {
	protocol *protocolclient.Client
	metadata *imetadata.Manager
	logger   observability.Logger
	tracer   trace.Tracer
}

// New creates an admin client.
func New(protocolClient *protocolclient.Client, metadataManager *imetadata.Manager) *Client {
	return NewWithOptions(protocolClient, metadataManager, Options{})
}

// NewWithOptions creates an admin client with explicit options.
func NewWithOptions(protocolClient *protocolclient.Client, metadataManager *imetadata.Manager, options Options) *Client {
	obs := observability.Normalize(options.Observability)
	return &Client{
		protocol: protocolClient,
		metadata: metadataManager,
		logger:   obs.Logger,
		tracer:   observability.Tracer(obs),
	}
}

// APIVersions queries the first bootstrap broker for API versions.
func (c *Client) APIVersions(ctx context.Context) (message.APIVersionsResponseBody, error) {
	ctx, span := c.tracer.Start(ctx, "stellflow.admin.api_versions")
	defer span.End()
	bootstrap, err := c.metadata.BootstrapEndpoint()
	if err != nil {
		c.recordError(ctx, span, "admin api versions failed", err)
		return message.APIVersionsResponseBody{}, err
	}
	response, err := c.protocol.APIVersions(ctx, bootstrap)
	if err != nil {
		c.recordError(ctx, span, "admin api versions failed", err)
		return message.APIVersionsResponseBody{}, err
	}
	return response, nil
}

// Metadata queries broker metadata and refreshes the shared metadata cache.
func (c *Client) Metadata(ctx context.Context, topics []string) (message.MetadataResponseBody, error) {
	ctx, span := c.tracer.Start(ctx, "stellflow.admin.metadata",
		trace.WithAttributes(attribute.Int("stellflow.topic_count", len(topics))),
	)
	defer span.End()
	response, err := c.metadata.Refresh(ctx, topics)
	if err != nil {
		c.recordError(ctx, span, "admin metadata failed", err)
		return message.MetadataResponseBody{}, err
	}
	return response, nil
}

// ClusterDescription is a compact cluster view built from Metadata.
type ClusterDescription struct {
	ClusterID                   *string
	ControllerID                int32
	Brokers                     []message.MetadataBroker
	ClusterAuthorizedOperations int32
}

// DescribeCluster returns cluster metadata.
func (c *Client) DescribeCluster(ctx context.Context) (ClusterDescription, error) {
	response, err := c.Metadata(ctx, nil)
	if err != nil {
		return ClusterDescription{}, err
	}
	return ClusterDescription{
		ClusterID:                   response.ClusterID,
		ControllerID:                response.ControllerID,
		Brokers:                     response.Brokers,
		ClusterAuthorizedOperations: response.ClusterAuthorizedOperations,
	}, nil
}

// ListOffsetsRequest is one offset query.
type ListOffsetsRequest struct {
	Topic              string
	Partition          int32
	CurrentLeaderEpoch int32
	Timestamp          int64
	MaxNumOffsets      int32
}

// ListOffsets queries offsets by routing each request to the partition leader.
func (c *Client) ListOffsets(ctx context.Context, requests []ListOffsetsRequest) (map[imetadata.TopicPartition]message.ListOffsetsPartitionResponse, error) {
	ctx, span := c.tracer.Start(ctx, "stellflow.admin.list_offsets",
		trace.WithAttributes(attribute.Int("stellflow.request_count", len(requests))),
	)
	defer span.End()
	results := make(map[imetadata.TopicPartition]message.ListOffsetsPartitionResponse, len(requests))
	for _, request := range requests {
		var lastErr error
	attemptLoop:
		for attempt := 1; attempt <= routeRefreshAttempts; attempt++ {
			route, err := c.route(ctx, request.Topic, request.Partition, attempt)
			if err != nil {
				return nil, err
			}
			leaderEpoch := request.CurrentLeaderEpoch
			if leaderEpoch == 0 {
				leaderEpoch = route.LeaderEpoch
			}
			maxNumOffsets := request.MaxNumOffsets
			if maxNumOffsets == 0 {
				maxNumOffsets = 1
			}
			body := message.ListOffsetsRequestBody{
				ReplicaID:      -1,
				IsolationLevel: 0,
				Topics: []message.ListOffsetsTopicRequest{
					{
						Topic: request.Topic,
						Partitions: []message.ListOffsetsPartitionRequest{
							{
								Partition:          request.Partition,
								CurrentLeaderEpoch: leaderEpoch,
								Timestamp:          request.Timestamp,
								MaxNumOffsets:      maxNumOffsets,
							},
						},
					},
				},
			}
			response, err := c.protocol.Send(ctx, route.Endpoint, protocol.ApiKeyListOffsets, protocol.DefaultAPIVersion, body)
			if err != nil {
				lastErr = err
				if attempt < routeRefreshAttempts && shouldRefreshRoute(ctx, err) {
					continue
				}
				return nil, err
			}
			typed, ok := response.Body.(message.ListOffsetsResponseBody)
			if !ok {
				return nil, errors.New("unexpected ListOffsets response body")
			}
			for _, topic := range typed.Topics {
				if topic.Topic == nil || *topic.Topic != request.Topic {
					continue
				}
				for _, partition := range topic.Partitions {
					if partition.Partition != request.Partition {
						continue
					}
					if partition.ErrorCode != protocol.ErrorCodeNone {
						err := &protocol.ClientError{Code: partition.ErrorCode, Message: "list offsets failed"}
						lastErr = err
						if attempt < routeRefreshAttempts && shouldRefreshRoute(ctx, err) {
							continue attemptLoop
						}
						return nil, err
					}
					results[imetadata.TopicPartition{Topic: request.Topic, Partition: request.Partition}] = partition
					lastErr = nil
					break attemptLoop
				}
			}
			lastErr = errors.New("list offsets response missing partition result")
		}
		if lastErr != nil {
			return nil, lastErr
		}
	}
	c.logger.Info(ctx, "admin list offsets completed", observability.Int("request_count", len(requests)))
	return results, nil
}

func (c *Client) route(ctx context.Context, topic string, partition int32, attempt int) (imetadata.PartitionRoute, error) {
	if attempt == 1 {
		return c.metadata.Route(ctx, topic, partition)
	}
	return c.metadata.RefreshRoute(ctx, topic, partition)
}

func shouldRefreshRoute(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	if protocol.RequiresMetadataRefresh(err) {
		return true
	}
	var clientErr *protocol.ClientError
	return !errors.As(err, &clientErr)
}

func (c *Client) recordError(ctx context.Context, span trace.Span, msg string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	c.logger.Error(ctx, msg, observability.Error(err))
}
