package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

// CreateTopicResult describes a topic creation result.
type CreateTopicResult struct {
	Topic      string
	Created    bool
	Partitions []CreateTopicPartitionResult
}

// CreateTopicPartitionResult describes one created topic partition.
type CreateTopicPartitionResult struct {
	Partition   int32
	ErrorCode   protocol.ErrorCode
	LeaderEpoch int32
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

// CreateTopic creates topic with partitionCount partitions.
func (c *Client) CreateTopic(ctx context.Context, topic string, partitionCount int32) (CreateTopicResult, error) {
	ctx, span := c.tracer.Start(ctx, "stellflow.admin.create_topic",
		trace.WithAttributes(
			attribute.String("stellflow.topic", topic),
			attribute.Int("stellflow.partition_count", int(partitionCount)),
		),
	)
	defer span.End()
	if err := validateTopicCreation(topic, partitionCount); err != nil {
		c.recordError(ctx, span, "admin create topic validation failed", err)
		return CreateTopicResult{}, err
	}
	bootstrap, err := c.metadata.BootstrapEndpoint()
	if err != nil {
		c.recordError(ctx, span, "admin create topic failed", err)
		return CreateTopicResult{}, err
	}
	body := message.TopicAdminRequestBody{
		Topic:          &topic,
		PartitionCount: partitionCount,
		Partition:      -1,
		LeaderID:       -1,
		LeaderEpoch:    -1,
	}
	response, err := c.protocol.Send(ctx, bootstrap, protocol.ApiKeyCreateTopic, protocol.DefaultAPIVersion, body)
	if err != nil {
		c.recordError(ctx, span, "admin create topic failed", err)
		return CreateTopicResult{}, err
	}
	typed, ok := response.Body.(message.TopicAdminResponseBody)
	if !ok {
		err := errors.New("unexpected CreateTopic response body")
		c.recordError(ctx, span, "admin create topic response decode failed", err)
		return CreateTopicResult{}, err
	}
	result, err := createTopicResultFromResponse(topic, typed, true)
	if err != nil {
		c.recordError(ctx, span, "admin create topic response validation failed", err)
		return CreateTopicResult{}, err
	}
	if err := requireSuccessfulCreateTopicResult(result); err != nil {
		c.recordError(ctx, span, "admin create topic failed", err)
		return CreateTopicResult{}, err
	}
	c.metadata.Invalidate(topic)
	c.logger.Info(ctx, "admin create topic completed",
		observability.String("topic", topic),
		observability.Int("partition_count", len(result.Partitions)),
	)
	return result, nil
}

// CreateTopicIfAbsent creates topic only when metadata does not report an existing topic.
func (c *Client) CreateTopicIfAbsent(ctx context.Context, topic string, partitionCount int32) (CreateTopicResult, error) {
	if err := validateTopicCreation(topic, partitionCount); err != nil {
		return CreateTopicResult{}, err
	}
	response, err := c.Metadata(ctx, []string{topic})
	if err != nil {
		return CreateTopicResult{}, err
	}
	for _, topicResponse := range response.Topics {
		if topicResponse.Topic == nil || *topicResponse.Topic != topic {
			continue
		}
		if topicResponse.ErrorCode == protocol.ErrorCodeNone {
			return existingCreateTopicResult(topicResponse), nil
		}
		break
	}
	return c.CreateTopic(ctx, topic, partitionCount)
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

func validateTopicCreation(topic string, partitionCount int32) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("topic must not be blank")
	}
	if partitionCount <= 0 {
		return errors.New("partitionCount must be positive")
	}
	return nil
}

func createTopicResultFromResponse(topic string, response message.TopicAdminResponseBody, created bool) (CreateTopicResult, error) {
	if response.Topic == nil {
		return CreateTopicResult{}, errors.New("create topic response missing topic")
	}
	if *response.Topic != topic {
		return CreateTopicResult{}, fmt.Errorf("create topic response mismatch: %s", *response.Topic)
	}
	partitions := make([]CreateTopicPartitionResult, 0, len(response.Partitions))
	for _, partition := range response.Partitions {
		partitions = append(partitions, CreateTopicPartitionResult{
			Partition:   partition.Partition,
			ErrorCode:   partition.ErrorCode,
			LeaderEpoch: partition.LeaderEpoch,
		})
	}
	return CreateTopicResult{Topic: topic, Created: created, Partitions: partitions}, nil
}

func existingCreateTopicResult(topic message.MetadataTopicResponse) CreateTopicResult {
	name := ""
	if topic.Topic != nil {
		name = *topic.Topic
	}
	partitions := make([]CreateTopicPartitionResult, 0, len(topic.Partitions))
	for _, partition := range topic.Partitions {
		partitions = append(partitions, CreateTopicPartitionResult{
			Partition:   partition.Partition,
			ErrorCode:   partition.ErrorCode,
			LeaderEpoch: partition.LeaderEpoch,
		})
	}
	return CreateTopicResult{Topic: name, Created: false, Partitions: partitions}
}

func requireSuccessfulCreateTopicResult(result CreateTopicResult) error {
	for _, partition := range result.Partitions {
		if partition.ErrorCode != protocol.ErrorCodeNone {
			return &protocol.ClientError{Code: partition.ErrorCode, Message: "create topic failed"}
		}
	}
	return nil
}

func (c *Client) recordError(ctx context.Context, span trace.Span, msg string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	c.logger.Error(ctx, msg, observability.Error(err))
}
