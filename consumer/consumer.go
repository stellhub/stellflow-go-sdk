package consumer

import (
	"context"
	"errors"

	imetadata "github.com/stellhub/stellflow-go-sdk/internal/metadata"
	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// TopicPartition identifies a manually assigned partition.
type TopicPartition struct {
	Topic     string
	Partition int32
	Offset    int64
}

// Record is one fetched consumer message.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []message.RecordHeader
	Timestamp int64
}

// Options configures a consumer.
type Options struct {
	GroupID           string
	FetchMaxBytes     int32
	PartitionMaxBytes int32
	CommitMetadata    string
}

// Client fetches records from manually assigned partitions.
type Client struct {
	protocol        *protocolclient.Client
	metadata        *imetadata.Manager
	options         Options
	assignment      []TopicPartition
	nextOffsets     map[imetadata.TopicPartition]int64
	consumedOffsets map[imetadata.TopicPartition]int64
}

// New creates a consumer.
func New(protocolClient *protocolclient.Client, metadataManager *imetadata.Manager, options Options) *Client {
	if options.FetchMaxBytes == 0 {
		options.FetchMaxBytes = 4 * 1024 * 1024
	}
	if options.PartitionMaxBytes == 0 {
		options.PartitionMaxBytes = options.FetchMaxBytes
	}
	return &Client{
		protocol:        protocolClient,
		metadata:        metadataManager,
		options:         options,
		nextOffsets:     make(map[imetadata.TopicPartition]int64),
		consumedOffsets: make(map[imetadata.TopicPartition]int64),
	}
}

// Assign manually assigns partitions. It does not join a consumer group.
func (c *Client) Assign(partitions []TopicPartition) error {
	if len(partitions) == 0 {
		return errors.New("partitions must not be empty")
	}
	c.assignment = append([]TopicPartition(nil), partitions...)
	c.nextOffsets = make(map[imetadata.TopicPartition]int64, len(partitions))
	c.consumedOffsets = make(map[imetadata.TopicPartition]int64)
	for _, partition := range partitions {
		c.nextOffsets[imetadata.TopicPartition{Topic: partition.Topic, Partition: partition.Partition}] = partition.Offset
	}
	return nil
}

// Poll fetches from assigned partitions.
func (c *Client) Poll(ctx context.Context) ([]Record, error) {
	if len(c.assignment) == 0 {
		return nil, nil
	}
	var records []Record
	for _, partition := range c.assignment {
		fetched, err := c.fetchPartition(ctx, partition.Topic, partition.Partition)
		if err != nil {
			return nil, err
		}
		records = append(records, fetched...)
	}
	return records, nil
}

// Commit commits consumed offsets for the configured group id.
func (c *Client) Commit(ctx context.Context) error {
	if c.options.GroupID == "" {
		return errors.New("consumer group id must not be blank")
	}
	if len(c.consumedOffsets) == 0 {
		return nil
	}
	coordinator, err := c.findCoordinator(ctx, c.options.GroupID)
	if err != nil {
		return err
	}
	groupID := c.options.GroupID
	metadata := c.options.CommitMetadata
	for partition, offset := range c.consumedOffsets {
		body := message.OffsetCommitRequestBody{
			GroupID: &groupID,
			Topics: []message.OffsetCommitTopic{
				{
					Topic: partition.Topic,
					Partitions: []message.OffsetCommitPartition{
						{Partition: partition.Partition, Offset: offset, Metadata: &metadata},
					},
				},
			},
		}
		response, err := c.protocol.Send(ctx, coordinator, protocol.ApiKeyOffsetCommit, protocol.DefaultAPIVersion, body)
		if err != nil {
			return err
		}
		typed, ok := response.Body.(message.OffsetCommitResponseBody)
		if !ok {
			return errors.New("unexpected OffsetCommit response body")
		}
		for _, topic := range typed.Topics {
			if topic.Topic == nil || *topic.Topic != partition.Topic {
				continue
			}
			for _, part := range topic.Partitions {
				if part.Partition == partition.Partition && part.ErrorCode != protocol.ErrorCodeNone {
					return &protocol.ClientError{Code: part.ErrorCode, Message: "offset commit failed"}
				}
			}
		}
	}
	return nil
}

func (c *Client) findCoordinator(ctx context.Context, groupID string) (transport.Endpoint, error) {
	bootstrap, err := c.metadata.BootstrapEndpoint()
	if err != nil {
		return transport.Endpoint{}, err
	}
	body := message.FindCoordinatorRequestBody{Key: &groupID, KeyType: message.CoordinatorKeyTypeGroup}
	response, err := c.protocol.Send(ctx, bootstrap, protocol.ApiKeyFindCoordinator, protocol.DefaultAPIVersion, body)
	if err != nil {
		return transport.Endpoint{}, err
	}
	typed, ok := response.Body.(message.FindCoordinatorResponseBody)
	if !ok {
		return transport.Endpoint{}, errors.New("unexpected FindCoordinator response body")
	}
	if typed.ErrorCode != protocol.ErrorCodeNone {
		return transport.Endpoint{}, &protocol.ClientError{Code: typed.ErrorCode, Message: "find coordinator failed"}
	}
	if typed.Host == nil {
		return transport.Endpoint{}, errors.New("find coordinator response missing host")
	}
	return transport.Endpoint{Host: *typed.Host, Port: int(typed.Port)}, nil
}

func (c *Client) fetchPartition(ctx context.Context, topic string, partition int32) ([]Record, error) {
	key := imetadata.TopicPartition{Topic: topic, Partition: partition}
	fetchOffset := c.nextOffsets[key]
	route, err := c.metadata.Route(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	body := message.FetchRequestBody{
		ReplicaID:      -1,
		MaxWaitMs:      500,
		MinBytes:       1,
		MaxBytes:       c.options.FetchMaxBytes,
		IsolationLevel: 0,
		SessionID:      0,
		TopicPartitions: []message.FetchTopicRequest{
			{
				Topic: topic,
				Partitions: []message.FetchPartitionRequest{
					{
						Partition:          partition,
						CurrentLeaderEpoch: route.LeaderEpoch,
						FetchOffset:        fetchOffset,
						LogStartOffset:     -1,
						PartitionMaxBytes:  c.options.PartitionMaxBytes,
					},
				},
			},
		},
	}
	response, err := c.protocol.Send(ctx, route.Endpoint, protocol.ApiKeyFetch, protocol.DefaultAPIVersion, body)
	if err != nil {
		return nil, err
	}
	typed, ok := response.Body.(message.FetchResponseBody)
	if !ok {
		return nil, errors.New("unexpected Fetch response body")
	}
	return c.toRecords(topic, partition, fetchOffset, typed)
}

func (c *Client) toRecords(topic string, partition int32, fetchOffset int64, response message.FetchResponseBody) ([]Record, error) {
	key := imetadata.TopicPartition{Topic: topic, Partition: partition}
	var records []Record
	for _, topicResponse := range response.Responses {
		if topicResponse.Topic == nil || *topicResponse.Topic != topic {
			continue
		}
		for _, partitionResponse := range topicResponse.Partitions {
			if partitionResponse.Partition != partition {
				continue
			}
			if partitionResponse.ErrorCode != protocol.ErrorCodeNone {
				return nil, &protocol.ClientError{Code: partitionResponse.ErrorCode, Message: "fetch partition failed"}
			}
			batches, err := codec.DecodeRecordBatchSet(partitionResponse.Records)
			if err != nil {
				return nil, err
			}
			for _, batch := range batches {
				baseOffset := fetchOffset + int64(batch.BaseOffsetDelta)
				for _, batchRecord := range batch.Records {
					offset := baseOffset + int64(batchRecord.OffsetDelta)
					records = append(records, Record{
						Topic:     topic,
						Partition: partition,
						Offset:    offset,
						Key:       batchRecord.Key,
						Value:     batchRecord.Value,
						Headers:   batchRecord.Headers,
						Timestamp: batch.BaseTimestamp + batchRecord.TimestampDelta,
					})
					c.nextOffsets[key] = offset + 1
					c.consumedOffsets[key] = offset + 1
				}
			}
		}
	}
	return records, nil
}
