package producer

import (
	"context"
	"errors"
	"time"

	imetadata "github.com/stellhub/stellflow-go-sdk/internal/metadata"
	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

const NoPartition int32 = -1

const routeRefreshAttempts = 2

// Record is one producer message.
type Record struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
	Headers   []message.RecordHeader
}

// Metadata describes an acknowledged record.
type Metadata struct {
	Topic              string
	Partition          int32
	Offset             int64
	CurrentLeaderEpoch int32
	LogStartOffset     int64
}

// Client sends records to Stellflow.
type Client struct {
	protocol *protocolclient.Client
	metadata *imetadata.Manager
	Acks     int16
	Timeout  int32
}

// New creates a producer.
func New(protocolClient *protocolclient.Client, metadataManager *imetadata.Manager) *Client {
	return &Client{protocol: protocolClient, metadata: metadataManager, Acks: -1, Timeout: 30000}
}

// Send sends a single record.
func (c *Client) Send(ctx context.Context, record Record) (Metadata, error) {
	if record.Topic == "" {
		return Metadata{}, errors.New("record topic must not be blank")
	}
	partition := record.Partition
	if partition == 0 {
		partition = NoPartition
	}
	if partition == NoPartition {
		partitions, err := c.metadata.PartitionIDs(ctx, record.Topic)
		if err != nil {
			return Metadata{}, err
		}
		partition = partitions[0]
	}
	var lastErr error
attemptLoop:
	for attempt := 1; attempt <= routeRefreshAttempts; attempt++ {
		route, err := c.route(ctx, record.Topic, partition, attempt)
		if err != nil {
			return Metadata{}, err
		}
		now := time.Now().UnixMilli()
		batchBytes, err := codec.EncodeRecordBatchSet([]message.RecordBatch{
			{
				PartitionLeaderEpoch: route.LeaderEpoch,
				Magic:                message.RecordBatchMagicV1,
				Attributes:           0,
				LastOffsetDelta:      0,
				BaseTimestamp:        now,
				MaxTimestamp:         now,
				ProducerID:           -1,
				ProducerEpoch:        -1,
				BaseSequence:         -1,
				Records: []message.Record{
					{
						Attributes:     0,
						TimestampDelta: 0,
						OffsetDelta:    0,
						Key:            record.Key,
						Value:          record.Value,
						Headers:        record.Headers,
					},
				},
			},
		})
		if err != nil {
			return Metadata{}, err
		}
		body := message.ProduceRequestBody{
			Acks:      c.Acks,
			TimeoutMs: c.Timeout,
			TopicData: []message.ProduceTopicData{
				{Topic: record.Topic, Partitions: []message.ProducePartitionData{{Partition: partition, Records: batchBytes}}},
			},
		}
		response, err := c.protocol.Send(ctx, route.Endpoint, protocol.ApiKeyProduce, protocol.DefaultAPIVersion, body)
		if err != nil {
			lastErr = err
			if attempt < routeRefreshAttempts && shouldRefreshRoute(ctx, err) {
				continue
			}
			return Metadata{}, err
		}
		typed, ok := response.Body.(message.ProduceResponseBody)
		if !ok {
			return Metadata{}, errors.New("unexpected Produce response body")
		}
		for _, topic := range typed.Responses {
			if topic.Topic == nil || *topic.Topic != record.Topic {
				continue
			}
			for _, partitionResponse := range topic.Partitions {
				if partitionResponse.Partition != partition {
					continue
				}
				if partitionResponse.ErrorCode != protocol.ErrorCodeNone {
					err := &protocol.ClientError{Code: partitionResponse.ErrorCode, Message: "produce partition failed"}
					lastErr = err
					if attempt < routeRefreshAttempts && shouldRefreshRoute(ctx, err) {
						continue attemptLoop
					}
					return Metadata{}, err
				}
				return Metadata{
					Topic:              record.Topic,
					Partition:          partition,
					Offset:             partitionResponse.BaseOffset,
					CurrentLeaderEpoch: partitionResponse.CurrentLeaderEpoch,
					LogStartOffset:     partitionResponse.LogStartOffset,
				}, nil
			}
		}
		lastErr = errors.New("produce response missing partition result")
	}
	return Metadata{}, lastErr
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
