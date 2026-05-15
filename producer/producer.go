package producer

import (
	"context"
	"errors"
	"sync"
	"time"

	imetadata "github.com/stellhub/stellflow-go-sdk/internal/metadata"
	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/observability"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	protocol   *protocolclient.Client
	metadata   *imetadata.Manager
	options    Options
	logger     observability.Logger
	tracer     trace.Tracer
	Acks       int16
	Timeout    int32
	mu         sync.Mutex
	roundRobin int
	workerOnce sync.Once
	closeOnce  sync.Once
	asyncCh    chan asyncRecord
	flushCh    chan flushRequest
	stopCh     chan struct{}
	workerDone chan struct{}
}

// New creates a producer.
func New(protocolClient *protocolclient.Client, metadataManager *imetadata.Manager) *Client {
	return NewWithOptions(protocolClient, metadataManager, Options{})
}

// NewWithOptions creates a producer with explicit options.
func NewWithOptions(protocolClient *protocolclient.Client, metadataManager *imetadata.Manager, options Options) *Client {
	options = normalizeOptions(options)
	obs := observability.Normalize(options.Observability)
	return &Client{
		protocol:   protocolClient,
		metadata:   metadataManager,
		options:    options,
		logger:     obs.Logger,
		tracer:     observability.Tracer(obs),
		Acks:       options.Acks,
		Timeout:    options.TimeoutMs,
		asyncCh:    make(chan asyncRecord, options.QueueSize),
		flushCh:    make(chan flushRequest),
		stopCh:     make(chan struct{}),
		workerDone: make(chan struct{}),
	}
}

// Send sends a single record.
func (c *Client) Send(ctx context.Context, record Record) (Metadata, error) {
	ctx, span := c.tracer.Start(ctx, "stellflow.producer.send",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("stellflow.topic", record.Topic)),
	)
	defer span.End()
	if err := validateRecord(record); err != nil {
		c.recordError(ctx, span, "producer record validation failed", err)
		return Metadata{}, err
	}
	partition, err := c.selectPartition(ctx, record)
	if err != nil {
		c.recordError(ctx, span, "producer partition selection failed", err)
		return Metadata{}, err
	}
	span.SetAttributes(attribute.Int("stellflow.partition", int(partition)))
	metadata, err := c.produceRecords(ctx, record.Topic, partition, []Record{record})
	if err != nil {
		c.recordError(ctx, span, "producer send failed", err)
		return Metadata{}, err
	}
	if len(metadata) == 0 {
		err := errors.New("produce returned no metadata")
		c.recordError(ctx, span, "producer send failed", err)
		return Metadata{}, err
	}
	c.logger.Info(ctx, "producer send completed",
		observability.String("topic", record.Topic),
		observability.Int32("partition", partition),
		observability.Int64("offset", metadata[0].Offset),
	)
	return metadata[0], nil
}

// SendAsync enqueues one record for background batching.
func (c *Client) SendAsync(ctx context.Context, record Record) (*Future, error) {
	ctx, span := c.tracer.Start(ctx, "stellflow.producer.send_async",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("stellflow.topic", record.Topic)),
	)
	defer span.End()
	if err := validateRecord(record); err != nil {
		c.recordError(ctx, span, "producer async record validation failed", err)
		return nil, err
	}
	c.ensureWorker()
	future := newFuture()
	item := asyncRecord{ctx: ctx, record: record, future: future}
	select {
	case c.asyncCh <- item:
		c.logger.Debug(ctx, "producer record queued",
			observability.String("topic", record.Topic),
			observability.Int("queue_size", len(c.asyncCh)),
		)
		return future, nil
	case <-ctx.Done():
		c.recordError(ctx, span, "producer async enqueue failed", ctx.Err())
		return nil, ctx.Err()
	}
}

// Flush sends all buffered asynchronous records.
func (c *Client) Flush(ctx context.Context) error {
	ctx, span := c.tracer.Start(ctx, "stellflow.producer.flush")
	defer span.End()
	c.ensureWorker()
	request := flushRequest{ctx: ctx, done: make(chan error, 1)}
	select {
	case c.flushCh <- request:
	case <-ctx.Done():
		c.recordError(ctx, span, "producer flush enqueue failed", ctx.Err())
		return ctx.Err()
	}
	select {
	case err := <-request.done:
		if err != nil {
			c.recordError(ctx, span, "producer flush failed", err)
			return err
		}
		c.logger.Info(ctx, "producer flush completed")
		return err
	case <-ctx.Done():
		c.recordError(ctx, span, "producer flush canceled", ctx.Err())
		return ctx.Err()
	}
}

// Close flushes buffered records and stops the async producer worker.
func (c *Client) Close(ctx context.Context) error {
	var closeErr error
	c.closeOnce.Do(func() {
		closeErr = c.Flush(ctx)
		close(c.stopCh)
		<-c.workerDone
	})
	return closeErr
}

func validateRecord(record Record) error {
	if record.Topic == "" {
		return errors.New("record topic must not be blank")
	}
	return nil
}

func (c *Client) selectPartition(ctx context.Context, record Record) (int32, error) {
	partition := record.Partition
	if partition != 0 && partition != NoPartition {
		return partition, nil
	}
	partitions, err := c.metadata.PartitionIDs(ctx, record.Topic)
	if err != nil {
		return 0, err
	}
	if c.options.Partitioner != nil {
		return c.options.Partitioner(record, partitions)
	}
	if len(record.Key) > 0 {
		return KeyHashPartitioner(record, partitions)
	}
	c.mu.Lock()
	index := c.roundRobin
	c.roundRobin++
	c.mu.Unlock()
	return partitions[index%len(partitions)], nil
}

func (c *Client) produceRecords(ctx context.Context, topic string, partition int32, records []Record) ([]Metadata, error) {
	ctx, span := c.tracer.Start(ctx, "stellflow.producer.produce_records",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("stellflow.topic", topic),
			attribute.Int("stellflow.partition", int(partition)),
			attribute.Int("stellflow.record_count", len(records)),
		),
	)
	defer span.End()
	var lastErr error
attemptLoop:
	for attempt := 1; attempt <= routeRefreshAttempts; attempt++ {
		route, err := c.route(ctx, topic, partition, attempt)
		if err != nil {
			c.recordError(ctx, span, "producer route lookup failed", err)
			return nil, err
		}
		now := time.Now().UnixMilli()
		batch := message.NewRecordBatch(toProtocolRecords(records))
		batch.PartitionLeaderEpoch = route.LeaderEpoch
		batch.BaseTimestamp = now
		batch.MaxTimestamp = now
		batchBytes, err := codec.EncodeRecordBatchSet([]message.RecordBatch{batch})
		if err != nil {
			c.recordError(ctx, span, "producer record batch encode failed", err)
			return nil, err
		}
		body := message.ProduceRequestBody{
			Acks:      c.Acks,
			TimeoutMs: c.Timeout,
			TopicData: []message.ProduceTopicData{
				{Topic: topic, Partitions: []message.ProducePartitionData{{Partition: partition, Records: batchBytes}}},
			},
		}
		response, err := c.protocol.Send(ctx, route.Endpoint, protocol.ApiKeyProduce, protocol.DefaultAPIVersion, body)
		if err != nil {
			lastErr = err
			if attempt < routeRefreshAttempts && shouldRefreshRoute(ctx, err) {
				c.logger.Warn(ctx, "producer send will refresh route",
					observability.String("topic", topic),
					observability.Int32("partition", partition),
					observability.Int("attempt", attempt),
					observability.Error(err),
				)
				continue
			}
			c.recordError(ctx, span, "producer produce request failed", err)
			return nil, err
		}
		typed, ok := response.Body.(message.ProduceResponseBody)
		if !ok {
			err := errors.New("unexpected Produce response body")
			c.recordError(ctx, span, "producer response decode failed", err)
			return nil, err
		}
		for _, topicResponse := range typed.Responses {
			if topicResponse.Topic == nil || *topicResponse.Topic != topic {
				continue
			}
			for _, partitionResponse := range topicResponse.Partitions {
				if partitionResponse.Partition != partition {
					continue
				}
				if partitionResponse.ErrorCode != protocol.ErrorCodeNone {
					err := &protocol.ClientError{Code: partitionResponse.ErrorCode, Message: "produce partition failed"}
					lastErr = err
					if attempt < routeRefreshAttempts && shouldRefreshRoute(ctx, err) {
						c.logger.Warn(ctx, "producer partition error will refresh route",
							observability.String("topic", topic),
							observability.Int32("partition", partition),
							observability.Int("attempt", attempt),
							observability.Error(err),
						)
						continue attemptLoop
					}
					c.recordError(ctx, span, "producer partition failed", err)
					return nil, err
				}
				c.logger.Info(ctx, "producer batch completed",
					observability.String("topic", topic),
					observability.Int32("partition", partition),
					observability.Int64("base_offset", partitionResponse.BaseOffset),
					observability.Int("record_count", len(records)),
				)
				return metadataFromResponse(topic, partitionResponse, len(records)), nil
			}
		}
		lastErr = errors.New("produce response missing partition result")
	}
	if lastErr != nil {
		c.recordError(ctx, span, "producer attempts exhausted", lastErr)
	}
	return nil, lastErr
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

func toProtocolRecords(records []Record) []message.Record {
	protocolRecords := make([]message.Record, 0, len(records))
	for index, record := range records {
		protocolRecords = append(protocolRecords, message.Record{
			Attributes:  0,
			OffsetDelta: int32(index),
			Key:         record.Key,
			Value:       record.Value,
			Headers:     record.Headers,
		})
	}
	return protocolRecords
}

func metadataFromResponse(topic string, response message.ProducePartitionResponse, count int) []Metadata {
	metadata := make([]Metadata, 0, count)
	for index := range count {
		metadata = append(metadata, Metadata{
			Topic:              topic,
			Partition:          response.Partition,
			Offset:             response.BaseOffset + int64(index),
			CurrentLeaderEpoch: response.CurrentLeaderEpoch,
			LogStartOffset:     response.LogStartOffset,
		})
	}
	return metadata
}

func (c *Client) recordError(ctx context.Context, span trace.Span, msg string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	c.logger.Error(ctx, msg, observability.Error(err))
}
