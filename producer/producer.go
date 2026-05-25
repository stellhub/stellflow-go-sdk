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

// ErrClosed is returned when producer methods are called after Close.
var ErrClosed = errors.New("producer is closed")

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
	closed     bool
	inFlightMu sync.Mutex
	inFlight   map[string]chan struct{}
	orderMu    sync.Mutex
	orderLocks map[batchKey]chan struct{}
	initMu     sync.Mutex
	stateMu    sync.Mutex
	producerID int64
	epoch      int16
	sequences  map[batchKey]int32
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
		inFlight:   make(map[string]chan struct{}),
		orderLocks: make(map[batchKey]chan struct{}),
		producerID: options.ProducerID,
		epoch:      options.ProducerEpoch,
		sequences:  make(map[batchKey]int32),
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
	if c.isClosed() {
		c.recordError(ctx, span, "producer send failed", ErrClosed)
		return Metadata{}, ErrClosed
	}
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
	if c.isClosed() {
		c.recordError(ctx, span, "producer async enqueue failed", ErrClosed)
		return nil, ErrClosed
	}
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
	return c.flush(ctx, false)
}

func (c *Client) flush(ctx context.Context, allowClosed bool) error {
	ctx, span := c.tracer.Start(ctx, "stellflow.producer.flush")
	defer span.End()
	if !allowClosed && c.isClosed() {
		return ErrClosed
	}
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
		c.markClosed()
		closeErr = c.flush(ctx, true)
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
		if err := c.ensureTopic(ctx, record.Topic, partition+1); err != nil {
			return 0, err
		}
		return partition, nil
	}
	if err := c.ensureTopic(ctx, record.Topic, c.options.AutoCreateTopicPartitionCount); err != nil {
		return 0, err
	}
	return c.selectPartitionFromMetadata(ctx, record)
}

func (c *Client) selectPartitionFromMetadata(ctx context.Context, record Record) (int32, error) {
	if c.metadata == nil {
		return 0, errors.New("producer requires metadata manager to select partition")
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

func (c *Client) ensureTopic(ctx context.Context, topic string, partitionCount int32) error {
	if c.metadata == nil || c.options.DisableAutoCreateTopics {
		return nil
	}
	partitions, err := c.metadata.PartitionIDs(ctx, topic)
	if err == nil && len(partitions) > 0 {
		return nil
	}
	if err != nil && !imetadata.IsMissingPartitions(err) {
		return err
	}
	if partitionCount <= 0 {
		partitionCount = c.options.AutoCreateTopicPartitionCount
	}
	if partitionCount <= 0 {
		partitionCount = 1
	}
	if err := c.createTopic(ctx, topic, partitionCount); err != nil {
		return err
	}
	_, err = c.metadata.Refresh(ctx, []string{topic})
	return err
}

func (c *Client) createTopic(ctx context.Context, topic string, partitionCount int32) error {
	bootstrap, err := c.metadata.BootstrapEndpoint()
	if err != nil {
		return err
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
		return err
	}
	typed, ok := response.Body.(message.TopicAdminResponseBody)
	if !ok {
		return errors.New("unexpected CreateTopic response body")
	}
	if typed.Topic == nil || *typed.Topic != topic {
		return errors.New("create topic response mismatch")
	}
	for _, partition := range typed.Partitions {
		if partition.ErrorCode != protocol.ErrorCodeNone {
			return &protocol.ClientError{Code: partition.ErrorCode, Message: "create topic failed"}
		}
	}
	c.metadata.Invalidate(topic)
	c.logger.Info(ctx, "producer auto-created topic",
		observability.String("topic", topic),
		observability.Int("partition_count", len(typed.Partitions)),
	)
	return nil
}

func (c *Client) produceRecords(ctx context.Context, topic string, partition int32, records []Record) ([]Metadata, error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.DeliveryTimeout)
	defer cancel()
	ctx, span := c.tracer.Start(ctx, "stellflow.producer.produce_records",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("stellflow.topic", topic),
			attribute.Int("stellflow.partition", int(partition)),
			attribute.Int("stellflow.record_count", len(records)),
		),
	)
	defer span.End()
	key := batchKey{topic: topic, partition: partition}
	if err := c.acquireOrdering(ctx, key); err != nil {
		c.recordError(ctx, span, "producer ordering wait failed", err)
		return nil, err
	}
	defer c.releaseOrdering(key)
	if err := c.ensureProducerID(ctx); err != nil {
		c.recordError(ctx, span, "producer id initialization failed", err)
		return nil, err
	}
	producerID, producerEpoch, baseSequence := c.batchIdentity(key)
	var lastErr error
attemptLoop:
	for attempt := 1; attempt <= c.options.RetryMaxAttempts; attempt++ {
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
		batch.ProducerID = producerID
		batch.ProducerEpoch = producerEpoch
		batch.BaseSequence = baseSequence
		batchBytes, err := codec.EncodeRecordBatchSet([]message.RecordBatch{batch})
		if err != nil {
			c.recordError(ctx, span, "producer record batch encode failed", err)
			return nil, err
		}
		body := message.ProduceRequestBody{
			TransactionalID: c.transactionalID(),
			Acks:            c.Acks,
			TimeoutMs:       c.Timeout,
			TopicData: []message.ProduceTopicData{
				{Topic: topic, Partitions: []message.ProducePartitionData{{Partition: partition, Records: batchBytes}}},
			},
		}
		endpoint := route.Endpoint.Address()
		if err := c.acquireInFlight(ctx, endpoint); err != nil {
			c.recordError(ctx, span, "producer in-flight limit wait failed", err)
			return nil, err
		}
		requestCtx, requestCancel := c.requestContext(ctx)
		response, err := c.protocol.Send(requestCtx, route.Endpoint, protocol.ApiKeyProduce, protocol.DefaultAPIVersion, body)
		requestCancel()
		c.releaseInFlight(endpoint)
		if err != nil {
			lastErr = err
			if attempt < c.options.RetryMaxAttempts && shouldRetryProduce(ctx, err) {
				c.logger.Warn(ctx, "producer send will retry",
					observability.String("topic", topic),
					observability.Int32("partition", partition),
					observability.Int("attempt", attempt),
					observability.Error(err),
				)
				if err := c.retryBackoff(ctx); err != nil {
					c.recordError(ctx, span, "producer retry backoff interrupted", err)
					return nil, err
				}
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
					if attempt < c.options.RetryMaxAttempts && shouldRetryProduce(ctx, err) {
						c.logger.Warn(ctx, "producer partition error will retry",
							observability.String("topic", topic),
							observability.Int32("partition", partition),
							observability.Int("attempt", attempt),
							observability.Error(err),
						)
						if err := c.retryBackoff(ctx); err != nil {
							c.recordError(ctx, span, "producer retry backoff interrupted", err)
							return nil, err
						}
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
				c.commitSequence(key, len(records))
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

func (c *Client) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.options.RequestTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.options.RequestTimeout)
}

func (c *Client) retryBackoff(ctx context.Context) error {
	if c.options.RetryBackoff <= 0 {
		return nil
	}
	timer := time.NewTimer(c.options.RetryBackoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) acquireOrdering(ctx context.Context, key batchKey) error {
	if c.options.Ordering != OrderingPerPartition {
		return nil
	}
	c.orderMu.Lock()
	lock := c.orderLocks[key]
	if lock == nil {
		lock = make(chan struct{}, 1)
		c.orderLocks[key] = lock
	}
	c.orderMu.Unlock()
	select {
	case lock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) releaseOrdering(key batchKey) {
	if c.options.Ordering != OrderingPerPartition {
		return
	}
	c.orderMu.Lock()
	lock := c.orderLocks[key]
	c.orderMu.Unlock()
	if lock == nil {
		return
	}
	select {
	case <-lock:
	default:
	}
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

func shouldRetryProduce(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	return shouldRefreshRoute(ctx, err) || protocol.IsRetriable(err)
}

func (c *Client) transactionalID() *string {
	if c.options.TransactionalID == "" {
		return nil
	}
	return &c.options.TransactionalID
}

func (c *Client) batchIdentity(key batchKey) (int64, int16, int32) {
	if !c.options.Idempotent {
		return -1, -1, -1
	}
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.producerID, c.epoch, c.sequences[key]
}

func (c *Client) commitSequence(key batchKey, count int) {
	if !c.options.Idempotent || count <= 0 {
		return
	}
	c.stateMu.Lock()
	c.sequences[key] += int32(count)
	c.stateMu.Unlock()
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Client) markClosed() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
}

func (c *Client) acquireInFlight(ctx context.Context, endpoint string) error {
	c.inFlightMu.Lock()
	limiter := c.inFlight[endpoint]
	if limiter == nil {
		limiter = make(chan struct{}, c.options.MaxInFlight)
		c.inFlight[endpoint] = limiter
	}
	c.inFlightMu.Unlock()
	select {
	case limiter <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) releaseInFlight(endpoint string) {
	c.inFlightMu.Lock()
	limiter := c.inFlight[endpoint]
	c.inFlightMu.Unlock()
	if limiter == nil {
		return
	}
	select {
	case <-limiter:
	default:
	}
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
