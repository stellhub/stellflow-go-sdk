package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	imetadata "github.com/stellhub/stellflow-go-sdk/internal/metadata"
	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/observability"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const routeRefreshAttempts = 2

var (
	// ErrNoOffset is returned when no committed offset exists and reset policy is none.
	ErrNoOffset = errors.New("consumer offset is not committed")

	// ErrMaxPollIntervalExceeded is returned when Poll is not called within MaxPollInterval.
	ErrMaxPollIntervalExceeded = errors.New("consumer max poll interval exceeded")
)

// OffsetResetStrategy controls where consumption starts when no committed offset exists.
type OffsetResetStrategy string

const (
	OffsetResetEarliest OffsetResetStrategy = "earliest"
	OffsetResetLatest   OffsetResetStrategy = "latest"
	OffsetResetNone     OffsetResetStrategy = "none"
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

// RebalanceListener observes partition assignment changes.
type RebalanceListener interface {
	OnPartitionsRevoked(ctx context.Context, partitions []TopicPartition)
	OnPartitionsAssigned(ctx context.Context, partitions []TopicPartition)
}

// Options configures a consumer.
type Options struct {
	GroupID            string
	MemberID           string
	SessionTimeout     time.Duration
	HeartbeatInterval  time.Duration
	MaxPollInterval    time.Duration
	FetchMaxBytes      int32
	PartitionMaxBytes  int32
	CommitMetadata     string
	OffsetReset        OffsetResetStrategy
	EnableAutoCommit   bool
	AutoCommitInterval time.Duration
	RebalanceListener  RebalanceListener
	Observability      observability.Options
}

// Client fetches records from manually assigned partitions.
type Client struct {
	protocol         *protocolclient.Client
	metadata         *imetadata.Manager
	options          Options
	logger           observability.Logger
	tracer           trace.Tracer
	subscribedTopics []string
	assignment       []TopicPartition
	nextOffsets      map[imetadata.TopicPartition]int64
	consumedOffsets  map[imetadata.TopicPartition]int64
	paused           map[imetadata.TopicPartition]struct{}
	lastPoll         time.Time
	mu               sync.Mutex
	groupSession     *GroupSession
	heartbeatCancel  context.CancelFunc
	autoCommitCancel context.CancelFunc
}

// GroupSession describes the active consumer group session.
type GroupSession struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	LeaderID     string
	Coordinator  transport.Endpoint
}

// New creates a consumer.
func New(protocolClient *protocolclient.Client, metadataManager *imetadata.Manager, options Options) *Client {
	if options.FetchMaxBytes == 0 {
		options.FetchMaxBytes = 4 * 1024 * 1024
	}
	if options.PartitionMaxBytes == 0 {
		options.PartitionMaxBytes = options.FetchMaxBytes
	}
	if options.SessionTimeout == 0 {
		options.SessionTimeout = 30 * time.Second
	}
	if options.HeartbeatInterval == 0 {
		options.HeartbeatInterval = 3 * time.Second
	}
	if options.MaxPollInterval == 0 {
		options.MaxPollInterval = 5 * time.Minute
	}
	if options.AutoCommitInterval == 0 {
		options.AutoCommitInterval = 5 * time.Second
	}
	if options.OffsetReset == "" {
		options.OffsetReset = OffsetResetLatest
	}
	if options.MemberID == "" {
		options.MemberID = "stellflow-go-consumer"
	}
	obs := observability.Normalize(options.Observability)
	return &Client{
		protocol:        protocolClient,
		metadata:        metadataManager,
		options:         options,
		logger:          obs.Logger,
		tracer:          observability.Tracer(obs),
		nextOffsets:     make(map[imetadata.TopicPartition]int64),
		consumedOffsets: make(map[imetadata.TopicPartition]int64),
		paused:          make(map[imetadata.TopicPartition]struct{}),
		lastPoll:        time.Now(),
	}
}

// Subscribe joins a consumer group, loads metadata assignment, restores offsets, and starts heartbeat.
func (c *Client) Subscribe(ctx context.Context, topics []string) error {
	ctx, span := c.tracer.Start(ctx, "stellflow.consumer.subscribe",
		trace.WithAttributes(attribute.Int("stellflow.topic_count", len(topics))),
	)
	defer span.End()
	if c.options.GroupID == "" {
		err := errors.New("consumer group id must not be blank")
		c.recordError(ctx, span, "consumer subscribe failed", err)
		return err
	}
	if len(topics) == 0 {
		err := errors.New("topics must not be empty")
		c.recordError(ctx, span, "consumer subscribe failed", err)
		return err
	}
	c.mu.Lock()
	c.subscribedTopics = append([]string(nil), topics...)
	c.lastPoll = time.Now()
	c.mu.Unlock()
	if err := c.joinSubscription(ctx, topics); err != nil {
		c.recordError(ctx, span, "consumer subscribe failed", err)
		return err
	}
	c.logger.Info(ctx, "consumer subscribed",
		observability.String("group_id", c.options.GroupID),
		observability.Int("topic_count", len(topics)),
	)
	return nil
}

func (c *Client) joinSubscription(ctx context.Context, topics []string) error {
	coordinator, err := c.findCoordinator(ctx, c.options.GroupID)
	if err != nil {
		return err
	}
	session, err := c.joinGroup(ctx, coordinator)
	if err != nil {
		return err
	}
	if err := c.syncGroup(ctx, session); err != nil {
		return err
	}
	assignment, err := c.assignmentFromMetadata(ctx, topics)
	if err != nil {
		return err
	}
	if err := c.assignWithOffsets(ctx, coordinator, assignment); err != nil {
		return err
	}
	c.mu.Lock()
	c.groupSession = &session
	c.mu.Unlock()
	c.startHeartbeatLoop(session)
	c.startAutoCommitLoop()
	return nil
}

// Assign manually assigns partitions. It does not join a consumer group.
func (c *Client) Assign(partitions []TopicPartition) error {
	if len(partitions) == 0 {
		return errors.New("partitions must not be empty")
	}
	c.stopHeartbeatLoop()
	c.stopAutoCommitLoop()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groupSession = nil
	c.subscribedTopics = nil
	c.assignment = append([]TopicPartition(nil), partitions...)
	c.nextOffsets = make(map[imetadata.TopicPartition]int64, len(partitions))
	c.consumedOffsets = make(map[imetadata.TopicPartition]int64)
	c.paused = make(map[imetadata.TopicPartition]struct{})
	for _, partition := range partitions {
		c.nextOffsets[imetadata.TopicPartition{Topic: partition.Topic, Partition: partition.Partition}] = partition.Offset
	}
	return nil
}

// Seek changes the next fetch offset for an assigned partition.
func (c *Client) Seek(topic string, partition int32, offset int64) error {
	key := imetadata.TopicPartition{Topic: topic, Partition: partition}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.isAssignedLocked(key) {
		return fmt.Errorf("partition is not assigned: %s[%d]", topic, partition)
	}
	c.nextOffsets[key] = offset
	delete(c.consumedOffsets, key)
	return nil
}

// Pause stops Poll from fetching the specified partitions.
func (c *Client) Pause(partitions []TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, partition := range partitions {
		c.paused[imetadata.TopicPartition{Topic: partition.Topic, Partition: partition.Partition}] = struct{}{}
	}
}

// Resume allows Poll to fetch the specified paused partitions again.
func (c *Client) Resume(partitions []TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, partition := range partitions {
		delete(c.paused, imetadata.TopicPartition{Topic: partition.Topic, Partition: partition.Partition})
	}
}

// Poll fetches from assigned partitions.
func (c *Client) Poll(ctx context.Context) ([]Record, error) {
	ctx, span := c.tracer.Start(ctx, "stellflow.consumer.poll")
	defer span.End()
	c.mu.Lock()
	if time.Since(c.lastPoll) > c.options.MaxPollInterval {
		c.mu.Unlock()
		err := ErrMaxPollIntervalExceeded
		c.recordError(ctx, span, "consumer max poll interval exceeded", err)
		if rejoinErr := c.rejoin(ctx); rejoinErr != nil {
			return nil, rejoinErr
		}
		return nil, err
	}
	c.lastPoll = time.Now()
	assignment := append([]TopicPartition(nil), c.assignment...)
	paused := make(map[imetadata.TopicPartition]struct{}, len(c.paused))
	for key := range c.paused {
		paused[key] = struct{}{}
	}
	c.mu.Unlock()
	if len(assignment) == 0 {
		return nil, nil
	}
	var records []Record
	for _, partition := range assignment {
		if _, ok := paused[imetadata.TopicPartition{Topic: partition.Topic, Partition: partition.Partition}]; ok {
			continue
		}
		fetched, err := c.fetchPartition(ctx, partition.Topic, partition.Partition)
		if err != nil {
			if c.shouldRejoin(ctx, err) {
				if rejoinErr := c.rejoin(ctx); rejoinErr != nil {
					return nil, rejoinErr
				}
			}
			c.recordError(ctx, span, "consumer poll failed", err)
			return nil, err
		}
		records = append(records, fetched...)
	}
	span.SetAttributes(attribute.Int("stellflow.record_count", len(records)))
	return records, nil
}

// Commit commits consumed offsets for the configured group id.
func (c *Client) Commit(ctx context.Context) error {
	ctx, span := c.tracer.Start(ctx, "stellflow.consumer.commit")
	defer span.End()
	if c.options.GroupID == "" {
		err := errors.New("consumer group id must not be blank")
		c.recordError(ctx, span, "consumer commit failed", err)
		return err
	}
	c.mu.Lock()
	consumedOffsets := make(map[imetadata.TopicPartition]int64, len(c.consumedOffsets))
	for partition, offset := range c.consumedOffsets {
		consumedOffsets[partition] = offset
	}
	c.mu.Unlock()
	if len(consumedOffsets) == 0 {
		return nil
	}
	span.SetAttributes(attribute.Int("stellflow.partition_count", len(consumedOffsets)))
	coordinator, err := c.coordinator(ctx)
	if err != nil {
		return err
	}
	groupID := c.options.GroupID
	metadata := c.options.CommitMetadata
	for partition, offset := range consumedOffsets {
		var lastErr error
	attemptLoop:
		for attempt := 1; attempt <= routeRefreshAttempts; attempt++ {
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
				lastErr = err
				if attempt < routeRefreshAttempts && c.shouldRefreshCoordinator(ctx, err) {
					coordinator, err = c.refreshCoordinator(ctx)
					if err != nil {
						return lastErr
					}
					continue
				}
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
					if part.Partition != partition.Partition {
						continue
					}
					if part.ErrorCode != protocol.ErrorCodeNone {
						err := &protocol.ClientError{Code: part.ErrorCode, Message: "offset commit failed"}
						lastErr = err
						if attempt < routeRefreshAttempts && c.shouldRefreshCoordinator(ctx, err) {
							var refreshErr error
							coordinator, refreshErr = c.refreshCoordinator(ctx)
							if refreshErr != nil {
								return lastErr
							}
							continue attemptLoop
						}
						return err
					}
					lastErr = nil
					break attemptLoop
				}
			}
			lastErr = errors.New("offset commit response missing partition")
		}
		if lastErr != nil {
			return lastErr
		}
	}
	c.logger.Info(ctx, "consumer offsets committed",
		observability.String("group_id", c.options.GroupID),
		observability.Int("partition_count", len(consumedOffsets)),
	)
	return nil
}

// Close stops background group work. It does not close the shared factory transport.
func (c *Client) Close() error {
	c.stopHeartbeatLoop()
	c.stopAutoCommitLoop()
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

func (c *Client) coordinator(ctx context.Context) (transport.Endpoint, error) {
	c.mu.Lock()
	session := c.groupSession
	c.mu.Unlock()
	if session != nil {
		return session.Coordinator, nil
	}
	return c.findCoordinator(ctx, c.options.GroupID)
}

func (c *Client) refreshCoordinator(ctx context.Context) (transport.Endpoint, error) {
	coordinator, err := c.findCoordinator(ctx, c.options.GroupID)
	if err != nil {
		return transport.Endpoint{}, err
	}
	c.mu.Lock()
	if c.groupSession != nil {
		c.groupSession.Coordinator = coordinator
	}
	c.mu.Unlock()
	return coordinator, nil
}

func (c *Client) joinGroup(ctx context.Context, coordinator transport.Endpoint) (GroupSession, error) {
	groupID := c.options.GroupID
	memberID := c.options.MemberID
	body := message.JoinGroupRequestBody{
		GroupID:          &groupID,
		MemberID:         &memberID,
		SessionTimeoutMs: int32(c.options.SessionTimeout / time.Millisecond),
	}
	response, err := c.protocol.Send(ctx, coordinator, protocol.ApiKeyJoinGroup, protocol.DefaultAPIVersion, body)
	if err != nil {
		return GroupSession{}, err
	}
	typed, ok := response.Body.(message.JoinGroupResponseBody)
	if !ok {
		return GroupSession{}, errors.New("unexpected JoinGroup response body")
	}
	if typed.ErrorCode != protocol.ErrorCodeNone {
		return GroupSession{}, &protocol.ClientError{Code: typed.ErrorCode, Message: "join group failed"}
	}
	if typed.MemberID != nil {
		memberID = *typed.MemberID
	}
	leaderID := ""
	if typed.LeaderID != nil {
		leaderID = *typed.LeaderID
	}
	return GroupSession{
		GroupID:      groupID,
		GenerationID: typed.GenerationID,
		MemberID:     memberID,
		LeaderID:     leaderID,
		Coordinator:  coordinator,
	}, nil
}

func (c *Client) syncGroup(ctx context.Context, session GroupSession) error {
	groupID := session.GroupID
	memberID := session.MemberID
	body := message.SyncGroupRequestBody{
		GroupID:      &groupID,
		GenerationID: session.GenerationID,
		MemberID:     &memberID,
	}
	response, err := c.protocol.Send(ctx, session.Coordinator, protocol.ApiKeySyncGroup, protocol.DefaultAPIVersion, body)
	if err != nil {
		return err
	}
	typed, ok := response.Body.(message.SyncGroupResponseBody)
	if !ok {
		return errors.New("unexpected SyncGroup response body")
	}
	if typed.ErrorCode != protocol.ErrorCodeNone {
		return &protocol.ClientError{Code: typed.ErrorCode, Message: "sync group failed"}
	}
	return nil
}

func (c *Client) assignmentFromMetadata(ctx context.Context, topics []string) ([]TopicPartition, error) {
	response, err := c.metadata.Refresh(ctx, topics)
	if err != nil {
		return nil, err
	}
	var assignment []TopicPartition
	for _, topic := range response.Topics {
		if topic.Topic == nil || topic.ErrorCode != protocol.ErrorCodeNone {
			continue
		}
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != protocol.ErrorCodeNone {
				continue
			}
			assignment = append(assignment, TopicPartition{Topic: *topic.Topic, Partition: partition.Partition})
		}
	}
	if len(assignment) == 0 {
		return nil, fmt.Errorf("subscription has no assigned partitions")
	}
	return assignment, nil
}

func (c *Client) assignWithOffsets(ctx context.Context, coordinator transport.Endpoint, assignment []TopicPartition) error {
	nextOffsets := make(map[imetadata.TopicPartition]int64, len(assignment))
	for _, partition := range assignment {
		offset, err := c.fetchCommittedOffset(ctx, coordinator, partition.Topic, partition.Partition)
		if err != nil {
			return err
		}
		if offset < 0 {
			offset, err = c.resetOffset(ctx, partition.Topic, partition.Partition)
			if err != nil {
				return err
			}
		}
		key := imetadata.TopicPartition{Topic: partition.Topic, Partition: partition.Partition}
		nextOffsets[key] = offset
	}
	c.mu.Lock()
	revoked := diffAssignments(c.assignment, assignment)
	assigned := diffAssignments(assignment, c.assignment)
	c.mu.Unlock()
	c.notifyPartitionsRevoked(ctx, revoked)
	c.mu.Lock()
	c.assignment = append([]TopicPartition(nil), assignment...)
	c.nextOffsets = nextOffsets
	c.consumedOffsets = make(map[imetadata.TopicPartition]int64)
	c.paused = keepPaused(c.paused, assignment)
	c.mu.Unlock()
	c.notifyPartitionsAssigned(ctx, assigned)
	return nil
}

func (c *Client) fetchCommittedOffset(ctx context.Context, coordinator transport.Endpoint, topic string, partition int32) (int64, error) {
	groupID := c.options.GroupID
	body := message.OffsetFetchRequestBody{
		GroupID: &groupID,
		Topics: []message.OffsetFetchTopic{
			{Topic: topic, Partitions: []message.OffsetFetchPartition{{Partition: partition}}},
		},
	}
	var lastErr error
attemptLoop:
	for attempt := 1; attempt <= routeRefreshAttempts; attempt++ {
		response, err := c.protocol.Send(ctx, coordinator, protocol.ApiKeyOffsetFetch, protocol.DefaultAPIVersion, body)
		if err != nil {
			lastErr = err
			if attempt < routeRefreshAttempts && c.shouldRefreshCoordinator(ctx, err) {
				coordinator, err = c.findCoordinator(ctx, c.options.GroupID)
				if err != nil {
					return 0, lastErr
				}
				continue
			}
			return 0, err
		}
		typed, ok := response.Body.(message.OffsetFetchResponseBody)
		if !ok {
			return 0, errors.New("unexpected OffsetFetch response body")
		}
		for _, topicResponse := range typed.Topics {
			if topicResponse.Topic == nil || *topicResponse.Topic != topic {
				continue
			}
			for _, partitionResponse := range topicResponse.Partitions {
				if partitionResponse.Partition != partition {
					continue
				}
				if partitionResponse.ErrorCode != protocol.ErrorCodeNone {
					err := &protocol.ClientError{Code: partitionResponse.ErrorCode, Message: "offset fetch failed"}
					lastErr = err
					if attempt < routeRefreshAttempts && c.shouldRefreshCoordinator(ctx, err) {
						var refreshErr error
						coordinator, refreshErr = c.findCoordinator(ctx, c.options.GroupID)
						if refreshErr != nil {
							return 0, lastErr
						}
						continue attemptLoop
					}
					return 0, err
				}
				return partitionResponse.Offset, nil
			}
		}
		lastErr = errors.New("offset fetch response missing partition")
	}
	return 0, lastErr
}

func (c *Client) resetOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	switch c.options.OffsetReset {
	case OffsetResetEarliest:
		return c.listOffset(ctx, topic, partition, message.ListOffsetsEarliestTimestamp)
	case OffsetResetLatest:
		return c.listOffset(ctx, topic, partition, message.ListOffsetsLatestTimestamp)
	case OffsetResetNone:
		return 0, ErrNoOffset
	default:
		return 0, fmt.Errorf("unsupported offset reset strategy: %s", c.options.OffsetReset)
	}
}

func (c *Client) listOffset(ctx context.Context, topic string, partition int32, timestamp int64) (int64, error) {
	var lastErr error
attemptLoop:
	for attempt := 1; attempt <= routeRefreshAttempts; attempt++ {
		route, err := c.route(ctx, topic, partition, attempt)
		if err != nil {
			return 0, err
		}
		body := message.ListOffsetsRequestBody{
			ReplicaID:      -1,
			IsolationLevel: 0,
			Topics: []message.ListOffsetsTopicRequest{
				{
					Topic: topic,
					Partitions: []message.ListOffsetsPartitionRequest{
						{
							Partition:          partition,
							CurrentLeaderEpoch: route.LeaderEpoch,
							Timestamp:          timestamp,
							MaxNumOffsets:      1,
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
			return 0, err
		}
		typed, ok := response.Body.(message.ListOffsetsResponseBody)
		if !ok {
			return 0, errors.New("unexpected ListOffsets response body")
		}
		for _, topicResponse := range typed.Topics {
			if topicResponse.Topic == nil || *topicResponse.Topic != topic {
				continue
			}
			for _, partitionResponse := range topicResponse.Partitions {
				if partitionResponse.Partition != partition {
					continue
				}
				if partitionResponse.ErrorCode != protocol.ErrorCodeNone {
					err := &protocol.ClientError{Code: partitionResponse.ErrorCode, Message: "list offsets failed"}
					lastErr = err
					if attempt < routeRefreshAttempts && shouldRefreshRoute(ctx, err) {
						continue attemptLoop
					}
					return 0, err
				}
				return partitionResponse.Offset, nil
			}
		}
		lastErr = errors.New("list offsets response missing partition")
	}
	return 0, lastErr
}

func (c *Client) startHeartbeatLoop(session GroupSession) {
	c.stopHeartbeatLoop()
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.heartbeatCancel = cancel
	c.mu.Unlock()
	interval := c.options.HeartbeatInterval
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.heartbeat(ctx, session); err != nil {
					c.logger.Warn(ctx, "consumer heartbeat failed", observability.Error(err))
					if c.shouldRejoin(ctx, err) {
						rejoinCtx, cancel := context.WithTimeout(context.Background(), c.options.SessionTimeout)
						if rejoinErr := c.rejoin(rejoinCtx); rejoinErr != nil {
							c.logger.Error(rejoinCtx, "consumer rejoin failed", observability.Error(rejoinErr))
						}
						cancel()
					}
				}
			}
		}
	}()
}

func (c *Client) stopHeartbeatLoop() {
	c.mu.Lock()
	cancel := c.heartbeatCancel
	c.heartbeatCancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Client) startAutoCommitLoop() {
	if !c.options.EnableAutoCommit {
		c.stopAutoCommitLoop()
		return
	}
	c.stopAutoCommitLoop()
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.autoCommitCancel = cancel
	c.mu.Unlock()
	go func() {
		ticker := time.NewTicker(c.options.AutoCommitInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				commitCtx, commitCancel := context.WithTimeout(context.Background(), c.options.SessionTimeout)
				if err := c.Commit(commitCtx); err != nil {
					c.logger.Warn(commitCtx, "consumer auto commit failed", observability.Error(err))
				}
				commitCancel()
			}
		}
	}()
}

func (c *Client) stopAutoCommitLoop() {
	c.mu.Lock()
	cancel := c.autoCommitCancel
	c.autoCommitCancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Client) rejoin(ctx context.Context) error {
	c.mu.Lock()
	topics := append([]string(nil), c.subscribedTopics...)
	c.mu.Unlock()
	if len(topics) == 0 {
		return nil
	}
	return c.joinSubscription(ctx, topics)
}

func (c *Client) heartbeat(ctx context.Context, session GroupSession) error {
	groupID := session.GroupID
	memberID := session.MemberID
	body := message.HeartbeatRequestBody{
		GroupID:      &groupID,
		GenerationID: session.GenerationID,
		MemberID:     &memberID,
	}
	coordinator := session.Coordinator
	var lastErr error
	for attempt := 1; attempt <= routeRefreshAttempts; attempt++ {
		response, err := c.protocol.Send(ctx, coordinator, protocol.ApiKeyHeartbeat, protocol.DefaultAPIVersion, body)
		if err != nil {
			lastErr = err
			if attempt < routeRefreshAttempts && c.shouldRefreshCoordinator(ctx, err) {
				coordinator, err = c.refreshCoordinator(ctx)
				if err != nil {
					return lastErr
				}
				continue
			}
			return err
		}
		typed, ok := response.Body.(message.HeartbeatResponseBody)
		if !ok {
			return errors.New("unexpected Heartbeat response body")
		}
		if typed.ErrorCode != protocol.ErrorCodeNone {
			err := &protocol.ClientError{Code: typed.ErrorCode, Message: "heartbeat failed"}
			lastErr = err
			if attempt < routeRefreshAttempts && c.shouldRefreshCoordinator(ctx, err) {
				var refreshErr error
				coordinator, refreshErr = c.refreshCoordinator(ctx)
				if refreshErr != nil {
					return lastErr
				}
				continue
			}
			return err
		}
		return nil
	}
	return lastErr
}

func (c *Client) fetchPartition(ctx context.Context, topic string, partition int32) ([]Record, error) {
	key := imetadata.TopicPartition{Topic: topic, Partition: partition}
	c.mu.Lock()
	fetchOffset := c.nextOffsets[key]
	c.mu.Unlock()
	var lastErr error
	for attempt := 1; attempt <= routeRefreshAttempts; attempt++ {
		route, err := c.route(ctx, topic, partition, attempt)
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
			lastErr = err
			if attempt < routeRefreshAttempts && shouldRefreshRoute(ctx, err) {
				continue
			}
			return nil, err
		}
		typed, ok := response.Body.(message.FetchResponseBody)
		if !ok {
			return nil, errors.New("unexpected Fetch response body")
		}
		records, err := c.toRecords(topic, partition, fetchOffset, typed)
		if err != nil {
			lastErr = err
			if attempt < routeRefreshAttempts && shouldRefreshRoute(ctx, err) {
				continue
			}
			return nil, err
		}
		return records, nil
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

func (c *Client) shouldRefreshCoordinator(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	if protocol.RequiresCoordinatorRefresh(err) {
		return true
	}
	var clientErr *protocol.ClientError
	return !errors.As(err, &clientErr)
}

func (c *Client) shouldRejoin(ctx context.Context, err error) bool {
	return c.shouldRefreshCoordinator(ctx, err) || protocol.IsRetriable(err)
}

func (c *Client) recordError(ctx context.Context, span trace.Span, msg string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	c.logger.Error(ctx, msg, observability.Error(err))
}

func (c *Client) isAssignedLocked(key imetadata.TopicPartition) bool {
	for _, partition := range c.assignment {
		if partition.Topic == key.Topic && partition.Partition == key.Partition {
			return true
		}
	}
	return false
}

func (c *Client) notifyPartitionsRevoked(ctx context.Context, partitions []TopicPartition) {
	if c.options.RebalanceListener != nil && len(partitions) > 0 {
		c.options.RebalanceListener.OnPartitionsRevoked(ctx, append([]TopicPartition(nil), partitions...))
	}
}

func (c *Client) notifyPartitionsAssigned(ctx context.Context, partitions []TopicPartition) {
	if c.options.RebalanceListener != nil && len(partitions) > 0 {
		c.options.RebalanceListener.OnPartitionsAssigned(ctx, append([]TopicPartition(nil), partitions...))
	}
}

func diffAssignments(left []TopicPartition, right []TopicPartition) []TopicPartition {
	var diff []TopicPartition
	for _, candidate := range left {
		if !slices.ContainsFunc(right, func(existing TopicPartition) bool {
			return existing.Topic == candidate.Topic && existing.Partition == candidate.Partition
		}) {
			diff = append(diff, candidate)
		}
	}
	return diff
}

func keepPaused(paused map[imetadata.TopicPartition]struct{}, assignment []TopicPartition) map[imetadata.TopicPartition]struct{} {
	next := make(map[imetadata.TopicPartition]struct{})
	for _, partition := range assignment {
		key := imetadata.TopicPartition{Topic: partition.Topic, Partition: partition.Partition}
		if _, ok := paused[key]; ok {
			next[key] = struct{}{}
		}
	}
	return next
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
					nextOffset := offset + 1
					records = append(records, Record{
						Topic:     topic,
						Partition: partition,
						Offset:    offset,
						Key:       batchRecord.Key,
						Value:     batchRecord.Value,
						Headers:   batchRecord.Headers,
						Timestamp: batch.BaseTimestamp + batchRecord.TimestampDelta,
					})
					c.mu.Lock()
					c.nextOffsets[key] = nextOffset
					c.consumedOffsets[key] = nextOffset
					c.mu.Unlock()
				}
			}
		}
	}
	return records, nil
}
