package metadata

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/observability"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TopicPartition identifies one topic partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// PartitionRoute contains the current leader route for one partition.
type PartitionRoute struct {
	Topic       string
	Partition   int32
	LeaderID    int32
	LeaderEpoch int32
	Endpoint    transport.Endpoint
}

// Options configures metadata manager behavior.
type Options struct {
	Observability observability.Options
}

// Manager owns metadata refresh and route cache.
type Manager struct {
	client    *protocolclient.Client
	bootstrap []transport.Endpoint
	logger    observability.Logger
	tracer    trace.Tracer
	mu        sync.RWMutex
	response  message.MetadataResponseBody
	brokers   map[int32]transport.Endpoint
	routes    map[TopicPartition]PartitionRoute
}

// New creates a metadata manager.
func New(client *protocolclient.Client, bootstrap []transport.Endpoint) *Manager {
	return NewWithOptions(client, bootstrap, Options{})
}

// NewWithOptions creates a metadata manager with explicit options.
func NewWithOptions(client *protocolclient.Client, bootstrap []transport.Endpoint, options Options) *Manager {
	obs := observability.Normalize(options.Observability)
	return &Manager{
		client:    client,
		bootstrap: append([]transport.Endpoint(nil), bootstrap...),
		logger:    obs.Logger,
		tracer:    observability.Tracer(obs),
		brokers:   make(map[int32]transport.Endpoint),
		routes:    make(map[TopicPartition]PartitionRoute),
	}
}

// BootstrapEndpoint returns the first bootstrap endpoint.
func (m *Manager) BootstrapEndpoint() (transport.Endpoint, error) {
	if len(m.bootstrap) == 0 {
		return transport.Endpoint{}, fmt.Errorf("bootstrap endpoints must not be empty")
	}
	return m.bootstrap[0], nil
}

// Refresh reloads metadata for topics.
func (m *Manager) Refresh(ctx context.Context, topics []string) (message.MetadataResponseBody, error) {
	ctx, span := m.tracer.Start(ctx, "stellflow.metadata.refresh",
		trace.WithAttributes(attribute.Int("stellflow.topic_count", len(topics))),
	)
	defer span.End()
	if len(m.bootstrap) == 0 {
		err := fmt.Errorf("bootstrap endpoints must not be empty")
		m.recordRefreshError(ctx, span, err)
		return message.MetadataResponseBody{}, err
	}
	var lastErr error
	for _, endpoint := range m.bootstrap {
		m.logger.Debug(ctx, "refreshing stellflow metadata",
			observability.String("endpoint", endpoint.Address()),
			observability.Int("topic_count", len(topics)),
		)
		response, err := m.client.Metadata(ctx, endpoint, topics)
		if err != nil {
			lastErr = err
			m.logger.Warn(ctx, "metadata refresh failed on bootstrap endpoint",
				observability.String("endpoint", endpoint.Address()),
				observability.Error(err),
			)
			continue
		}
		m.update(response)
		m.logger.Info(ctx, "metadata refresh completed",
			observability.String("endpoint", endpoint.Address()),
			observability.Int("broker_count", len(response.Brokers)),
			observability.Int("topic_count", len(response.Topics)),
		)
		return response, nil
	}
	if lastErr != nil {
		m.recordRefreshError(ctx, span, lastErr)
		return message.MetadataResponseBody{}, lastErr
	}
	err := fmt.Errorf("metadata refresh failed")
	m.recordRefreshError(ctx, span, err)
	return message.MetadataResponseBody{}, err
}

func (m *Manager) update(response message.MetadataResponseBody) {
	brokers := make(map[int32]transport.Endpoint)
	for _, broker := range response.Brokers {
		if broker.Host == nil {
			continue
		}
		brokers[broker.BrokerID] = transport.Endpoint{Host: *broker.Host, Port: int(broker.Port)}
	}
	routes := make(map[TopicPartition]PartitionRoute)
	for _, topic := range response.Topics {
		if topic.Topic == nil || topic.ErrorCode != protocol.ErrorCodeNone {
			continue
		}
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != protocol.ErrorCodeNone {
				continue
			}
			endpoint, ok := brokers[partition.LeaderID]
			if !ok {
				continue
			}
			key := TopicPartition{Topic: *topic.Topic, Partition: partition.Partition}
			routes[key] = PartitionRoute{
				Topic:       *topic.Topic,
				Partition:   partition.Partition,
				LeaderID:    partition.LeaderID,
				LeaderEpoch: partition.LeaderEpoch,
				Endpoint:    endpoint,
			}
		}
	}
	m.mu.Lock()
	m.response = response
	m.brokers = brokers
	m.routes = routes
	m.mu.Unlock()
}

// Route returns the cached leader route, refreshing metadata when absent.
func (m *Manager) Route(ctx context.Context, topic string, partition int32) (PartitionRoute, error) {
	key := TopicPartition{Topic: topic, Partition: partition}
	m.mu.RLock()
	route, ok := m.routes[key]
	m.mu.RUnlock()
	if ok {
		return route, nil
	}
	if _, err := m.Refresh(ctx, []string{topic}); err != nil {
		return PartitionRoute{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	route, ok = m.routes[key]
	if !ok {
		return PartitionRoute{}, fmt.Errorf("missing route for %s[%d]", topic, partition)
	}
	return route, nil
}

// RefreshRoute reloads metadata and returns the current leader route.
func (m *Manager) RefreshRoute(ctx context.Context, topic string, partition int32) (PartitionRoute, error) {
	ctx, span := m.tracer.Start(ctx, "stellflow.metadata.refresh_route",
		trace.WithAttributes(
			attribute.String("stellflow.topic", topic),
			attribute.Int("stellflow.partition", int(partition)),
		),
	)
	defer span.End()
	if _, err := m.Refresh(ctx, []string{topic}); err != nil {
		m.recordRefreshError(ctx, span, err)
		return PartitionRoute{}, err
	}
	key := TopicPartition{Topic: topic, Partition: partition}
	m.mu.RLock()
	defer m.mu.RUnlock()
	route, ok := m.routes[key]
	if !ok {
		err := fmt.Errorf("missing route for %s[%d]", topic, partition)
		m.recordRefreshError(ctx, span, err)
		return PartitionRoute{}, err
	}
	return route, nil
}

// PartitionIDs returns known partition ids for topic.
func (m *Manager) PartitionIDs(ctx context.Context, topic string) ([]int32, error) {
	m.mu.RLock()
	ids := m.partitionIDsLocked(topic)
	m.mu.RUnlock()
	if len(ids) > 0 {
		return ids, nil
	}
	if _, err := m.Refresh(ctx, []string{topic}); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids = m.partitionIDsLocked(topic)
	if len(ids) == 0 {
		return nil, fmt.Errorf("missing partitions for topic %s", topic)
	}
	return ids, nil
}

// Snapshot returns the last metadata response.
func (m *Manager) Snapshot() message.MetadataResponseBody {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.response
}

func (m *Manager) partitionIDsLocked(topic string) []int32 {
	var ids []int32
	for key := range m.routes {
		if key.Topic == topic {
			ids = append(ids, key.Partition)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (m *Manager) recordRefreshError(ctx context.Context, span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	m.logger.Error(ctx, "metadata operation failed", observability.Error(err))
}
