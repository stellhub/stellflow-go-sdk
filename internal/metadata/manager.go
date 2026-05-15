package metadata

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
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

// Manager owns metadata refresh and route cache.
type Manager struct {
	client    *protocolclient.Client
	bootstrap []transport.Endpoint
	mu        sync.RWMutex
	response  message.MetadataResponseBody
	brokers   map[int32]transport.Endpoint
	routes    map[TopicPartition]PartitionRoute
}

// New creates a metadata manager.
func New(client *protocolclient.Client, bootstrap []transport.Endpoint) *Manager {
	return &Manager{
		client:    client,
		bootstrap: append([]transport.Endpoint(nil), bootstrap...),
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
	if len(m.bootstrap) == 0 {
		return message.MetadataResponseBody{}, fmt.Errorf("bootstrap endpoints must not be empty")
	}
	var lastErr error
	for _, endpoint := range m.bootstrap {
		response, err := m.client.Metadata(ctx, endpoint, topics)
		if err != nil {
			lastErr = err
			continue
		}
		m.update(response)
		return response, nil
	}
	if lastErr != nil {
		return message.MetadataResponseBody{}, lastErr
	}
	return message.MetadataResponseBody{}, fmt.Errorf("metadata refresh failed")
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
	if _, err := m.Refresh(ctx, []string{topic}); err != nil {
		return PartitionRoute{}, err
	}
	key := TopicPartition{Topic: topic, Partition: partition}
	m.mu.RLock()
	defer m.mu.RUnlock()
	route, ok := m.routes[key]
	if !ok {
		return PartitionRoute{}, fmt.Errorf("missing route for %s[%d]", topic, partition)
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
