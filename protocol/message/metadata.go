package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

// MetadataRequestBody is the body for apiKey=1, apiVersion=0.
type MetadataRequestBody struct {
	Topics                             []MetadataTopicRequest
	IncludeClusterAuthorizedOperations bool
	IncludeTopicAuthorizedOperations   bool
	AllowAutoTopicCreation             bool
}

// MetadataTopicRequest identifies a topic to query.
type MetadataTopicRequest struct {
	Topic string
}

// MetadataResponseBody is the response body for apiKey=1, apiVersion=0.
type MetadataResponseBody struct {
	ClusterID                   *string
	ControllerID                int32
	Brokers                     []MetadataBroker
	Topics                      []MetadataTopicResponse
	ClusterAuthorizedOperations int32
}

// MetadataBroker describes a broker endpoint.
type MetadataBroker struct {
	BrokerID int32
	Host     *string
	Port     int32
	Rack     *string
}

// MetadataTopicResponse describes one topic.
type MetadataTopicResponse struct {
	ErrorCode                 protocol.ErrorCode
	Topic                     *string
	Internal                  bool
	Partitions                []MetadataPartitionResponse
	TopicAuthorizedOperations int32
}

// MetadataPartitionResponse describes one topic partition.
type MetadataPartitionResponse struct {
	ErrorCode           protocol.ErrorCode
	Partition           int32
	LeaderID            int32
	LeaderEpoch         int32
	ReplicaNodes        []int32
	ISRNodes            []int32
	OfflineReplicaNodes []int32
}
