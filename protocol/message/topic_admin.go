package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

// TopicAdminRequestBody is the body for topic management APIs.
type TopicAdminRequestBody struct {
	Topic          *string
	PartitionCount int32
	Partition      int32
	LeaderID       int32
	LeaderEpoch    int32
	ReplicaNodes   []int32
	ISRNodes       []int32
}

// TopicAdminResponseBody is the body for topic management responses.
type TopicAdminResponseBody struct {
	Topic      *string
	Partitions []TopicAdminPartitionResponse
}

// TopicAdminPartitionResponse describes one partition result from a topic management API.
type TopicAdminPartitionResponse struct {
	Partition   int32
	ErrorCode   protocol.ErrorCode
	LeaderEpoch int32
}
