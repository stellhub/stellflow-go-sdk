package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

const (
	// ListOffsetsEarliestTimestamp asks for the earliest retained offset.
	ListOffsetsEarliestTimestamp int64 = -2

	// ListOffsetsLatestTimestamp asks for the log end offset.
	ListOffsetsLatestTimestamp int64 = -1
)

// ListOffsetsRequestBody is the body for apiKey=4, apiVersion=0.
type ListOffsetsRequestBody struct {
	ReplicaID      int32
	IsolationLevel int8
	Topics         []ListOffsetsTopicRequest
}

// ListOffsetsTopicRequest contains offset queries for one topic.
type ListOffsetsTopicRequest struct {
	Topic      string
	Partitions []ListOffsetsPartitionRequest
}

// ListOffsetsPartitionRequest contains one partition offset query.
type ListOffsetsPartitionRequest struct {
	Partition          int32
	CurrentLeaderEpoch int32
	Timestamp          int64
	MaxNumOffsets      int32
}

// ListOffsetsResponseBody is the response body for apiKey=4, apiVersion=0.
type ListOffsetsResponseBody struct {
	Topics []ListOffsetsTopicResponse
}

// ListOffsetsTopicResponse contains offset query results for one topic.
type ListOffsetsTopicResponse struct {
	Topic      *string
	Partitions []ListOffsetsPartitionResponse
}

// ListOffsetsPartitionResponse contains one partition offset query result.
type ListOffsetsPartitionResponse struct {
	Partition   int32
	ErrorCode   protocol.ErrorCode
	LeaderEpoch int32
	Timestamp   int64
	Offset      int64
	Offsets     []int64
}
