package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

// OffsetCommitRequestBody is the body for apiKey=5, apiVersion=0.
type OffsetCommitRequestBody struct {
	GroupID *string
	Topics  []OffsetCommitTopic
}

// OffsetCommitTopic contains committed offsets for one topic.
type OffsetCommitTopic struct {
	Topic      string
	Partitions []OffsetCommitPartition
}

// OffsetCommitPartition contains one partition commit.
type OffsetCommitPartition struct {
	Partition int32
	Offset    int64
	Metadata  *string
}

// OffsetCommitResponseBody is the response body for apiKey=5, apiVersion=0.
type OffsetCommitResponseBody struct {
	Topics []OffsetCommitTopicResponse
}

// OffsetCommitTopicResponse contains commit results for one topic.
type OffsetCommitTopicResponse struct {
	Topic      *string
	Partitions []OffsetCommitPartitionResponse
}

// OffsetCommitPartitionResponse contains one partition commit result.
type OffsetCommitPartitionResponse struct {
	Partition int32
	ErrorCode protocol.ErrorCode
}
