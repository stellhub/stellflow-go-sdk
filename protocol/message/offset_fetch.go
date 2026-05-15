package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

// OffsetFetchRequestBody is the body for apiKey=6, apiVersion=0.
type OffsetFetchRequestBody struct {
	GroupID *string
	Topics  []OffsetFetchTopic
}

// OffsetFetchTopic contains requested partitions for one topic.
type OffsetFetchTopic struct {
	Topic      string
	Partitions []OffsetFetchPartition
}

// OffsetFetchPartition identifies one partition to query.
type OffsetFetchPartition struct {
	Partition int32
}

// OffsetFetchResponseBody is the response body for apiKey=6, apiVersion=0.
type OffsetFetchResponseBody struct {
	Topics []OffsetFetchTopicResponse
}

// OffsetFetchTopicResponse contains committed offsets for one topic.
type OffsetFetchTopicResponse struct {
	Topic      *string
	Partitions []OffsetFetchPartitionResponse
}

// OffsetFetchPartitionResponse contains committed offset metadata for one partition.
type OffsetFetchPartitionResponse struct {
	Partition int32
	Offset    int64
	Metadata  *string
	ErrorCode protocol.ErrorCode
}
