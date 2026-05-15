package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

// ProduceRequestBody is the body for apiKey=2, apiVersion=0.
type ProduceRequestBody struct {
	TransactionalID *string
	Acks            int16
	TimeoutMs       int32
	TopicData       []ProduceTopicData
}

// ProduceTopicData contains Produce records for one topic.
type ProduceTopicData struct {
	Topic      string
	Partitions []ProducePartitionData
}

// ProducePartitionData contains a RecordBatchSet for one partition.
type ProducePartitionData struct {
	Partition int32
	Records   []byte
}

// ProduceResponseBody is the response body for apiKey=2, apiVersion=0.
type ProduceResponseBody struct {
	Responses []ProduceTopicResponse
}

// ProduceTopicResponse contains Produce results for one topic.
type ProduceTopicResponse struct {
	Topic      *string
	Partitions []ProducePartitionResponse
}

// ProducePartitionResponse contains Produce result metadata for one partition.
type ProducePartitionResponse struct {
	Partition          int32
	ErrorCode          protocol.ErrorCode
	BaseOffset         int64
	CurrentLeaderEpoch int32
	LogAppendTimeMs    int64
	LogStartOffset     int64
}
