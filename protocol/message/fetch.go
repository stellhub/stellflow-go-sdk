package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

// FetchRequestBody is the body for apiKey=3, apiVersion=0.
type FetchRequestBody struct {
	ReplicaID       int32
	MaxWaitMs       int32
	MinBytes        int32
	MaxBytes        int32
	IsolationLevel  int8
	SessionID       int32
	TopicPartitions []FetchTopicRequest
}

// FetchTopicRequest contains fetch partitions for one topic.
type FetchTopicRequest struct {
	Topic      string
	Partitions []FetchPartitionRequest
}

// FetchPartitionRequest contains fetch coordinates for one partition.
type FetchPartitionRequest struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

// FetchResponseBody is the response body for apiKey=3, apiVersion=0.
type FetchResponseBody struct {
	SessionID int32
	Responses []FetchTopicResponse
}

// FetchTopicResponse contains fetch results for one topic.
type FetchTopicResponse struct {
	Topic      *string
	Partitions []FetchPartitionResponse
}

// FetchPartitionResponse contains a fetched RecordBatchSet for one partition.
type FetchPartitionResponse struct {
	Partition           int32
	ErrorCode           protocol.ErrorCode
	HighWatermark       int64
	LogStartOffset      int64
	LastStableOffset    int64
	AbortedTransactions []AbortedTransaction
	Records             []byte
}

// AbortedTransaction identifies an aborted transactional range.
type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}
