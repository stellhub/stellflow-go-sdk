package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeFetchRequestBody encodes apiKey=3, apiVersion=0 request body.
func EncodeFetchRequestBody(w *Writer, body message.FetchRequestBody) {
	w.WriteInt32(body.ReplicaID)
	w.WriteInt32(body.MaxWaitMs)
	w.WriteInt32(body.MinBytes)
	w.WriteInt32(body.MaxBytes)
	w.WriteInt8(body.IsolationLevel)
	w.WriteInt32(body.SessionID)
	w.WriteArrayLen(len(body.TopicPartitions))
	for _, topic := range body.TopicPartitions {
		w.WriteNullableString(&topic.Topic)
		w.WriteArrayLen(len(topic.Partitions))
		for _, partition := range topic.Partitions {
			w.WriteInt32(partition.Partition)
			w.WriteInt32(partition.CurrentLeaderEpoch)
			w.WriteInt64(partition.FetchOffset)
			w.WriteInt64(partition.LogStartOffset)
			w.WriteInt32(partition.PartitionMaxBytes)
		}
	}
}

func encodeFetchRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.FetchRequestBody)
	if !ok {
		return fmt.Errorf("expected message.FetchRequestBody, got %T", body)
	}
	EncodeFetchRequestBody(w, typedBody)
	return nil
}

// DecodeFetchResponseBody decodes apiKey=3, apiVersion=0 response body.
func DecodeFetchResponseBody(r *Reader) (message.FetchResponseBody, error) {
	sessionID, err := r.ReadInt32()
	if err != nil {
		return message.FetchResponseBody{}, err
	}
	length, err := r.ReadArrayLen()
	if err != nil {
		return message.FetchResponseBody{}, err
	}
	responses := make([]message.FetchTopicResponse, 0, length)
	for range length {
		topic, err := r.ReadNullableString()
		if err != nil {
			return message.FetchResponseBody{}, err
		}
		partitions, err := decodeFetchPartitionResponses(r)
		if err != nil {
			return message.FetchResponseBody{}, err
		}
		responses = append(responses, message.FetchTopicResponse{Topic: topic, Partitions: partitions})
	}
	return message.FetchResponseBody{SessionID: sessionID, Responses: responses}, nil
}

func decodeFetchResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeFetchResponseBody(r)
}

func decodeFetchPartitionResponses(r *Reader) ([]message.FetchPartitionResponse, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	partitions := make([]message.FetchPartitionResponse, 0, length)
	for range length {
		partition, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		errorCode, err := r.ReadInt16()
		if err != nil {
			return nil, err
		}
		highWatermark, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		logStartOffset, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		lastStableOffset, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		abortedTransactions, err := decodeAbortedTransactions(r)
		if err != nil {
			return nil, err
		}
		records, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, message.FetchPartitionResponse{
			Partition:           partition,
			ErrorCode:           protocol.ErrorCodeFromCode(errorCode),
			HighWatermark:       highWatermark,
			LogStartOffset:      logStartOffset,
			LastStableOffset:    lastStableOffset,
			AbortedTransactions: abortedTransactions,
			Records:             records,
		})
	}
	return partitions, nil
}

func decodeAbortedTransactions(r *Reader) ([]message.AbortedTransaction, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	transactions := make([]message.AbortedTransaction, 0, length)
	for range length {
		producerID, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		firstOffset, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, message.AbortedTransaction{ProducerID: producerID, FirstOffset: firstOffset})
	}
	return transactions, nil
}
