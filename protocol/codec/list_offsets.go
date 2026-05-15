package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeListOffsetsRequestBody encodes apiKey=4, apiVersion=0 request body.
func EncodeListOffsetsRequestBody(w *Writer, body message.ListOffsetsRequestBody) {
	w.WriteInt32(body.ReplicaID)
	w.WriteInt8(body.IsolationLevel)
	w.WriteArrayLen(len(body.Topics))
	for _, topic := range body.Topics {
		w.WriteNullableString(&topic.Topic)
		w.WriteArrayLen(len(topic.Partitions))
		for _, partition := range topic.Partitions {
			w.WriteInt32(partition.Partition)
			w.WriteInt32(partition.CurrentLeaderEpoch)
			w.WriteInt64(partition.Timestamp)
			w.WriteInt32(partition.MaxNumOffsets)
		}
	}
}

func encodeListOffsetsRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.ListOffsetsRequestBody)
	if !ok {
		return fmt.Errorf("expected message.ListOffsetsRequestBody, got %T", body)
	}
	EncodeListOffsetsRequestBody(w, typedBody)
	return nil
}

// DecodeListOffsetsResponseBody decodes apiKey=4, apiVersion=0 response body.
func DecodeListOffsetsResponseBody(r *Reader) (message.ListOffsetsResponseBody, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return message.ListOffsetsResponseBody{}, err
	}
	topics := make([]message.ListOffsetsTopicResponse, 0, length)
	for range length {
		topic, err := r.ReadNullableString()
		if err != nil {
			return message.ListOffsetsResponseBody{}, err
		}
		partitions, err := decodeListOffsetsPartitionResponses(r)
		if err != nil {
			return message.ListOffsetsResponseBody{}, err
		}
		topics = append(topics, message.ListOffsetsTopicResponse{Topic: topic, Partitions: partitions})
	}
	return message.ListOffsetsResponseBody{Topics: topics}, nil
}

func decodeListOffsetsResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeListOffsetsResponseBody(r)
}

func decodeListOffsetsPartitionResponses(r *Reader) ([]message.ListOffsetsPartitionResponse, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	partitions := make([]message.ListOffsetsPartitionResponse, 0, length)
	for range length {
		partition, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		errorCode, err := r.ReadInt16()
		if err != nil {
			return nil, err
		}
		leaderEpoch, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		timestamp, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		offset, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		offsets, err := r.ReadInt64Array()
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, message.ListOffsetsPartitionResponse{
			Partition:   partition,
			ErrorCode:   protocol.ErrorCodeFromCode(errorCode),
			LeaderEpoch: leaderEpoch,
			Timestamp:   timestamp,
			Offset:      offset,
			Offsets:     offsets,
		})
	}
	return partitions, nil
}
