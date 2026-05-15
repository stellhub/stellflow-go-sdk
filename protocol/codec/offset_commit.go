package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeOffsetCommitRequestBody encodes apiKey=5, apiVersion=0 request body.
func EncodeOffsetCommitRequestBody(w *Writer, body message.OffsetCommitRequestBody) {
	w.WriteNullableString(body.GroupID)
	w.WriteArrayLen(len(body.Topics))
	for _, topic := range body.Topics {
		w.WriteNullableString(&topic.Topic)
		w.WriteArrayLen(len(topic.Partitions))
		for _, partition := range topic.Partitions {
			w.WriteInt32(partition.Partition)
			w.WriteInt64(partition.Offset)
			w.WriteNullableString(partition.Metadata)
		}
	}
}

func encodeOffsetCommitRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.OffsetCommitRequestBody)
	if !ok {
		return fmt.Errorf("expected message.OffsetCommitRequestBody, got %T", body)
	}
	EncodeOffsetCommitRequestBody(w, typedBody)
	return nil
}

// DecodeOffsetCommitResponseBody decodes apiKey=5, apiVersion=0 response body.
func DecodeOffsetCommitResponseBody(r *Reader) (message.OffsetCommitResponseBody, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return message.OffsetCommitResponseBody{}, err
	}
	topics := make([]message.OffsetCommitTopicResponse, 0, length)
	for range length {
		topic, err := r.ReadNullableString()
		if err != nil {
			return message.OffsetCommitResponseBody{}, err
		}
		partitions, err := decodeOffsetCommitPartitionResponses(r)
		if err != nil {
			return message.OffsetCommitResponseBody{}, err
		}
		topics = append(topics, message.OffsetCommitTopicResponse{Topic: topic, Partitions: partitions})
	}
	return message.OffsetCommitResponseBody{Topics: topics}, nil
}

func decodeOffsetCommitResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeOffsetCommitResponseBody(r)
}

func decodeOffsetCommitPartitionResponses(r *Reader) ([]message.OffsetCommitPartitionResponse, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	partitions := make([]message.OffsetCommitPartitionResponse, 0, length)
	for range length {
		partition, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		errorCode, err := r.ReadInt16()
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, message.OffsetCommitPartitionResponse{
			Partition: partition,
			ErrorCode: protocol.ErrorCodeFromCode(errorCode),
		})
	}
	return partitions, nil
}
