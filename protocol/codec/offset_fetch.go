package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeOffsetFetchRequestBody encodes apiKey=6, apiVersion=0 request body.
func EncodeOffsetFetchRequestBody(w *Writer, body message.OffsetFetchRequestBody) {
	w.WriteNullableString(body.GroupID)
	w.WriteArrayLen(len(body.Topics))
	for _, topic := range body.Topics {
		w.WriteNullableString(&topic.Topic)
		w.WriteArrayLen(len(topic.Partitions))
		for _, partition := range topic.Partitions {
			w.WriteInt32(partition.Partition)
		}
	}
}

func encodeOffsetFetchRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.OffsetFetchRequestBody)
	if !ok {
		return fmt.Errorf("expected message.OffsetFetchRequestBody, got %T", body)
	}
	EncodeOffsetFetchRequestBody(w, typedBody)
	return nil
}

// DecodeOffsetFetchResponseBody decodes apiKey=6, apiVersion=0 response body.
func DecodeOffsetFetchResponseBody(r *Reader) (message.OffsetFetchResponseBody, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return message.OffsetFetchResponseBody{}, err
	}
	topics := make([]message.OffsetFetchTopicResponse, 0, length)
	for range length {
		topic, err := r.ReadNullableString()
		if err != nil {
			return message.OffsetFetchResponseBody{}, err
		}
		partitions, err := decodeOffsetFetchPartitionResponses(r)
		if err != nil {
			return message.OffsetFetchResponseBody{}, err
		}
		topics = append(topics, message.OffsetFetchTopicResponse{Topic: topic, Partitions: partitions})
	}
	return message.OffsetFetchResponseBody{Topics: topics}, nil
}

func decodeOffsetFetchResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeOffsetFetchResponseBody(r)
}

func decodeOffsetFetchPartitionResponses(r *Reader) ([]message.OffsetFetchPartitionResponse, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	partitions := make([]message.OffsetFetchPartitionResponse, 0, length)
	for range length {
		partition, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		offset, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		metadata, err := r.ReadNullableString()
		if err != nil {
			return nil, err
		}
		errorCode, err := r.ReadInt16()
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, message.OffsetFetchPartitionResponse{
			Partition: partition,
			Offset:    offset,
			Metadata:  metadata,
			ErrorCode: protocol.ErrorCodeFromCode(errorCode),
		})
	}
	return partitions, nil
}
