package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeProduceRequestBody encodes apiKey=2, apiVersion=0 request body.
func EncodeProduceRequestBody(w *Writer, body message.ProduceRequestBody) {
	w.WriteNullableString(body.TransactionalID)
	w.WriteInt16(body.Acks)
	w.WriteInt32(body.TimeoutMs)
	w.WriteArrayLen(len(body.TopicData))
	for _, topic := range body.TopicData {
		w.WriteNullableString(&topic.Topic)
		w.WriteArrayLen(len(topic.Partitions))
		for _, partition := range topic.Partitions {
			w.WriteInt32(partition.Partition)
			w.WriteBytes(partition.Records)
		}
	}
}

func encodeProduceRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.ProduceRequestBody)
	if !ok {
		return fmt.Errorf("expected message.ProduceRequestBody, got %T", body)
	}
	EncodeProduceRequestBody(w, typedBody)
	return nil
}

// DecodeProduceResponseBody decodes apiKey=2, apiVersion=0 response body.
func DecodeProduceResponseBody(r *Reader) (message.ProduceResponseBody, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return message.ProduceResponseBody{}, err
	}
	responses := make([]message.ProduceTopicResponse, 0, length)
	for range length {
		topic, err := r.ReadNullableString()
		if err != nil {
			return message.ProduceResponseBody{}, err
		}
		partitions, err := decodeProducePartitionResponses(r)
		if err != nil {
			return message.ProduceResponseBody{}, err
		}
		responses = append(responses, message.ProduceTopicResponse{Topic: topic, Partitions: partitions})
	}
	return message.ProduceResponseBody{Responses: responses}, nil
}

func decodeProduceResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeProduceResponseBody(r)
}

func decodeProducePartitionResponses(r *Reader) ([]message.ProducePartitionResponse, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	partitions := make([]message.ProducePartitionResponse, 0, length)
	for range length {
		partition, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		errorCode, err := r.ReadInt16()
		if err != nil {
			return nil, err
		}
		baseOffset, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		currentLeaderEpoch, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		logAppendTimeMs, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		logStartOffset, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, message.ProducePartitionResponse{
			Partition:          partition,
			ErrorCode:          protocol.ErrorCodeFromCode(errorCode),
			BaseOffset:         baseOffset,
			CurrentLeaderEpoch: currentLeaderEpoch,
			LogAppendTimeMs:    logAppendTimeMs,
			LogStartOffset:     logStartOffset,
		})
	}
	return partitions, nil
}
