package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeTopicAdminRequestBody encodes apiKey=50/51/52, apiVersion=0 request body.
func EncodeTopicAdminRequestBody(w *Writer, body message.TopicAdminRequestBody) {
	w.WriteNullableString(body.Topic)
	w.WriteInt32(body.PartitionCount)
	w.WriteInt32(body.Partition)
	w.WriteInt32(body.LeaderID)
	w.WriteInt32(body.LeaderEpoch)
	w.WriteInt32Array(body.ReplicaNodes)
	w.WriteInt32Array(body.ISRNodes)
}

func encodeTopicAdminRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.TopicAdminRequestBody)
	if !ok {
		return fmt.Errorf("expected message.TopicAdminRequestBody, got %T", body)
	}
	EncodeTopicAdminRequestBody(w, typedBody)
	return nil
}

// DecodeTopicAdminResponseBody decodes apiKey=50/51/52, apiVersion=0 response body.
func DecodeTopicAdminResponseBody(r *Reader) (message.TopicAdminResponseBody, error) {
	topic, err := r.ReadNullableString()
	if err != nil {
		return message.TopicAdminResponseBody{}, err
	}
	partitions, err := decodeTopicAdminPartitionResponses(r)
	if err != nil {
		return message.TopicAdminResponseBody{}, err
	}
	return message.TopicAdminResponseBody{Topic: topic, Partitions: partitions}, nil
}

func decodeTopicAdminResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeTopicAdminResponseBody(r)
}

func decodeTopicAdminPartitionResponses(r *Reader) ([]message.TopicAdminPartitionResponse, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	partitions := make([]message.TopicAdminPartitionResponse, 0, length)
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
		partitions = append(partitions, message.TopicAdminPartitionResponse{
			Partition:   partition,
			ErrorCode:   protocol.ErrorCodeFromCode(errorCode),
			LeaderEpoch: leaderEpoch,
		})
	}
	return partitions, nil
}
