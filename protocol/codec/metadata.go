package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeMetadataRequestBody encodes apiKey=1, apiVersion=0 request body.
func EncodeMetadataRequestBody(w *Writer, body message.MetadataRequestBody) {
	w.WriteArrayLen(len(body.Topics))
	for _, topic := range body.Topics {
		w.WriteNullableString(&topic.Topic)
	}
	w.WriteBool(body.IncludeClusterAuthorizedOperations)
	w.WriteBool(body.IncludeTopicAuthorizedOperations)
	w.WriteBool(body.AllowAutoTopicCreation)
}

func encodeMetadataRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.MetadataRequestBody)
	if !ok {
		return fmt.Errorf("expected message.MetadataRequestBody, got %T", body)
	}
	EncodeMetadataRequestBody(w, typedBody)
	return nil
}

// DecodeMetadataResponseBody decodes apiKey=1, apiVersion=0 response body.
func DecodeMetadataResponseBody(r *Reader) (message.MetadataResponseBody, error) {
	clusterID, err := r.ReadNullableString()
	if err != nil {
		return message.MetadataResponseBody{}, err
	}
	controllerID, err := r.ReadInt32()
	if err != nil {
		return message.MetadataResponseBody{}, err
	}
	brokers, err := decodeMetadataBrokers(r)
	if err != nil {
		return message.MetadataResponseBody{}, err
	}
	topics, err := decodeMetadataTopics(r)
	if err != nil {
		return message.MetadataResponseBody{}, err
	}
	clusterAuthorizedOperations, err := r.ReadInt32()
	if err != nil {
		return message.MetadataResponseBody{}, err
	}
	return message.MetadataResponseBody{
		ClusterID:                   clusterID,
		ControllerID:                controllerID,
		Brokers:                     brokers,
		Topics:                      topics,
		ClusterAuthorizedOperations: clusterAuthorizedOperations,
	}, nil
}

func decodeMetadataResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeMetadataResponseBody(r)
}

func decodeMetadataBrokers(r *Reader) ([]message.MetadataBroker, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	brokers := make([]message.MetadataBroker, 0, length)
	for range length {
		brokerID, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		host, err := r.ReadNullableString()
		if err != nil {
			return nil, err
		}
		port, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		rack, err := r.ReadNullableString()
		if err != nil {
			return nil, err
		}
		brokers = append(brokers, message.MetadataBroker{
			BrokerID: brokerID,
			Host:     host,
			Port:     port,
			Rack:     rack,
		})
	}
	return brokers, nil
}

func decodeMetadataTopics(r *Reader) ([]message.MetadataTopicResponse, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	topics := make([]message.MetadataTopicResponse, 0, length)
	for range length {
		topicErrorCode, err := r.ReadInt16()
		if err != nil {
			return nil, err
		}
		topic, err := r.ReadNullableString()
		if err != nil {
			return nil, err
		}
		internal, err := r.ReadBool()
		if err != nil {
			return nil, err
		}
		partitions, err := decodeMetadataPartitions(r)
		if err != nil {
			return nil, err
		}
		topicAuthorizedOperations, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		topics = append(topics, message.MetadataTopicResponse{
			ErrorCode:                 protocol.ErrorCodeFromCode(topicErrorCode),
			Topic:                     topic,
			Internal:                  internal,
			Partitions:                partitions,
			TopicAuthorizedOperations: topicAuthorizedOperations,
		})
	}
	return topics, nil
}

func decodeMetadataPartitions(r *Reader) ([]message.MetadataPartitionResponse, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	partitions := make([]message.MetadataPartitionResponse, 0, length)
	for range length {
		partitionErrorCode, err := r.ReadInt16()
		if err != nil {
			return nil, err
		}
		partition, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		leaderID, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		leaderEpoch, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		replicaNodes, err := r.ReadInt32Array()
		if err != nil {
			return nil, err
		}
		isrNodes, err := r.ReadInt32Array()
		if err != nil {
			return nil, err
		}
		offlineReplicaNodes, err := r.ReadInt32Array()
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, message.MetadataPartitionResponse{
			ErrorCode:           protocol.ErrorCodeFromCode(partitionErrorCode),
			Partition:           partition,
			LeaderID:            leaderID,
			LeaderEpoch:         leaderEpoch,
			ReplicaNodes:        replicaNodes,
			ISRNodes:            isrNodes,
			OfflineReplicaNodes: offlineReplicaNodes,
		})
	}
	return partitions, nil
}
