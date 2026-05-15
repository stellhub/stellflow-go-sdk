package codec_test

import (
	"reflect"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

func TestEncodeMetadataRequestBody(t *testing.T) {
	writer := codec.NewWriter()
	codec.EncodeMetadataRequestBody(writer, message.MetadataRequestBody{
		Topics: []message.MetadataTopicRequest{
			{Topic: "orders.created"},
			{Topic: "payments.created"},
		},
		IncludeClusterAuthorizedOperations: true,
		IncludeTopicAuthorizedOperations:   false,
		AllowAutoTopicCreation:             false,
	})
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}

	reader := codec.NewReader(data)
	length, err := reader.ReadArrayLen()
	if err != nil {
		t.Fatalf("ReadArrayLen() error = %v", err)
	}
	if length != 2 {
		t.Fatalf("topic length = %d, want 2", length)
	}
	firstTopic, err := reader.ReadString()
	if err != nil {
		t.Fatalf("first topic error = %v", err)
	}
	secondTopic, err := reader.ReadString()
	if err != nil {
		t.Fatalf("second topic error = %v", err)
	}
	includeClusterOps, err := reader.ReadBool()
	if err != nil {
		t.Fatalf("includeClusterOps error = %v", err)
	}
	includeTopicOps, err := reader.ReadBool()
	if err != nil {
		t.Fatalf("includeTopicOps error = %v", err)
	}
	allowAutoTopicCreation, err := reader.ReadBool()
	if err != nil {
		t.Fatalf("allowAutoTopicCreation error = %v", err)
	}

	if firstTopic != "orders.created" || secondTopic != "payments.created" {
		t.Fatalf("topics = %q/%q", firstTopic, secondTopic)
	}
	if !includeClusterOps || includeTopicOps || allowAutoTopicCreation {
		t.Fatalf("flags = %v/%v/%v", includeClusterOps, includeTopicOps, allowAutoTopicCreation)
	}
	if reader.Remaining() != 0 {
		t.Fatalf("remaining = %d, want 0", reader.Remaining())
	}
}

func TestDecodeMetadataResponseBody(t *testing.T) {
	clusterID := "stellflow-dev-cluster"
	host := "127.0.0.1"
	topic := "orders.created"

	writer := codec.NewWriter()
	writer.WriteNullableString(&clusterID)
	writer.WriteInt32(1)
	writer.WriteArrayLen(1)
	writer.WriteInt32(1)
	writer.WriteNullableString(&host)
	writer.WriteInt32(9092)
	writer.WriteNullableString(nil)
	writer.WriteArrayLen(1)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteNullableString(&topic)
	writer.WriteBool(false)
	writer.WriteArrayLen(1)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteInt32(0)
	writer.WriteInt32(1)
	writer.WriteInt32(7)
	writer.WriteInt32Array([]int32{1, 2})
	writer.WriteInt32Array([]int32{1})
	writer.WriteInt32Array([]int32{})
	writer.WriteInt32(0)
	writer.WriteInt32(0)
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}

	got, err := codec.DecodeMetadataResponseBody(codec.NewReader(data))
	if err != nil {
		t.Fatalf("DecodeMetadataResponseBody() error = %v", err)
	}
	want := message.MetadataResponseBody{
		ClusterID:    &clusterID,
		ControllerID: 1,
		Brokers: []message.MetadataBroker{
			{BrokerID: 1, Host: &host, Port: 9092, Rack: nil},
		},
		Topics: []message.MetadataTopicResponse{
			{
				ErrorCode: protocol.ErrorCodeNone,
				Topic:     &topic,
				Internal:  false,
				Partitions: []message.MetadataPartitionResponse{
					{
						ErrorCode:           protocol.ErrorCodeNone,
						Partition:           0,
						LeaderID:            1,
						LeaderEpoch:         7,
						ReplicaNodes:        []int32{1, 2},
						ISRNodes:            []int32{1},
						OfflineReplicaNodes: []int32{},
					},
				},
				TopicAuthorizedOperations: 0,
			},
		},
		ClusterAuthorizedOperations: 0,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeMetadataResponseBody() = %+v, want %+v", got, want)
	}
}

func TestDefaultRegistryEncodesAndDecodesMetadata(t *testing.T) {
	registry := codec.DefaultRegistry()

	encoded, err := registry.EncodeRequestBody(protocol.ApiKeyMetadata, protocol.DefaultAPIVersion, message.MetadataRequestBody{
		Topics: []message.MetadataTopicRequest{{Topic: "orders.created"}},
	})
	if err != nil {
		t.Fatalf("EncodeRequestBody() error = %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("EncodeRequestBody() returned empty bytes")
	}

	writer := codec.NewWriter()
	writer.WriteNullableString(nil)
	writer.WriteInt32(-1)
	writer.WriteArrayLen(0)
	writer.WriteArrayLen(0)
	writer.WriteInt32(0)
	responseBytes, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	body, err := registry.DecodeResponseBody(protocol.ApiKeyMetadata, protocol.DefaultAPIVersion, responseBytes)
	if err != nil {
		t.Fatalf("DecodeResponseBody() error = %v", err)
	}
	if _, ok := body.(message.MetadataResponseBody); !ok {
		t.Fatalf("DecodeResponseBody() type = %T", body)
	}
}
