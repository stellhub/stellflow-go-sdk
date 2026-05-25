package codec_test

import (
	"reflect"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

func TestEncodeTopicAdminRequestBody(t *testing.T) {
	topic := "orders"
	writer := codec.NewWriter()
	codec.EncodeTopicAdminRequestBody(writer, message.TopicAdminRequestBody{
		Topic:          &topic,
		PartitionCount: 3,
		Partition:      -1,
		LeaderID:       -1,
		LeaderEpoch:    -1,
		ReplicaNodes:   []int32{1, 2},
		ISRNodes:       []int32{1},
	})
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	reader := codec.NewReader(data)
	gotTopic, err := reader.ReadNullableString()
	if err != nil {
		t.Fatalf("ReadNullableString() error = %v", err)
	}
	if gotTopic == nil || *gotTopic != topic {
		t.Fatalf("topic = %v, want %q", gotTopic, topic)
	}
	values := []int32{3, -1, -1, -1}
	for _, want := range values {
		got, err := reader.ReadInt32()
		if err != nil {
			t.Fatalf("ReadInt32() error = %v", err)
		}
		if got != want {
			t.Fatalf("int32 = %d, want %d", got, want)
		}
	}
	replicas, err := reader.ReadInt32Array()
	if err != nil {
		t.Fatalf("ReadInt32Array(replicas) error = %v", err)
	}
	if !reflect.DeepEqual(replicas, []int32{1, 2}) {
		t.Fatalf("replicas = %v", replicas)
	}
	isr, err := reader.ReadInt32Array()
	if err != nil {
		t.Fatalf("ReadInt32Array(isr) error = %v", err)
	}
	if !reflect.DeepEqual(isr, []int32{1}) {
		t.Fatalf("isr = %v", isr)
	}
}

func TestDecodeTopicAdminResponseBody(t *testing.T) {
	topic := "orders"
	writer := codec.NewWriter()
	writer.WriteNullableString(&topic)
	writer.WriteArrayLen(2)
	writer.WriteInt32(0)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteInt32(7)
	writer.WriteInt32(1)
	writer.WriteInt16(protocol.ErrorCodeLeaderNotAvailable.Code())
	writer.WriteInt32(8)
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	got, err := codec.DecodeTopicAdminResponseBody(codec.NewReader(data))
	if err != nil {
		t.Fatalf("DecodeTopicAdminResponseBody() error = %v", err)
	}
	if got.Topic == nil || *got.Topic != topic {
		t.Fatalf("topic = %v", got.Topic)
	}
	want := []message.TopicAdminPartitionResponse{
		{Partition: 0, ErrorCode: protocol.ErrorCodeNone, LeaderEpoch: 7},
		{Partition: 1, ErrorCode: protocol.ErrorCodeLeaderNotAvailable, LeaderEpoch: 8},
	}
	if !reflect.DeepEqual(got.Partitions, want) {
		t.Fatalf("partitions = %+v, want %+v", got.Partitions, want)
	}
}

func TestDefaultRegistryCoversTopicAdminCodec(t *testing.T) {
	topic := "orders"
	registry := codec.DefaultRegistry()
	encoded, err := registry.EncodeRequestBody(protocol.ApiKeyCreateTopic, protocol.DefaultAPIVersion, message.TopicAdminRequestBody{
		Topic:          &topic,
		PartitionCount: 2,
		Partition:      -1,
		LeaderID:       -1,
		LeaderEpoch:    -1,
	})
	if err != nil {
		t.Fatalf("EncodeRequestBody() error = %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("EncodeRequestBody() returned empty bytes")
	}
	writer := codec.NewWriter()
	writer.WriteNullableString(&topic)
	writer.WriteArrayLen(1)
	writer.WriteInt32(0)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteInt32(1)
	responseBytes, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	body, err := registry.DecodeResponseBody(protocol.ApiKeyCreateTopic, protocol.DefaultAPIVersion, responseBytes)
	if err != nil {
		t.Fatalf("DecodeResponseBody() error = %v", err)
	}
	if _, ok := body.(message.TopicAdminResponseBody); !ok {
		t.Fatalf("DecodeResponseBody() type = %T", body)
	}
}
