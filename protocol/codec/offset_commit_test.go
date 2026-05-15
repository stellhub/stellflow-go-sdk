package codec_test

import (
	"reflect"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

func TestOffsetCommitCodec(t *testing.T) {
	groupID := "orders-group"
	metadata := "manual"
	writer := codec.NewWriter()
	codec.EncodeOffsetCommitRequestBody(writer, message.OffsetCommitRequestBody{
		GroupID: &groupID,
		Topics: []message.OffsetCommitTopic{
			{
				Topic: "orders",
				Partitions: []message.OffsetCommitPartition{
					{Partition: 0, Offset: 43, Metadata: &metadata},
				},
			},
		},
	})
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	if len(data) == 0 {
		t.Fatal("offset commit request is empty")
	}

	topic := "orders"
	writer = codec.NewWriter()
	writer.WriteArrayLen(1)
	writer.WriteNullableString(&topic)
	writer.WriteArrayLen(1)
	writer.WriteInt32(0)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	responseBytes, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	got, err := codec.DecodeOffsetCommitResponseBody(codec.NewReader(responseBytes))
	if err != nil {
		t.Fatalf("DecodeOffsetCommitResponseBody() error = %v", err)
	}
	want := message.OffsetCommitResponseBody{
		Topics: []message.OffsetCommitTopicResponse{
			{Topic: &topic, Partitions: []message.OffsetCommitPartitionResponse{{Partition: 0, ErrorCode: protocol.ErrorCodeNone}}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeOffsetCommitResponseBody() = %+v, want %+v", got, want)
	}
}
