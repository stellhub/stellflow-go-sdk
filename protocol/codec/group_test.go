package codec_test

import (
	"reflect"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

const (
	goldenOffsetFetchRequestHex  = "000c6f72646572732d67726f75700000000100066f72646572730000000100000000"
	goldenOffsetFetchResponseHex = "0000000100066f72646572730000000100000000000000000000002b00066d616e75616c0000"
	goldenJoinGroupRequestHex    = "000c6f72646572732d67726f757000086d656d6265722d6100007530"
	goldenJoinGroupResponseHex   = "00000000000300086d656d6265722d6100086d656d6265722d61"
	goldenSyncGroupRequestHex    = "000c6f72646572732d67726f75700000000300086d656d6265722d61"
	goldenHeartbeatRequestHex    = "000c6f72646572732d67726f75700000000300086d656d6265722d61"
)

func TestOffsetFetchRequestGoldenBytes(t *testing.T) {
	writer := codec.NewWriter()
	codec.EncodeOffsetFetchRequestBody(writer, sampleOffsetFetchRequestBody())
	got, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	want := mustDecodeHex(t, goldenOffsetFetchRequestHex)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OffsetFetchRequest bytes = %x, want %x", got, want)
	}
}

func TestDecodeOffsetFetchResponseGoldenBytes(t *testing.T) {
	got, err := codec.DecodeOffsetFetchResponseBody(codec.NewReader(mustDecodeHex(t, goldenOffsetFetchResponseHex)))
	if err != nil {
		t.Fatalf("DecodeOffsetFetchResponseBody() error = %v", err)
	}
	topic := "orders"
	metadata := "manual"
	want := message.OffsetFetchResponseBody{Topics: []message.OffsetFetchTopicResponse{
		{
			Topic: &topic,
			Partitions: []message.OffsetFetchPartitionResponse{
				{Partition: 0, Offset: 43, Metadata: &metadata, ErrorCode: protocol.ErrorCodeNone},
			},
		},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeOffsetFetchResponseBody() = %+v, want %+v", got, want)
	}
}

func TestJoinGroupGoldenBytes(t *testing.T) {
	writer := codec.NewWriter()
	codec.EncodeJoinGroupRequestBody(writer, sampleJoinGroupRequestBody())
	got, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	want := mustDecodeHex(t, goldenJoinGroupRequestHex)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JoinGroupRequest bytes = %x, want %x", got, want)
	}

	decoded, err := codec.DecodeJoinGroupResponseBody(codec.NewReader(mustDecodeHex(t, goldenJoinGroupResponseHex)))
	if err != nil {
		t.Fatalf("DecodeJoinGroupResponseBody() error = %v", err)
	}
	memberID := "member-a"
	leaderID := "member-a"
	wantResponse := message.JoinGroupResponseBody{
		ErrorCode:    protocol.ErrorCodeNone,
		GenerationID: 3,
		MemberID:     &memberID,
		LeaderID:     &leaderID,
	}
	if !reflect.DeepEqual(decoded, wantResponse) {
		t.Fatalf("DecodeJoinGroupResponseBody() = %+v, want %+v", decoded, wantResponse)
	}
}

func TestSyncGroupAndHeartbeatGoldenBytes(t *testing.T) {
	syncWriter := codec.NewWriter()
	codec.EncodeSyncGroupRequestBody(syncWriter, sampleSyncGroupRequestBody())
	syncBytes, err := syncWriter.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	if !reflect.DeepEqual(syncBytes, mustDecodeHex(t, goldenSyncGroupRequestHex)) {
		t.Fatalf("SyncGroupRequest bytes = %x", syncBytes)
	}

	heartbeatWriter := codec.NewWriter()
	codec.EncodeHeartbeatRequestBody(heartbeatWriter, sampleHeartbeatRequestBody())
	heartbeatBytes, err := heartbeatWriter.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	if !reflect.DeepEqual(heartbeatBytes, mustDecodeHex(t, goldenHeartbeatRequestHex)) {
		t.Fatalf("HeartbeatRequest bytes = %x", heartbeatBytes)
	}

	responseWriter := codec.NewWriter()
	responseWriter.WriteInt16(protocol.ErrorCodeNone.Code())
	responseBytes, err := responseWriter.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	if _, err := codec.DecodeSyncGroupResponseBody(codec.NewReader(responseBytes)); err != nil {
		t.Fatalf("DecodeSyncGroupResponseBody() error = %v", err)
	}
	if _, err := codec.DecodeHeartbeatResponseBody(codec.NewReader(responseBytes)); err != nil {
		t.Fatalf("DecodeHeartbeatResponseBody() error = %v", err)
	}
}

func TestDefaultRegistryCoversGroupCodecs(t *testing.T) {
	registry := codec.DefaultRegistry()
	cases := []struct {
		name     string
		apiKey   protocol.ApiKey
		request  codec.RequestBody
		response []byte
		wantType any
	}{
		{"offset_fetch", protocol.ApiKeyOffsetFetch, sampleOffsetFetchRequestBody(), mustDecodeHex(t, goldenOffsetFetchResponseHex), message.OffsetFetchResponseBody{}},
		{"join_group", protocol.ApiKeyJoinGroup, sampleJoinGroupRequestBody(), mustDecodeHex(t, goldenJoinGroupResponseHex), message.JoinGroupResponseBody{}},
		{"sync_group", protocol.ApiKeySyncGroup, sampleSyncGroupRequestBody(), []byte{0, 0}, message.SyncGroupResponseBody{}},
		{"heartbeat", protocol.ApiKeyHeartbeat, sampleHeartbeatRequestBody(), []byte{0, 0}, message.HeartbeatResponseBody{}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := registry.EncodeRequestBody(tt.apiKey, protocol.DefaultAPIVersion, tt.request)
			if err != nil {
				t.Fatalf("EncodeRequestBody() error = %v", err)
			}
			if len(encoded) == 0 {
				t.Fatal("EncodeRequestBody() returned empty bytes")
			}
			body, err := registry.DecodeResponseBody(tt.apiKey, protocol.DefaultAPIVersion, tt.response)
			if err != nil {
				t.Fatalf("DecodeResponseBody() error = %v", err)
			}
			if reflect.TypeOf(body) != reflect.TypeOf(tt.wantType) {
				t.Fatalf("DecodeResponseBody() type = %T, want %T", body, tt.wantType)
			}
		})
	}
}

func sampleOffsetFetchRequestBody() message.OffsetFetchRequestBody {
	groupID := "orders-group"
	return message.OffsetFetchRequestBody{
		GroupID: &groupID,
		Topics: []message.OffsetFetchTopic{
			{Topic: "orders", Partitions: []message.OffsetFetchPartition{{Partition: 0}}},
		},
	}
}

func sampleJoinGroupRequestBody() message.JoinGroupRequestBody {
	groupID := "orders-group"
	memberID := "member-a"
	return message.JoinGroupRequestBody{GroupID: &groupID, MemberID: &memberID, SessionTimeoutMs: 30000}
}

func sampleSyncGroupRequestBody() message.SyncGroupRequestBody {
	groupID := "orders-group"
	memberID := "member-a"
	return message.SyncGroupRequestBody{GroupID: &groupID, GenerationID: 3, MemberID: &memberID}
}

func sampleHeartbeatRequestBody() message.HeartbeatRequestBody {
	groupID := "orders-group"
	memberID := "member-a"
	return message.HeartbeatRequestBody{GroupID: &groupID, GenerationID: 3, MemberID: &memberID}
}
