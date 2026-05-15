package transport_test

import (
	"bytes"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
)

func TestEncodeRequestFrameAndReadFrame(t *testing.T) {
	clientID := "client-a"
	frame, err := transport.EncodeRequestFrame(protocol.RequestHeader{
		APIKey:        protocol.ApiKeyMetadata,
		APIVersion:    protocol.DefaultAPIVersion,
		HeaderVersion: protocol.DefaultHeaderVersion,
		CorrelationID: 7,
		ClientID:      &clientID,
	}, []byte{1, 2, 3})
	if err != nil {
		t.Fatalf("EncodeRequestFrame() error = %v", err)
	}
	got, err := transport.ReadFrame(bytes.NewReader(frame), transport.DefaultMaxFrameLength)
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	if len(got) != len(frame)-4 {
		t.Fatalf("frame payload length = %d, want %d", len(got), len(frame)-4)
	}
}

func TestDecodeResponseFrame(t *testing.T) {
	writer := codec.NewWriter()
	writer.WriteInt32(7)
	writer.WriteInt16(protocol.DefaultHeaderVersion)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteInt32(3)
	writer.WriteRawBytes([]byte{9, 8, 7})
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	header, body, err := transport.DecodeResponseFrame(data)
	if err != nil {
		t.Fatalf("DecodeResponseFrame() error = %v", err)
	}
	if header.CorrelationID != 7 || header.ThrottleTimeMs != 3 {
		t.Fatalf("header = %+v", header)
	}
	if !bytes.Equal(body, []byte{9, 8, 7}) {
		t.Fatalf("body = %v", body)
	}
}
