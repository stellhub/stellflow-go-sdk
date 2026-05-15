package codec_test

import (
	"reflect"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
)

func TestEncodeRequestHeaderMatchesProtocolOrder(t *testing.T) {
	clientID := "go-client"
	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	spanID := "00f067aa0ba902b7"
	tenantID := "tenant-order"
	quotaKey := "quota-order-write"
	authContextID := "authctx-prod-a"

	writer := codec.NewWriter()
	codec.EncodeRequestHeader(writer, protocol.RequestHeader{
		APIKey:        protocol.ApiKeyProduce,
		APIVersion:    protocol.DefaultAPIVersion,
		HeaderVersion: protocol.DefaultHeaderVersion,
		CorrelationID: 10001,
		ClientID:      &clientID,
		TraceID:       &traceID,
		SpanID:        &spanID,
		TraceFlags:    1,
		TenantID:      &tenantID,
		QuotaKey:      &quotaKey,
		AuthContextID: &authContextID,
		TrafficClass:  0,
		TrafficTag:    nil,
		Flags:         0,
	})
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}

	reader := codec.NewReader(data)
	checkInt16(t, reader, protocol.ApiKeyProduce.Code())
	checkInt16(t, reader, protocol.DefaultAPIVersion)
	checkInt16(t, reader, protocol.DefaultHeaderVersion)
	checkInt32(t, reader, 10001)
	checkString(t, reader, clientID)
	checkString(t, reader, traceID)
	checkString(t, reader, spanID)
	checkInt8(t, reader, 1)
	checkString(t, reader, tenantID)
	checkString(t, reader, quotaKey)
	checkString(t, reader, authContextID)
	checkInt8(t, reader, 0)
	if value, err := reader.ReadNullableString(); err != nil {
		t.Fatalf("trafficTag error = %v", err)
	} else if value != nil {
		t.Fatalf("trafficTag = %q, want nil", *value)
	}
	checkInt16(t, reader, 0)
	if reader.Remaining() != 0 {
		t.Fatalf("remaining = %d, want 0", reader.Remaining())
	}
}

func TestDecodeResponseHeader(t *testing.T) {
	writer := codec.NewWriter()
	writer.WriteInt32(10001)
	writer.WriteInt16(protocol.DefaultHeaderVersion)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteInt32(12)
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}

	got, err := codec.DecodeResponseHeader(codec.NewReader(data))
	if err != nil {
		t.Fatalf("DecodeResponseHeader() error = %v", err)
	}
	want := protocol.ResponseHeader{
		CorrelationID:  10001,
		HeaderVersion:  protocol.DefaultHeaderVersion,
		ErrorCode:      protocol.ErrorCodeNone,
		ThrottleTimeMs: 12,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeResponseHeader() = %+v, want %+v", got, want)
	}
}

func checkInt8(t *testing.T, reader *codec.Reader, want int8) {
	t.Helper()
	got, err := reader.ReadInt8()
	if err != nil {
		t.Fatalf("ReadInt8() error = %v", err)
	}
	if got != want {
		t.Fatalf("ReadInt8() = %d, want %d", got, want)
	}
}

func checkInt16(t *testing.T, reader *codec.Reader, want int16) {
	t.Helper()
	got, err := reader.ReadInt16()
	if err != nil {
		t.Fatalf("ReadInt16() error = %v", err)
	}
	if got != want {
		t.Fatalf("ReadInt16() = %d, want %d", got, want)
	}
}

func checkInt32(t *testing.T, reader *codec.Reader, want int32) {
	t.Helper()
	got, err := reader.ReadInt32()
	if err != nil {
		t.Fatalf("ReadInt32() error = %v", err)
	}
	if got != want {
		t.Fatalf("ReadInt32() = %d, want %d", got, want)
	}
}

func checkString(t *testing.T, reader *codec.Reader, want string) {
	t.Helper()
	got, err := reader.ReadString()
	if err != nil {
		t.Fatalf("ReadString() error = %v", err)
	}
	if got != want {
		t.Fatalf("ReadString() = %q, want %q", got, want)
	}
}
