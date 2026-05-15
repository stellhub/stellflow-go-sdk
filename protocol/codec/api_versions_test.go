package codec_test

import (
	"reflect"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

func TestEncodeAPIVersionsRequestBody(t *testing.T) {
	name := "stellflow-go-sdk"
	version := "0.0.1"

	writer := codec.NewWriter()
	codec.EncodeAPIVersionsRequestBody(writer, message.APIVersionsRequestBody{
		ClientSoftwareName:    &name,
		ClientSoftwareVersion: &version,
		SupportedFeatures:     []string{"observability.trace_context", "governance.multi_tenant"},
	})
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}

	reader := codec.NewReader(data)
	gotName, err := reader.ReadString()
	if err != nil {
		t.Fatalf("name error = %v", err)
	}
	gotVersion, err := reader.ReadString()
	if err != nil {
		t.Fatalf("version error = %v", err)
	}
	features, err := reader.ReadStringArray()
	if err != nil {
		t.Fatalf("features error = %v", err)
	}
	if gotName != name || gotVersion != version {
		t.Fatalf("name/version = %q/%q, want %q/%q", gotName, gotVersion, name, version)
	}
	if !reflect.DeepEqual(features, []string{"observability.trace_context", "governance.multi_tenant"}) {
		t.Fatalf("features = %v", features)
	}
}

func TestDecodeAPIVersionsResponseBody(t *testing.T) {
	brokerName := "stellflow-broker"
	brokerVersion := "0.1.0"
	writer := codec.NewWriter()
	writer.WriteArrayLen(2)
	writer.WriteInt16(protocol.ApiKeyAPIVersions.Code())
	writer.WriteInt16(0)
	writer.WriteInt16(0)
	writer.WriteInt16(protocol.ApiKeyMetadata.Code())
	writer.WriteInt16(0)
	writer.WriteInt16(0)
	writer.WriteNullableString(&brokerName)
	writer.WriteNullableString(&brokerVersion)
	writer.WriteStringArray([]string{"fetch.long_poll"})
	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}

	got, err := codec.DecodeAPIVersionsResponseBody(codec.NewReader(data))
	if err != nil {
		t.Fatalf("DecodeAPIVersionsResponseBody() error = %v", err)
	}
	want := message.APIVersionsResponseBody{
		APIVersions: []message.APIVersionRange{
			{APIKey: protocol.ApiKeyAPIVersions.Code(), MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.ApiKeyMetadata.Code(), MinVersion: 0, MaxVersion: 0},
		},
		BrokerSoftwareName:    &brokerName,
		BrokerSoftwareVersion: &brokerVersion,
		SupportedFeatures:     []string{"fetch.long_poll"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeAPIVersionsResponseBody() = %+v, want %+v", got, want)
	}
}

func TestDefaultRegistryEncodesAndDecodesAPIVersions(t *testing.T) {
	name := "stellflow-go-sdk"
	registry := codec.DefaultRegistry()

	encoded, err := registry.EncodeRequestBody(protocol.ApiKeyAPIVersions, protocol.DefaultAPIVersion, message.APIVersionsRequestBody{
		ClientSoftwareName: &name,
		SupportedFeatures:  []string{},
	})
	if err != nil {
		t.Fatalf("EncodeRequestBody() error = %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("EncodeRequestBody() returned empty bytes")
	}

	brokerName := "stellflow-broker"
	writer := codec.NewWriter()
	writer.WriteArrayLen(1)
	writer.WriteInt16(protocol.ApiKeyAPIVersions.Code())
	writer.WriteInt16(0)
	writer.WriteInt16(0)
	writer.WriteNullableString(&brokerName)
	writer.WriteNullableString(nil)
	writer.WriteStringArray(nil)
	responseBytes, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	body, err := registry.DecodeResponseBody(protocol.ApiKeyAPIVersions, protocol.DefaultAPIVersion, responseBytes)
	if err != nil {
		t.Fatalf("DecodeResponseBody() error = %v", err)
	}
	if _, ok := body.(message.APIVersionsResponseBody); !ok {
		t.Fatalf("DecodeResponseBody() type = %T", body)
	}
}
