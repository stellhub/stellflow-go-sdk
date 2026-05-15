package codec_test

import (
	"bytes"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
)

func TestWriterUsesBigEndianEncoding(t *testing.T) {
	writer := codec.NewWriter()
	writer.WriteInt16(0x0102)
	writer.WriteInt32(0x01020304)
	writer.WriteInt64(0x0102030405060708)

	got, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	want := []byte{
		0x01, 0x02,
		0x01, 0x02, 0x03, 0x04,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded bytes = %v, want %v", got, want)
	}
}

func TestReaderReadsNullableStringAndArray(t *testing.T) {
	alpha := "alpha"
	writer := codec.NewWriter()
	writer.WriteNullableString(&alpha)
	writer.WriteNullableString(nil)
	writer.WriteStringArray([]string{"feature.a", "feature.b"})

	data, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}

	reader := codec.NewReader(data)
	gotAlpha, err := reader.ReadNullableString()
	if err != nil {
		t.Fatalf("ReadNullableString() error = %v", err)
	}
	if gotAlpha == nil || *gotAlpha != alpha {
		t.Fatalf("first nullable string = %v, want %q", gotAlpha, alpha)
	}
	gotNil, err := reader.ReadNullableString()
	if err != nil {
		t.Fatalf("ReadNullableString() nil error = %v", err)
	}
	if gotNil != nil {
		t.Fatalf("second nullable string = %q, want nil", *gotNil)
	}
	features, err := reader.ReadStringArray()
	if err != nil {
		t.Fatalf("ReadStringArray() error = %v", err)
	}
	if !bytes.Equal([]byte(features[0]+"|"+features[1]), []byte("feature.a|feature.b")) {
		t.Fatalf("features = %v", features)
	}
	if reader.Remaining() != 0 {
		t.Fatalf("remaining = %d, want 0", reader.Remaining())
	}
}

func TestReaderRejectsInvalidBool(t *testing.T) {
	reader := codec.NewReader([]byte{2})
	if _, err := reader.ReadBool(); err == nil {
		t.Fatal("ReadBool() error = nil, want invalid bool error")
	}
}
