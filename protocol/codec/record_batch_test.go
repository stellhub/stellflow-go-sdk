package codec_test

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

const goldenRecordBatchHex = "0000000000000058ffffffff019fca885500000000000000000000000003e800000000000003e8ffffffffffffffffffffffffffff000000010000002300000000000000000000000000000000016b0000000176000000010001680000000178"

func TestRecordBatchGoldenBytes(t *testing.T) {
	batch := sampleRecordBatch()

	got, err := codec.EncodeRecordBatch(batch)
	if err != nil {
		t.Fatalf("EncodeRecordBatch() error = %v", err)
	}
	want := mustDecodeHex(t, goldenRecordBatchHex)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("EncodeRecordBatch() = %x, want %x", got, want)
	}

	decoded, err := codec.DecodeRecordBatch(got)
	if err != nil {
		t.Fatalf("DecodeRecordBatch() error = %v", err)
	}
	if decoded.CRC32C == 0 {
		t.Fatal("DecodeRecordBatch() CRC32C = 0, want calculated checksum")
	}
	decoded.CRC32C = 0
	decoded.BatchLength = 0
	if !reflect.DeepEqual(decoded, batch) {
		t.Fatalf("decoded batch = %+v, want %+v", decoded, batch)
	}
}

func TestRecordBatchSetRoundTrip(t *testing.T) {
	batch := sampleRecordBatch()
	encoded, err := codec.EncodeRecordBatchSet([]message.RecordBatch{batch, batch})
	if err != nil {
		t.Fatalf("EncodeRecordBatchSet() error = %v", err)
	}
	decoded, err := codec.DecodeRecordBatchSet(encoded)
	if err != nil {
		t.Fatalf("DecodeRecordBatchSet() error = %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded batch count = %d, want 2", len(decoded))
	}
	for index := range decoded {
		decoded[index].CRC32C = 0
		decoded[index].BatchLength = 0
		if !reflect.DeepEqual(decoded[index], batch) {
			t.Fatalf("decoded[%d] = %+v, want %+v", index, decoded[index], batch)
		}
	}
}

func TestRecordBatchRejectsInvalidCRC(t *testing.T) {
	data := mustDecodeHex(t, goldenRecordBatchHex)
	data[len(data)-1] ^= 0xff
	if _, err := codec.DecodeRecordBatch(data); err == nil {
		t.Fatal("DecodeRecordBatch() error = nil, want CRC error")
	}
}

func sampleRecordBatch() message.RecordBatch {
	headerKey := "h"
	return message.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                message.RecordBatchMagicV1,
		Attributes:           0,
		LastOffsetDelta:      0,
		BaseTimestamp:        1000,
		MaxTimestamp:         1000,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		BaseSequence:         -1,
		Records: []message.Record{
			{
				Attributes:     0,
				TimestampDelta: 0,
				OffsetDelta:    0,
				Key:            []byte("k"),
				Value:          []byte("v"),
				Headers: []message.RecordHeader{
					{Key: &headerKey, Value: []byte("x")},
				},
			},
		},
	}
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("DecodeString(%q) error = %v", value, err)
	}
	return decoded
}
