package message

const (
	// RecordBatchMagicV1 is the first Stellflow RecordBatch format version.
	RecordBatchMagicV1 int8 = 1
)

// RecordBatch is the wire-level batch format used by Produce and Fetch.
type RecordBatch struct {
	BaseOffsetDelta      int32
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC32C               int32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}

// NewRecordBatch creates a v1 batch for encoding.
func NewRecordBatch(records []Record) RecordBatch {
	return RecordBatch{
		Magic:           RecordBatchMagicV1,
		ProducerID:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    -1,
		Records:         records,
		LastOffsetDelta: int32(len(records) - 1),
	}
}

// Record is a single message inside a RecordBatch.
type Record struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

// RecordHeader is a key/value header attached to a Record.
type RecordHeader struct {
	Key   *string
	Value []byte
}
