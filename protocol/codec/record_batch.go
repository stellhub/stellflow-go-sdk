package codec

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

const (
	recordBatchPrefixBytes = 8
	recordBatchCRCOffset   = 17
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// EncodeRecordBatch encodes a single RecordBatch.
func EncodeRecordBatch(batch message.RecordBatch) ([]byte, error) {
	crcPayload := NewWriter()
	crcPayload.WriteInt16(batch.Attributes)
	crcPayload.WriteInt32(batch.LastOffsetDelta)
	crcPayload.WriteInt64(batch.BaseTimestamp)
	crcPayload.WriteInt64(batch.MaxTimestamp)
	crcPayload.WriteInt64(batch.ProducerID)
	crcPayload.WriteInt16(batch.ProducerEpoch)
	crcPayload.WriteInt32(batch.BaseSequence)
	crcPayload.WriteInt32(int32(len(batch.Records)))
	for _, record := range batch.Records {
		writeRecord(crcPayload, record)
	}
	crcPayloadBytes, err := crcPayload.Bytes()
	if err != nil {
		return nil, err
	}
	checksum := crc32.Checksum(crcPayloadBytes, crc32cTable)
	batchLength := int32(4 + 1 + 4 + len(crcPayloadBytes))
	magic := batch.Magic
	if magic == 0 {
		magic = message.RecordBatchMagicV1
	}

	writer := NewWriter()
	writer.WriteInt32(batch.BaseOffsetDelta)
	writer.WriteInt32(batchLength)
	writer.WriteInt32(batch.PartitionLeaderEpoch)
	writer.WriteInt8(magic)
	writer.WriteInt32(int32(checksum))
	writer.WriteRawBytes(crcPayloadBytes)
	return writer.Bytes()
}

// DecodeRecordBatch decodes and validates a single RecordBatch.
func DecodeRecordBatch(data []byte) (message.RecordBatch, error) {
	if len(data) < recordBatchCRCOffset {
		return message.RecordBatch{}, &protocol.DecodingError{Message: fmt.Sprintf("record batch is too short: %d", len(data))}
	}
	reader := NewReader(data)
	baseOffsetDelta, err := reader.ReadInt32()
	if err != nil {
		return message.RecordBatch{}, err
	}
	batchLength, err := reader.ReadInt32()
	if err != nil {
		return message.RecordBatch{}, err
	}
	if batchLength != int32(len(data)-recordBatchPrefixBytes) {
		return message.RecordBatch{}, &protocol.DecodingError{Message: "invalid record batch length"}
	}
	partitionLeaderEpoch, err := reader.ReadInt32()
	if err != nil {
		return message.RecordBatch{}, err
	}
	magic, err := reader.ReadInt8()
	if err != nil {
		return message.RecordBatch{}, err
	}
	expectedCRC32C, err := reader.ReadInt32()
	if err != nil {
		return message.RecordBatch{}, err
	}
	actualCRC32C := int32(crc32.Checksum(data[recordBatchCRCOffset:], crc32cTable))
	if expectedCRC32C != actualCRC32C {
		return message.RecordBatch{}, &protocol.DecodingError{Message: "invalid record batch crc32c"}
	}
	attributes, err := reader.ReadInt16()
	if err != nil {
		return message.RecordBatch{}, err
	}
	lastOffsetDelta, err := reader.ReadInt32()
	if err != nil {
		return message.RecordBatch{}, err
	}
	baseTimestamp, err := reader.ReadInt64()
	if err != nil {
		return message.RecordBatch{}, err
	}
	maxTimestamp, err := reader.ReadInt64()
	if err != nil {
		return message.RecordBatch{}, err
	}
	producerID, err := reader.ReadInt64()
	if err != nil {
		return message.RecordBatch{}, err
	}
	producerEpoch, err := reader.ReadInt16()
	if err != nil {
		return message.RecordBatch{}, err
	}
	baseSequence, err := reader.ReadInt32()
	if err != nil {
		return message.RecordBatch{}, err
	}
	recordCount, err := reader.ReadInt32()
	if err != nil {
		return message.RecordBatch{}, err
	}
	if recordCount < 0 {
		return message.RecordBatch{}, &protocol.DecodingError{Message: "record count must not be negative"}
	}
	records := make([]message.Record, 0, recordCount)
	for range recordCount {
		record, err := readRecord(reader)
		if err != nil {
			return message.RecordBatch{}, err
		}
		records = append(records, record)
	}
	if reader.Remaining() != 0 {
		return message.RecordBatch{}, &protocol.DecodingError{Message: "record batch has trailing bytes"}
	}
	return message.RecordBatch{
		BaseOffsetDelta:      baseOffsetDelta,
		BatchLength:          batchLength,
		PartitionLeaderEpoch: partitionLeaderEpoch,
		Magic:                magic,
		CRC32C:               expectedCRC32C,
		Attributes:           attributes,
		LastOffsetDelta:      lastOffsetDelta,
		BaseTimestamp:        baseTimestamp,
		MaxTimestamp:         maxTimestamp,
		ProducerID:           producerID,
		ProducerEpoch:        producerEpoch,
		BaseSequence:         baseSequence,
		Records:              records,
	}, nil
}

// EncodeRecordBatchSet encodes contiguous RecordBatch bytes.
func EncodeRecordBatchSet(batches []message.RecordBatch) ([]byte, error) {
	writer := NewWriter()
	for _, batch := range batches {
		data, err := EncodeRecordBatch(batch)
		if err != nil {
			return nil, err
		}
		writer.WriteRawBytes(data)
	}
	return writer.Bytes()
}

// DecodeRecordBatchSet decodes contiguous RecordBatch bytes.
func DecodeRecordBatchSet(data []byte) ([]message.RecordBatch, error) {
	var batches []message.RecordBatch
	for offset := 0; offset < len(data); {
		if len(data)-offset < recordBatchPrefixBytes {
			return nil, &protocol.DecodingError{Message: "truncated record batch set"}
		}
		batchLength := int32(binary.BigEndian.Uint32(data[offset+4:]))
		totalLength := int(recordBatchPrefixBytes + batchLength)
		if batchLength < 0 || len(data)-offset < totalLength {
			return nil, &protocol.DecodingError{Message: "invalid record batch length in batch set"}
		}
		batch, err := DecodeRecordBatch(data[offset : offset+totalLength])
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
		offset += totalLength
	}
	return batches, nil
}

func writeRecord(writer *Writer, record message.Record) {
	payload := NewWriter()
	payload.WriteInt8(record.Attributes)
	payload.WriteInt64(record.TimestampDelta)
	payload.WriteInt32(record.OffsetDelta)
	payload.WriteBytes(record.Key)
	payload.WriteBytes(record.Value)
	payload.WriteArrayLen(len(record.Headers))
	for _, header := range record.Headers {
		payload.WriteNullableString(header.Key)
		payload.WriteBytes(header.Value)
	}
	payloadBytes, err := payload.Bytes()
	if err != nil {
		writer.setErr(err.Error())
		return
	}
	writer.WriteInt32(int32(len(payloadBytes)))
	writer.WriteRawBytes(payloadBytes)
}

func readRecord(reader *Reader) (message.Record, error) {
	length, err := reader.ReadInt32()
	if err != nil {
		return message.Record{}, err
	}
	if length < 0 {
		return message.Record{}, &protocol.DecodingError{Message: "invalid record length"}
	}
	payloadBytes, err := reader.ReadRawBytes(int(length))
	if err != nil {
		return message.Record{}, err
	}
	payload := NewReader(payloadBytes)
	attributes, err := payload.ReadInt8()
	if err != nil {
		return message.Record{}, err
	}
	timestampDelta, err := payload.ReadInt64()
	if err != nil {
		return message.Record{}, err
	}
	offsetDelta, err := payload.ReadInt32()
	if err != nil {
		return message.Record{}, err
	}
	key, err := payload.ReadBytes()
	if err != nil {
		return message.Record{}, err
	}
	value, err := payload.ReadBytes()
	if err != nil {
		return message.Record{}, err
	}
	headerCount, err := payload.ReadArrayLen()
	if err != nil {
		return message.Record{}, err
	}
	headers := make([]message.RecordHeader, 0, headerCount)
	for range headerCount {
		headerKey, err := payload.ReadNullableString()
		if err != nil {
			return message.Record{}, err
		}
		headerValue, err := payload.ReadBytes()
		if err != nil {
			return message.Record{}, err
		}
		headers = append(headers, message.RecordHeader{Key: headerKey, Value: headerValue})
	}
	if payload.Remaining() != 0 {
		return message.Record{}, &protocol.DecodingError{Message: "record has trailing bytes"}
	}
	return message.Record{
		Attributes:     attributes,
		TimestampDelta: timestampDelta,
		OffsetDelta:    offsetDelta,
		Key:            key,
		Value:          value,
		Headers:        headers,
	}, nil
}
