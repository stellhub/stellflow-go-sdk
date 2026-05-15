package codec

import (
	"encoding/binary"
	"math"
	"unicode/utf8"

	"github.com/stellhub/stellflow-go-sdk/protocol"
)

// Writer writes Stellflow protocol primitives in big-endian order.
type Writer struct {
	bytes []byte
	err   error
}

// NewWriter creates an empty protocol writer.
func NewWriter() *Writer {
	return &Writer{}
}

// Bytes returns the accumulated bytes and the first encoding error.
func (w *Writer) Bytes() ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	out := make([]byte, len(w.bytes))
	copy(out, w.bytes)
	return out, nil
}

// WriteInt8 writes an int8 value.
func (w *Writer) WriteInt8(value int8) {
	w.bytes = append(w.bytes, byte(value))
}

// WriteInt16 writes an int16 value.
func (w *Writer) WriteInt16(value int16) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(value))
	w.bytes = append(w.bytes, buf[:]...)
}

// WriteInt32 writes an int32 value.
func (w *Writer) WriteInt32(value int32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	w.bytes = append(w.bytes, buf[:]...)
}

// WriteInt64 writes an int64 value.
func (w *Writer) WriteInt64(value int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	w.bytes = append(w.bytes, buf[:]...)
}

// WriteBool writes a bool value.
func (w *Writer) WriteBool(value bool) {
	if value {
		w.WriteInt8(1)
		return
	}
	w.WriteInt8(0)
}

// WriteString writes a non-null UTF-8 string.
func (w *Writer) WriteString(value string) {
	w.WriteNullableString(&value)
}

// WriteNullableString writes a nullable UTF-8 string.
func (w *Writer) WriteNullableString(value *string) {
	if value == nil {
		w.WriteInt16(-1)
		return
	}
	if !utf8.ValidString(*value) {
		w.setErr("string must be valid UTF-8")
		return
	}
	if len(*value) > math.MaxInt16 {
		w.setErr("string length exceeds int16 max")
		return
	}
	w.WriteInt16(int16(len(*value)))
	w.WriteRawBytes([]byte(*value))
}

// WriteBytes writes nullable bytes with an int32 length prefix.
func (w *Writer) WriteBytes(value []byte) {
	if value == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(value)))
	w.WriteRawBytes(value)
}

// WriteInt64Array writes an array of int64 values.
func (w *Writer) WriteInt64Array(values []int64) {
	w.WriteArrayLen(len(values))
	for _, value := range values {
		w.WriteInt64(value)
	}
}

// WriteRawBytes writes bytes without a length prefix.
func (w *Writer) WriteRawBytes(value []byte) {
	w.bytes = append(w.bytes, value...)
}

// WriteStringArray writes an array of non-null strings.
func (w *Writer) WriteStringArray(values []string) {
	w.WriteArrayLen(len(values))
	for _, value := range values {
		w.WriteString(value)
	}
}

// WriteInt32Array writes an array of int32 values.
func (w *Writer) WriteInt32Array(values []int32) {
	w.WriteArrayLen(len(values))
	for _, value := range values {
		w.WriteInt32(value)
	}
}

// WriteArrayLen writes a protocol array length.
func (w *Writer) WriteArrayLen(length int) {
	if length < 0 {
		w.setErr("array length must not be negative")
		return
	}
	if length > math.MaxInt32 {
		w.setErr("array length exceeds int32 max")
		return
	}
	w.WriteInt32(int32(length))
}

func (w *Writer) setErr(message string) {
	if w.err == nil {
		w.err = &protocol.EncodingError{Message: message}
	}
}

// Reader reads Stellflow protocol primitives in big-endian order.
type Reader struct {
	bytes []byte
	pos   int
}

// NewReader creates a protocol reader over bytes.
func NewReader(data []byte) *Reader {
	return &Reader{bytes: data}
}

// Remaining returns unread bytes.
func (r *Reader) Remaining() int {
	return len(r.bytes) - r.pos
}

// ReadInt8 reads an int8 value.
func (r *Reader) ReadInt8() (int8, error) {
	if err := r.require(1); err != nil {
		return 0, err
	}
	value := int8(r.bytes[r.pos])
	r.pos++
	return value, nil
}

// ReadInt16 reads an int16 value.
func (r *Reader) ReadInt16() (int16, error) {
	if err := r.require(2); err != nil {
		return 0, err
	}
	value := int16(binary.BigEndian.Uint16(r.bytes[r.pos:]))
	r.pos += 2
	return value, nil
}

// ReadInt32 reads an int32 value.
func (r *Reader) ReadInt32() (int32, error) {
	if err := r.require(4); err != nil {
		return 0, err
	}
	value := int32(binary.BigEndian.Uint32(r.bytes[r.pos:]))
	r.pos += 4
	return value, nil
}

// ReadInt64 reads an int64 value.
func (r *Reader) ReadInt64() (int64, error) {
	if err := r.require(8); err != nil {
		return 0, err
	}
	value := int64(binary.BigEndian.Uint64(r.bytes[r.pos:]))
	r.pos += 8
	return value, nil
}

// ReadBool reads a bool value.
func (r *Reader) ReadBool() (bool, error) {
	value, err := r.ReadInt8()
	if err != nil {
		return false, err
	}
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, &protocol.DecodingError{Message: "invalid boolean value"}
	}
}

// ReadString reads a non-null UTF-8 string.
func (r *Reader) ReadString() (string, error) {
	value, err := r.ReadNullableString()
	if err != nil {
		return "", err
	}
	if value == nil {
		return "", &protocol.DecodingError{Message: "string must not be null"}
	}
	return *value, nil
}

// ReadNullableString reads a nullable UTF-8 string.
func (r *Reader) ReadNullableString() (*string, error) {
	length, err := r.ReadInt16()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if err := r.require(int(length)); err != nil {
		return nil, err
	}
	raw := r.bytes[r.pos : r.pos+int(length)]
	r.pos += int(length)
	if !utf8.Valid(raw) {
		return nil, &protocol.DecodingError{Message: "string bytes must be valid UTF-8"}
	}
	value := string(raw)
	return &value, nil
}

// ReadBytes reads nullable bytes with an int32 length prefix.
func (r *Reader) ReadBytes() ([]byte, error) {
	length, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if err := r.require(int(length)); err != nil {
		return nil, err
	}
	value := make([]byte, int(length))
	copy(value, r.bytes[r.pos:r.pos+int(length)])
	r.pos += int(length)
	return value, nil
}

// ReadRawBytes reads bytes without a length prefix.
func (r *Reader) ReadRawBytes(length int) ([]byte, error) {
	if err := r.require(length); err != nil {
		return nil, err
	}
	value := make([]byte, length)
	copy(value, r.bytes[r.pos:r.pos+length])
	r.pos += length
	return value, nil
}

// ReadStringArray reads an array of non-null strings.
func (r *Reader) ReadStringArray() ([]string, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, length)
	for range length {
		value, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

// ReadInt32Array reads an array of int32 values.
func (r *Reader) ReadInt32Array() ([]int32, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	values := make([]int32, 0, length)
	for range length {
		value, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

// ReadInt64Array reads an array of int64 values.
func (r *Reader) ReadInt64Array() ([]int64, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	values := make([]int64, 0, length)
	for range length {
		value, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

// ReadArrayLen reads a protocol array length.
func (r *Reader) ReadArrayLen() (int, error) {
	length, err := r.ReadInt32()
	if err != nil {
		return 0, err
	}
	if length < 0 {
		return 0, &protocol.DecodingError{Message: "array length must not be negative"}
	}
	return int(length), nil
}

func (r *Reader) require(length int) error {
	if length < 0 || r.Remaining() < length {
		return &protocol.DecodingError{Message: "not enough protocol bytes"}
	}
	return nil
}
