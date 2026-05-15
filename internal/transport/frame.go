package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
)

const DefaultMaxFrameLength = 64 * 1024 * 1024

// EncodeRequestFrame encodes frameLength + request header + body.
func EncodeRequestFrame(header protocol.RequestHeader, body []byte) ([]byte, error) {
	headerWriter := codec.NewWriter()
	codec.EncodeRequestHeader(headerWriter, header)
	headerBytes, err := headerWriter.Bytes()
	if err != nil {
		return nil, err
	}
	frameLength := len(headerBytes) + len(body)
	if frameLength > math.MaxInt32 {
		return nil, fmt.Errorf("frame too large: %d", frameLength)
	}
	frame := make([]byte, 4, 4+frameLength)
	binary.BigEndian.PutUint32(frame[:4], uint32(frameLength))
	frame = append(frame, headerBytes...)
	frame = append(frame, body...)
	return frame, nil
}

// ReadFrame reads one length-prefixed response frame.
func ReadFrame(reader io.Reader, maxFrameLength int) ([]byte, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return nil, err
	}
	frameLength := int(binary.BigEndian.Uint32(prefix[:]))
	if frameLength < 0 || frameLength > maxFrameLength {
		return nil, fmt.Errorf("invalid frame length: %d", frameLength)
	}
	frame := make([]byte, frameLength)
	if _, err := io.ReadFull(reader, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

// DecodeResponseFrame decodes response header and returns remaining body bytes.
func DecodeResponseFrame(frame []byte) (protocol.ResponseHeader, []byte, error) {
	reader := codec.NewReader(frame)
	header, err := codec.DecodeResponseHeader(reader)
	if err != nil {
		return protocol.ResponseHeader{}, nil, err
	}
	body, err := reader.ReadRawBytes(reader.Remaining())
	if err != nil {
		return protocol.ResponseHeader{}, nil, err
	}
	return header, body, nil
}
