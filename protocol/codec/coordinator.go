package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeFindCoordinatorRequestBody encodes apiKey=7, apiVersion=0 request body.
func EncodeFindCoordinatorRequestBody(w *Writer, body message.FindCoordinatorRequestBody) {
	w.WriteNullableString(body.Key)
	w.WriteInt8(body.KeyType)
}

func encodeFindCoordinatorRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.FindCoordinatorRequestBody)
	if !ok {
		return fmt.Errorf("expected message.FindCoordinatorRequestBody, got %T", body)
	}
	EncodeFindCoordinatorRequestBody(w, typedBody)
	return nil
}

// DecodeFindCoordinatorResponseBody decodes apiKey=7, apiVersion=0 response body.
func DecodeFindCoordinatorResponseBody(r *Reader) (message.FindCoordinatorResponseBody, error) {
	errorCode, err := r.ReadInt16()
	if err != nil {
		return message.FindCoordinatorResponseBody{}, err
	}
	nodeID, err := r.ReadInt32()
	if err != nil {
		return message.FindCoordinatorResponseBody{}, err
	}
	host, err := r.ReadNullableString()
	if err != nil {
		return message.FindCoordinatorResponseBody{}, err
	}
	port, err := r.ReadInt32()
	if err != nil {
		return message.FindCoordinatorResponseBody{}, err
	}
	return message.FindCoordinatorResponseBody{
		ErrorCode: protocol.ErrorCodeFromCode(errorCode),
		NodeID:    nodeID,
		Host:      host,
		Port:      port,
	}, nil
}

func decodeFindCoordinatorResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeFindCoordinatorResponseBody(r)
}
