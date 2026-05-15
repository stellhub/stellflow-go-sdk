package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeJoinGroupRequestBody encodes apiKey=9, apiVersion=0 request body.
func EncodeJoinGroupRequestBody(w *Writer, body message.JoinGroupRequestBody) {
	w.WriteNullableString(body.GroupID)
	w.WriteNullableString(body.MemberID)
	w.WriteInt32(body.SessionTimeoutMs)
}

func encodeJoinGroupRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.JoinGroupRequestBody)
	if !ok {
		return fmt.Errorf("expected message.JoinGroupRequestBody, got %T", body)
	}
	EncodeJoinGroupRequestBody(w, typedBody)
	return nil
}

// DecodeJoinGroupResponseBody decodes apiKey=9, apiVersion=0 response body.
func DecodeJoinGroupResponseBody(r *Reader) (message.JoinGroupResponseBody, error) {
	errorCode, err := r.ReadInt16()
	if err != nil {
		return message.JoinGroupResponseBody{}, err
	}
	generationID, err := r.ReadInt32()
	if err != nil {
		return message.JoinGroupResponseBody{}, err
	}
	memberID, err := r.ReadNullableString()
	if err != nil {
		return message.JoinGroupResponseBody{}, err
	}
	leaderID, err := r.ReadNullableString()
	if err != nil {
		return message.JoinGroupResponseBody{}, err
	}
	return message.JoinGroupResponseBody{
		ErrorCode:    protocol.ErrorCodeFromCode(errorCode),
		GenerationID: generationID,
		MemberID:     memberID,
		LeaderID:     leaderID,
	}, nil
}

func decodeJoinGroupResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeJoinGroupResponseBody(r)
}

// EncodeSyncGroupRequestBody encodes apiKey=10, apiVersion=0 request body.
func EncodeSyncGroupRequestBody(w *Writer, body message.SyncGroupRequestBody) {
	w.WriteNullableString(body.GroupID)
	w.WriteInt32(body.GenerationID)
	w.WriteNullableString(body.MemberID)
}

func encodeSyncGroupRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.SyncGroupRequestBody)
	if !ok {
		return fmt.Errorf("expected message.SyncGroupRequestBody, got %T", body)
	}
	EncodeSyncGroupRequestBody(w, typedBody)
	return nil
}

// DecodeSyncGroupResponseBody decodes apiKey=10, apiVersion=0 response body.
func DecodeSyncGroupResponseBody(r *Reader) (message.SyncGroupResponseBody, error) {
	errorCode, err := r.ReadInt16()
	if err != nil {
		return message.SyncGroupResponseBody{}, err
	}
	return message.SyncGroupResponseBody{ErrorCode: protocol.ErrorCodeFromCode(errorCode)}, nil
}

func decodeSyncGroupResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeSyncGroupResponseBody(r)
}

// EncodeHeartbeatRequestBody encodes apiKey=8, apiVersion=0 request body.
func EncodeHeartbeatRequestBody(w *Writer, body message.HeartbeatRequestBody) {
	w.WriteNullableString(body.GroupID)
	w.WriteInt32(body.GenerationID)
	w.WriteNullableString(body.MemberID)
}

func encodeHeartbeatRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.HeartbeatRequestBody)
	if !ok {
		return fmt.Errorf("expected message.HeartbeatRequestBody, got %T", body)
	}
	EncodeHeartbeatRequestBody(w, typedBody)
	return nil
}

// DecodeHeartbeatResponseBody decodes apiKey=8, apiVersion=0 response body.
func DecodeHeartbeatResponseBody(r *Reader) (message.HeartbeatResponseBody, error) {
	errorCode, err := r.ReadInt16()
	if err != nil {
		return message.HeartbeatResponseBody{}, err
	}
	return message.HeartbeatResponseBody{ErrorCode: protocol.ErrorCodeFromCode(errorCode)}, nil
}

func decodeHeartbeatResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeHeartbeatResponseBody(r)
}
