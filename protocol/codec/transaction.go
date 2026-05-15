package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeInitProducerIDRequestBody encodes apiKey=11, apiVersion=0 request body.
func EncodeInitProducerIDRequestBody(w *Writer, body message.InitProducerIDRequestBody) {
	w.WriteNullableString(body.TransactionalID)
}

func encodeInitProducerIDRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.InitProducerIDRequestBody)
	if !ok {
		return fmt.Errorf("expected message.InitProducerIDRequestBody, got %T", body)
	}
	EncodeInitProducerIDRequestBody(w, typedBody)
	return nil
}

// DecodeInitProducerIDResponseBody decodes apiKey=11, apiVersion=0 response body.
func DecodeInitProducerIDResponseBody(r *Reader) (message.InitProducerIDResponseBody, error) {
	errorCode, err := r.ReadInt16()
	if err != nil {
		return message.InitProducerIDResponseBody{}, err
	}
	producerID, err := r.ReadInt64()
	if err != nil {
		return message.InitProducerIDResponseBody{}, err
	}
	producerEpoch, err := r.ReadInt16()
	if err != nil {
		return message.InitProducerIDResponseBody{}, err
	}
	return message.InitProducerIDResponseBody{
		ErrorCode:     protocol.ErrorCodeFromCode(errorCode),
		ProducerID:    producerID,
		ProducerEpoch: producerEpoch,
	}, nil
}

func decodeInitProducerIDResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeInitProducerIDResponseBody(r)
}

// EncodeTransactionRequestBody encodes apiKey=12/13, apiVersion=0 request body.
func EncodeTransactionRequestBody(w *Writer, apiKey protocol.ApiKey, body message.TransactionRequestBody) {
	w.WriteNullableString(body.TransactionalID)
	w.WriteInt64(body.ProducerID)
	w.WriteInt16(body.ProducerEpoch)
	if apiKey == protocol.ApiKeyEndTxn {
		w.WriteBool(body.Commit)
	}
}

func encodeBeginTxnRequestBodyAny(w *Writer, body RequestBody) error {
	return encodeTransactionRequestBodyAny(w, protocol.ApiKeyBeginTxn, body)
}

func encodeEndTxnRequestBodyAny(w *Writer, body RequestBody) error {
	return encodeTransactionRequestBodyAny(w, protocol.ApiKeyEndTxn, body)
}

func encodeTransactionRequestBodyAny(w *Writer, apiKey protocol.ApiKey, body RequestBody) error {
	typedBody, ok := body.(message.TransactionRequestBody)
	if !ok {
		return fmt.Errorf("expected message.TransactionRequestBody, got %T", body)
	}
	EncodeTransactionRequestBody(w, apiKey, typedBody)
	return nil
}

// DecodeTransactionResponseBody decodes apiKey=12/13, apiVersion=0 response body.
func DecodeTransactionResponseBody(r *Reader) (message.TransactionResponseBody, error) {
	errorCode, err := r.ReadInt16()
	if err != nil {
		return message.TransactionResponseBody{}, err
	}
	producerID, err := r.ReadInt64()
	if err != nil {
		return message.TransactionResponseBody{}, err
	}
	producerEpoch, err := r.ReadInt16()
	if err != nil {
		return message.TransactionResponseBody{}, err
	}
	transactionState, err := r.ReadNullableString()
	if err != nil {
		return message.TransactionResponseBody{}, err
	}
	return message.TransactionResponseBody{
		ErrorCode:        protocol.ErrorCodeFromCode(errorCode),
		ProducerID:       producerID,
		ProducerEpoch:    producerEpoch,
		TransactionState: transactionState,
	}, nil
}

func decodeTransactionResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeTransactionResponseBody(r)
}
