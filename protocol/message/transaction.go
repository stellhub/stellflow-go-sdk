package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

// InitProducerIDRequestBody is the body for apiKey=11, apiVersion=0.
type InitProducerIDRequestBody struct {
	TransactionalID *string
}

// InitProducerIDResponseBody is the response body for apiKey=11, apiVersion=0.
type InitProducerIDResponseBody struct {
	ErrorCode     protocol.ErrorCode
	ProducerID    int64
	ProducerEpoch int16
}

// TransactionRequestBody is the request body shared by BeginTxn and EndTxn.
type TransactionRequestBody struct {
	TransactionalID *string
	ProducerID      int64
	ProducerEpoch   int16
	Commit          bool
}

// TransactionResponseBody is the response body shared by BeginTxn and EndTxn.
type TransactionResponseBody struct {
	ErrorCode        protocol.ErrorCode
	ProducerID       int64
	ProducerEpoch    int16
	TransactionState *string
}
