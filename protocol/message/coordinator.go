package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

const (
	// CoordinatorKeyTypeGroup identifies a consumer group coordinator lookup.
	CoordinatorKeyTypeGroup int8 = 0
)

// FindCoordinatorRequestBody is the body for apiKey=7, apiVersion=0.
type FindCoordinatorRequestBody struct {
	Key     *string
	KeyType int8
}

// FindCoordinatorResponseBody is the response body for apiKey=7, apiVersion=0.
type FindCoordinatorResponseBody struct {
	ErrorCode protocol.ErrorCode
	NodeID    int32
	Host      *string
	Port      int32
}
