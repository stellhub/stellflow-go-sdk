package message

import "github.com/stellhub/stellflow-go-sdk/protocol"

// JoinGroupRequestBody is the body for apiKey=9, apiVersion=0.
type JoinGroupRequestBody struct {
	GroupID          *string
	MemberID         *string
	SessionTimeoutMs int32
}

// JoinGroupResponseBody is the response body for apiKey=9, apiVersion=0.
type JoinGroupResponseBody struct {
	ErrorCode    protocol.ErrorCode
	GenerationID int32
	MemberID     *string
	LeaderID     *string
}

// SyncGroupRequestBody is the body for apiKey=10, apiVersion=0.
type SyncGroupRequestBody struct {
	GroupID      *string
	GenerationID int32
	MemberID     *string
}

// SyncGroupResponseBody is the response body for apiKey=10, apiVersion=0.
type SyncGroupResponseBody struct {
	ErrorCode protocol.ErrorCode
}

// HeartbeatRequestBody is the body for apiKey=8, apiVersion=0.
type HeartbeatRequestBody struct {
	GroupID      *string
	GenerationID int32
	MemberID     *string
}

// HeartbeatResponseBody is the response body for apiKey=8, apiVersion=0.
type HeartbeatResponseBody struct {
	ErrorCode protocol.ErrorCode
}
