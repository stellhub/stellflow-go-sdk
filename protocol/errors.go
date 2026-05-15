package protocol

import (
	"errors"
	"fmt"
)

// EncodingError indicates that an object cannot be encoded into valid protocol bytes.
type EncodingError struct {
	Message string
}

func (e *EncodingError) Error() string {
	return "stellflow protocol encode: " + e.Message
}

// DecodingError indicates that bytes cannot be decoded as a valid protocol object.
type DecodingError struct {
	Message string
}

func (e *DecodingError) Error() string {
	return "stellflow protocol decode: " + e.Message
}

// ClientError wraps a Stellflow protocol error code.
type ClientError struct {
	Code    ErrorCode
	Message string
}

func (e *ClientError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("stellflow error: code=%d", e.Code.Code())
	}
	return fmt.Sprintf("stellflow error: code=%d message=%s", e.Code.Code(), e.Message)
}

// IsRetriable reports whether an error code can be retried by default.
func IsRetriable(err error) bool {
	var clientErr *ClientError
	ok := errors.As(err, &clientErr)
	if !ok {
		return false
	}
	return IsRetriableCode(clientErr.Code)
}

// IsRetriableCode reports whether an error code can be retried by default.
func IsRetriableCode(code ErrorCode) bool {
	switch code {
	case ErrorCodeUnknownServerError,
		ErrorCodeBrokerNotAvailable,
		ErrorCodeLeaderNotAvailable,
		ErrorCodeNotLeaderOrFollower,
		ErrorCodeUnknownTopicOrPartition,
		ErrorCodeCoordinatorNotAvailable,
		ErrorCodeNotCoordinator,
		ErrorCodeThrottled:
		return true
	default:
		return false
	}
}

// RequiresMetadataRefresh reports whether partition routing should be refreshed.
func RequiresMetadataRefresh(err error) bool {
	var clientErr *ClientError
	ok := errors.As(err, &clientErr)
	if !ok {
		return false
	}
	switch clientErr.Code {
	case ErrorCodeBrokerNotAvailable,
		ErrorCodeLeaderNotAvailable,
		ErrorCodeNotLeaderOrFollower,
		ErrorCodeUnknownTopicOrPartition:
		return true
	default:
		return false
	}
}

// RequiresCoordinatorRefresh reports whether the group coordinator should be rediscovered.
func RequiresCoordinatorRefresh(err error) bool {
	var clientErr *ClientError
	ok := errors.As(err, &clientErr)
	if !ok {
		return false
	}
	switch clientErr.Code {
	case ErrorCodeCoordinatorNotAvailable, ErrorCodeNotCoordinator:
		return true
	default:
		return false
	}
}

// IsUnsupportedVersion reports whether the error is UNSUPPORTED_VERSION.
func IsUnsupportedVersion(err error) bool {
	return hasCode(err, ErrorCodeUnsupportedVersion)
}

// IsUnknownTopicOrPartition reports whether the error is UNKNOWN_TOPIC_OR_PARTITION.
func IsUnknownTopicOrPartition(err error) bool {
	return hasCode(err, ErrorCodeUnknownTopicOrPartition)
}

// IsOffsetOutOfRange reports whether the error is OFFSET_OUT_OF_RANGE.
func IsOffsetOutOfRange(err error) bool {
	return hasCode(err, ErrorCodeOffsetOutOfRange)
}

// IsNotLeaderOrFollower reports whether the error is NOT_LEADER_OR_FOLLOWER.
func IsNotLeaderOrFollower(err error) bool {
	return hasCode(err, ErrorCodeNotLeaderOrFollower)
}

func hasCode(err error, code ErrorCode) bool {
	var clientErr *ClientError
	ok := errors.As(err, &clientErr)
	return ok && clientErr.Code == code
}
