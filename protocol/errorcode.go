package protocol

// ErrorCode is a Stellflow protocol error code.
type ErrorCode int16

const (
	ErrorCodeNone ErrorCode = 0

	ErrorCodeUnknownServerError              ErrorCode = 1
	ErrorCodeUnsupportedVersion              ErrorCode = 2
	ErrorCodeInvalidRequest                  ErrorCode = 3
	ErrorCodeAuthenticationFailed            ErrorCode = 4
	ErrorCodeAuthorizationFailed             ErrorCode = 5
	ErrorCodeThrottled                       ErrorCode = 6
	ErrorCodeBrokerNotAvailable              ErrorCode = 7
	ErrorCodeLeaderNotAvailable              ErrorCode = 8
	ErrorCodeNotLeaderOrFollower             ErrorCode = 9
	ErrorCodeUnknownTopicOrPartition         ErrorCode = 10
	ErrorCodeOffsetOutOfRange                ErrorCode = 11
	ErrorCodeMessageTooLarge                 ErrorCode = 12
	ErrorCodeRecordListTooLarge              ErrorCode = 13
	ErrorCodeInvalidRecord                   ErrorCode = 14
	ErrorCodeCorruptMessage                  ErrorCode = 15
	ErrorCodeCoordinatorNotAvailable         ErrorCode = 16
	ErrorCodeNotCoordinator                  ErrorCode = 17
	ErrorCodeConcurrentTransactions          ErrorCode = 18
	ErrorCodeFencedInstanceID                ErrorCode = 19
	ErrorCodeFeatureNotEnabled               ErrorCode = 20
	ErrorCodeInvalidProducerEpoch            ErrorCode = 21
	ErrorCodeOutOfOrderSequenceNumber        ErrorCode = 22
	ErrorCodeDuplicateSequenceNumber         ErrorCode = 23
	ErrorCodeTransactionCoordinatorFenced    ErrorCode = 24
	ErrorCodeInvalidTxnState                 ErrorCode = 25
	ErrorCodeTransactionalIDAuthorizationErr ErrorCode = 26
	ErrorCodeProducerFenced                  ErrorCode = 27
)

// ErrorCodeFromCode converts a wire code to an error code.
func ErrorCodeFromCode(code int16) ErrorCode {
	switch ErrorCode(code) {
	case ErrorCodeNone,
		ErrorCodeUnknownServerError,
		ErrorCodeUnsupportedVersion,
		ErrorCodeInvalidRequest,
		ErrorCodeAuthenticationFailed,
		ErrorCodeAuthorizationFailed,
		ErrorCodeThrottled,
		ErrorCodeBrokerNotAvailable,
		ErrorCodeLeaderNotAvailable,
		ErrorCodeNotLeaderOrFollower,
		ErrorCodeUnknownTopicOrPartition,
		ErrorCodeOffsetOutOfRange,
		ErrorCodeMessageTooLarge,
		ErrorCodeRecordListTooLarge,
		ErrorCodeInvalidRecord,
		ErrorCodeCorruptMessage,
		ErrorCodeCoordinatorNotAvailable,
		ErrorCodeNotCoordinator,
		ErrorCodeConcurrentTransactions,
		ErrorCodeFencedInstanceID,
		ErrorCodeFeatureNotEnabled,
		ErrorCodeInvalidProducerEpoch,
		ErrorCodeOutOfOrderSequenceNumber,
		ErrorCodeDuplicateSequenceNumber,
		ErrorCodeTransactionCoordinatorFenced,
		ErrorCodeInvalidTxnState,
		ErrorCodeTransactionalIDAuthorizationErr,
		ErrorCodeProducerFenced:
		return ErrorCode(code)
	default:
		return ErrorCodeUnknownServerError
	}
}

// Code returns the int16 value used on the wire.
func (c ErrorCode) Code() int16 {
	return int16(c)
}
