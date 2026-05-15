package protocol

const (
	// DefaultAPIVersion is the first protocol version implemented by Stellflow.
	DefaultAPIVersion int16 = 0

	// DefaultHeaderVersion is the current request header version.
	DefaultHeaderVersion int16 = 2
)

// RequestHeader is the common header for every Stellflow request frame.
type RequestHeader struct {
	APIKey        ApiKey
	APIVersion    int16
	HeaderVersion int16
	CorrelationID int32
	ClientID      *string
	TraceID       *string
	SpanID        *string
	TraceFlags    int8
	TenantID      *string
	QuotaKey      *string
	AuthContextID *string
	TrafficClass  int8
	TrafficTag    *string
	Flags         int16
}

// ResponseHeader is the common header for every Stellflow response frame.
type ResponseHeader struct {
	CorrelationID  int32
	HeaderVersion  int16
	ErrorCode      ErrorCode
	ThrottleTimeMs int32
}
