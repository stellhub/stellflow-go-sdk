package codec

import "github.com/stellhub/stellflow-go-sdk/protocol"

// EncodeRequestHeader encodes the common request header.
func EncodeRequestHeader(w *Writer, header protocol.RequestHeader) {
	w.WriteInt16(header.APIKey.Code())
	w.WriteInt16(header.APIVersion)
	w.WriteInt16(header.HeaderVersion)
	w.WriteInt32(header.CorrelationID)
	w.WriteNullableString(header.ClientID)
	w.WriteNullableString(header.TraceID)
	w.WriteNullableString(header.SpanID)
	w.WriteInt8(header.TraceFlags)
	w.WriteNullableString(header.TenantID)
	w.WriteNullableString(header.QuotaKey)
	w.WriteNullableString(header.AuthContextID)
	w.WriteInt8(header.TrafficClass)
	w.WriteNullableString(header.TrafficTag)
	w.WriteInt16(header.Flags)
}

// DecodeResponseHeader decodes the common response header.
func DecodeResponseHeader(r *Reader) (protocol.ResponseHeader, error) {
	correlationID, err := r.ReadInt32()
	if err != nil {
		return protocol.ResponseHeader{}, err
	}
	headerVersion, err := r.ReadInt16()
	if err != nil {
		return protocol.ResponseHeader{}, err
	}
	errorCode, err := r.ReadInt16()
	if err != nil {
		return protocol.ResponseHeader{}, err
	}
	throttleTimeMs, err := r.ReadInt32()
	if err != nil {
		return protocol.ResponseHeader{}, err
	}
	return protocol.ResponseHeader{
		CorrelationID:  correlationID,
		HeaderVersion:  headerVersion,
		ErrorCode:      protocol.ErrorCodeFromCode(errorCode),
		ThrottleTimeMs: throttleTimeMs,
	}, nil
}
