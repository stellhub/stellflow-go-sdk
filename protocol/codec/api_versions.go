package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

// EncodeAPIVersionsRequestBody encodes apiKey=0, apiVersion=0 request body.
func EncodeAPIVersionsRequestBody(w *Writer, body message.APIVersionsRequestBody) {
	w.WriteNullableString(body.ClientSoftwareName)
	w.WriteNullableString(body.ClientSoftwareVersion)
	w.WriteStringArray(body.SupportedFeatures)
}

func encodeAPIVersionsRequestBodyAny(w *Writer, body RequestBody) error {
	typedBody, ok := body.(message.APIVersionsRequestBody)
	if !ok {
		return fmt.Errorf("expected message.APIVersionsRequestBody, got %T", body)
	}
	EncodeAPIVersionsRequestBody(w, typedBody)
	return nil
}

// DecodeAPIVersionsResponseBody decodes apiKey=0, apiVersion=0 response body.
func DecodeAPIVersionsResponseBody(r *Reader) (message.APIVersionsResponseBody, error) {
	length, err := r.ReadArrayLen()
	if err != nil {
		return message.APIVersionsResponseBody{}, err
	}
	apiVersions := make([]message.APIVersionRange, 0, length)
	for range length {
		apiKey, err := r.ReadInt16()
		if err != nil {
			return message.APIVersionsResponseBody{}, err
		}
		minVersion, err := r.ReadInt16()
		if err != nil {
			return message.APIVersionsResponseBody{}, err
		}
		maxVersion, err := r.ReadInt16()
		if err != nil {
			return message.APIVersionsResponseBody{}, err
		}
		apiVersions = append(apiVersions, message.APIVersionRange{
			APIKey:     apiKey,
			MinVersion: minVersion,
			MaxVersion: maxVersion,
		})
	}
	brokerSoftwareName, err := r.ReadNullableString()
	if err != nil {
		return message.APIVersionsResponseBody{}, err
	}
	brokerSoftwareVersion, err := r.ReadNullableString()
	if err != nil {
		return message.APIVersionsResponseBody{}, err
	}
	supportedFeatures, err := r.ReadStringArray()
	if err != nil {
		return message.APIVersionsResponseBody{}, err
	}
	return message.APIVersionsResponseBody{
		APIVersions:           apiVersions,
		BrokerSoftwareName:    brokerSoftwareName,
		BrokerSoftwareVersion: brokerSoftwareVersion,
		SupportedFeatures:     supportedFeatures,
	}, nil
}

func decodeAPIVersionsResponseBodyAny(r *Reader) (ResponseBody, error) {
	return DecodeAPIVersionsResponseBody(r)
}
