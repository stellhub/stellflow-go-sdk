package codec

import (
	"fmt"

	"github.com/stellhub/stellflow-go-sdk/protocol"
)

// RequestBody is a marker for protocol request bodies.
type RequestBody any

// ResponseBody is a marker for protocol response bodies.
type ResponseBody any

// RequestBodyEncoder encodes a request body.
type RequestBodyEncoder func(*Writer, RequestBody) error

// ResponseBodyDecoder decodes a response body.
type ResponseBodyDecoder func(*Reader) (ResponseBody, error)

// Registry maps apiKey + apiVersion to body codecs.
type Registry struct {
	requestEncoders  map[codecKey]RequestBodyEncoder
	responseDecoders map[codecKey]ResponseBodyDecoder
}

type codecKey struct {
	apiKey     protocol.ApiKey
	apiVersion int16
}

// NewRegistry creates an empty codec registry.
func NewRegistry() *Registry {
	return &Registry{
		requestEncoders:  make(map[codecKey]RequestBodyEncoder),
		responseDecoders: make(map[codecKey]ResponseBodyDecoder),
	}
}

// DefaultRegistry returns codecs for the currently implemented protocol baseline.
func DefaultRegistry() *Registry {
	registry := NewRegistry()
	registry.RegisterRequestEncoder(protocol.ApiKeyAPIVersions, protocol.DefaultAPIVersion, encodeAPIVersionsRequestBodyAny)
	registry.RegisterResponseDecoder(protocol.ApiKeyAPIVersions, protocol.DefaultAPIVersion, decodeAPIVersionsResponseBodyAny)
	registry.RegisterRequestEncoder(protocol.ApiKeyMetadata, protocol.DefaultAPIVersion, encodeMetadataRequestBodyAny)
	registry.RegisterResponseDecoder(protocol.ApiKeyMetadata, protocol.DefaultAPIVersion, decodeMetadataResponseBodyAny)
	registry.RegisterRequestEncoder(protocol.ApiKeyProduce, protocol.DefaultAPIVersion, encodeProduceRequestBodyAny)
	registry.RegisterResponseDecoder(protocol.ApiKeyProduce, protocol.DefaultAPIVersion, decodeProduceResponseBodyAny)
	registry.RegisterRequestEncoder(protocol.ApiKeyFetch, protocol.DefaultAPIVersion, encodeFetchRequestBodyAny)
	registry.RegisterResponseDecoder(protocol.ApiKeyFetch, protocol.DefaultAPIVersion, decodeFetchResponseBodyAny)
	registry.RegisterRequestEncoder(protocol.ApiKeyListOffsets, protocol.DefaultAPIVersion, encodeListOffsetsRequestBodyAny)
	registry.RegisterResponseDecoder(protocol.ApiKeyListOffsets, protocol.DefaultAPIVersion, decodeListOffsetsResponseBodyAny)
	return registry
}

// RegisterRequestEncoder registers a request body encoder.
func (r *Registry) RegisterRequestEncoder(apiKey protocol.ApiKey, apiVersion int16, encoder RequestBodyEncoder) {
	r.requestEncoders[codecKey{apiKey: apiKey, apiVersion: apiVersion}] = encoder
}

// RegisterResponseDecoder registers a response body decoder.
func (r *Registry) RegisterResponseDecoder(apiKey protocol.ApiKey, apiVersion int16, decoder ResponseBodyDecoder) {
	r.responseDecoders[codecKey{apiKey: apiKey, apiVersion: apiVersion}] = decoder
}

// EncodeRequestBody encodes a request body with the registered codec.
func (r *Registry) EncodeRequestBody(apiKey protocol.ApiKey, apiVersion int16, body RequestBody) ([]byte, error) {
	encoder, ok := r.requestEncoders[codecKey{apiKey: apiKey, apiVersion: apiVersion}]
	if !ok {
		return nil, &protocol.EncodingError{Message: fmt.Sprintf("missing request codec apiKey=%d apiVersion=%d", apiKey.Code(), apiVersion)}
	}
	writer := NewWriter()
	if err := encoder(writer, body); err != nil {
		return nil, err
	}
	return writer.Bytes()
}

// DecodeResponseBody decodes a response body with the registered codec.
func (r *Registry) DecodeResponseBody(apiKey protocol.ApiKey, apiVersion int16, data []byte) (ResponseBody, error) {
	decoder, ok := r.responseDecoders[codecKey{apiKey: apiKey, apiVersion: apiVersion}]
	if !ok {
		return nil, &protocol.DecodingError{Message: fmt.Sprintf("missing response codec apiKey=%d apiVersion=%d", apiKey.Code(), apiVersion)}
	}
	reader := NewReader(data)
	body, err := decoder(reader)
	if err != nil {
		return nil, err
	}
	if reader.Remaining() != 0 {
		return nil, &protocol.DecodingError{Message: "response body has trailing bytes"}
	}
	return body, nil
}
