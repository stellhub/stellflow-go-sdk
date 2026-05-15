package message

// APIVersionsRequestBody is the body for apiKey=0, apiVersion=0.
type APIVersionsRequestBody struct {
	ClientSoftwareName    *string
	ClientSoftwareVersion *string
	SupportedFeatures     []string
}

// APIVersionRange describes a supported version range for one API.
type APIVersionRange struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// APIVersionsResponseBody is the response body for apiKey=0, apiVersion=0.
type APIVersionsResponseBody struct {
	APIVersions           []APIVersionRange
	BrokerSoftwareName    *string
	BrokerSoftwareVersion *string
	SupportedFeatures     []string
}
