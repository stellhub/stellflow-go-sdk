package protocol

// ApiKey identifies a Stellflow data-plane API.
type ApiKey int16

const (
	ApiKeyUnknown ApiKey = -32768

	ApiKeyAPIVersions     ApiKey = 0
	ApiKeyMetadata        ApiKey = 1
	ApiKeyProduce         ApiKey = 2
	ApiKeyFetch           ApiKey = 3
	ApiKeyListOffsets     ApiKey = 4
	ApiKeyOffsetCommit    ApiKey = 5
	ApiKeyOffsetFetch     ApiKey = 6
	ApiKeyFindCoordinator ApiKey = 7
	ApiKeyHeartbeat       ApiKey = 8
	ApiKeyJoinGroup       ApiKey = 9
	ApiKeySyncGroup       ApiKey = 10
	ApiKeyInitProducerID  ApiKey = 11
	ApiKeyBeginTxn        ApiKey = 12
	ApiKeyEndTxn          ApiKey = 13

	ApiKeyCreateTopic        ApiKey = 50
	ApiKeyDeleteTopic        ApiKey = 51
	ApiKeyAlterPartition     ApiKey = 52
	ApiKeyDescribeCluster    ApiKey = 53
	ApiKeyHealthCheck        ApiKey = 54
	ApiKeyDecommissionBroker ApiKey = 55
)

// ApiKeyFromCode converts a wire code to an API key.
func ApiKeyFromCode(code int16) ApiKey {
	switch ApiKey(code) {
	case ApiKeyAPIVersions,
		ApiKeyMetadata,
		ApiKeyProduce,
		ApiKeyFetch,
		ApiKeyListOffsets,
		ApiKeyOffsetCommit,
		ApiKeyOffsetFetch,
		ApiKeyFindCoordinator,
		ApiKeyHeartbeat,
		ApiKeyJoinGroup,
		ApiKeySyncGroup,
		ApiKeyInitProducerID,
		ApiKeyBeginTxn,
		ApiKeyEndTxn,
		ApiKeyCreateTopic,
		ApiKeyDeleteTopic,
		ApiKeyAlterPartition,
		ApiKeyDescribeCluster,
		ApiKeyHealthCheck,
		ApiKeyDecommissionBroker:
		return ApiKey(code)
	default:
		return ApiKeyUnknown
	}
}

// Code returns the int16 value used on the wire.
func (k ApiKey) Code() int16 {
	return int16(k)
}
