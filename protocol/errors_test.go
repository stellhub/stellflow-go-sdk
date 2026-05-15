package protocol_test

import (
	"fmt"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol"
)

func TestErrorHelpers(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &protocol.ClientError{Code: protocol.ErrorCodeNotLeaderOrFollower})
	if !protocol.IsRetriable(err) {
		t.Fatal("IsRetriable() = false, want true")
	}
	if !protocol.IsNotLeaderOrFollower(err) {
		t.Fatal("IsNotLeaderOrFollower() = false, want true")
	}
	if protocol.IsUnsupportedVersion(err) {
		t.Fatal("IsUnsupportedVersion() = true, want false")
	}
}
