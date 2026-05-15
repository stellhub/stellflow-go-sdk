package stellflow_test

import (
	"testing"

	"github.com/stellhub/stellflow-go-sdk/stellflow"
)

func TestNewClientFactory(t *testing.T) {
	factory, err := stellflow.NewClientFactory(stellflow.Options{
		BootstrapServers: []string{"stellflow://127.0.0.1:9092"},
		ClientID:         "test-client",
	})
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}
	defer factory.Close()
	if factory.NewAdmin() == nil {
		t.Fatal("NewAdmin() = nil")
	}
	if factory.NewProducer() == nil {
		t.Fatal("NewProducer() = nil")
	}
	if factory.NewConsumer() == nil {
		t.Fatal("NewConsumer() = nil")
	}
}
