package transport_test

import (
	"testing"

	"github.com/stellhub/stellflow-go-sdk/internal/transport"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want transport.Endpoint
	}{
		{name: "scheme", in: "stellflow://127.0.0.1:9092", want: transport.Endpoint{Host: "127.0.0.1", Port: 9092}},
		{name: "host port", in: "localhost:19092", want: transport.Endpoint{Host: "localhost", Port: 19092}},
		{name: "default port", in: "localhost", want: transport.Endpoint{Host: "localhost", Port: 9092}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := transport.ParseEndpoint(tt.in)
			if err != nil {
				t.Fatalf("ParseEndpoint() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ParseEndpoint() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
