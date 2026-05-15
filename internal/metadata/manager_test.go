package metadata

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
)

func TestRefreshFallsBackToNextBootstrap(t *testing.T) {
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	firstEndpoint, err := transport.ParseEndpoint(closed.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	_ = closed.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()
	secondEndpoint, err := transport.ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	serverDone := make(chan error, 1)
	go serveMetadataBroker(listener, secondEndpoint, serverDone)

	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	protocolClient := protocolclient.NewWithOptions(pool, codec.DefaultRegistry(), "metadata-test", protocolclient.Options{
		Retry: protocolclient.RetryOptions{MaxAttempts: 1},
	})
	manager := New(protocolClient, []transport.Endpoint{firstEndpoint, secondEndpoint})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := manager.Refresh(ctx, []string{"orders"})
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if len(response.Brokers) != 1 {
		t.Fatalf("brokers = %d, want 1", len(response.Brokers))
	}
	route, err := manager.Route(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("Route() error = %v", err)
	}
	if route.Endpoint.Address() != secondEndpoint.Address() {
		t.Fatalf("route endpoint = %s, want %s", route.Endpoint.Address(), secondEndpoint.Address())
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func serveMetadataBroker(listener net.Listener, endpoint transport.Endpoint, done chan<- error) {
	conn, err := listener.Accept()
	if err != nil {
		done <- err
		return
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		done <- err
		return
	}
	frame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		done <- err
		return
	}
	apiKey, correlationID, err := metadataRequestHeader(frame)
	if err != nil {
		done <- err
		return
	}
	if apiKey != protocol.ApiKeyMetadata {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected metadata request"}
		return
	}
	if _, err := conn.Write(metadataResponseFrame(correlationID, endpoint)); err != nil {
		done <- err
		return
	}
	done <- nil
}

func metadataRequestHeader(frame []byte) (protocol.ApiKey, int32, error) {
	reader := codec.NewReader(frame)
	apiKeyCode, err := reader.ReadInt16()
	if err != nil {
		return protocol.ApiKeyUnknown, 0, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return protocol.ApiKeyUnknown, 0, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return protocol.ApiKeyUnknown, 0, err
	}
	correlationID, err := reader.ReadInt32()
	if err != nil {
		return protocol.ApiKeyUnknown, 0, err
	}
	return protocol.ApiKeyFromCode(apiKeyCode), correlationID, nil
}

func metadataResponseFrame(correlationID int32, endpoint transport.Endpoint) []byte {
	writer := codec.NewWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(protocol.DefaultHeaderVersion)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteInt32(0)
	clusterID := "cluster-a"
	topic := "orders"
	writer.WriteNullableString(&clusterID)
	writer.WriteInt32(1)
	writer.WriteArrayLen(1)
	writer.WriteInt32(1)
	writer.WriteNullableString(&endpoint.Host)
	writer.WriteInt32(int32(endpoint.Port))
	writer.WriteNullableString(nil)
	writer.WriteArrayLen(1)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteNullableString(&topic)
	writer.WriteBool(false)
	writer.WriteArrayLen(1)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteInt32(0)
	writer.WriteInt32(1)
	writer.WriteInt32(7)
	writer.WriteInt32Array([]int32{1})
	writer.WriteInt32Array([]int32{1})
	writer.WriteInt32Array([]int32{})
	writer.WriteInt32(0)
	writer.WriteInt32(0)
	payload, err := writer.Bytes()
	if err != nil {
		panic(err)
	}
	frame := make([]byte, 4, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	return append(frame, payload...)
}
