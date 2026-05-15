package protocolclient_test

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
)

func TestClientCorrelatesOutOfOrderResponses(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		first, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
		if err != nil {
			serverDone <- err
			return
		}
		second, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
		if err != nil {
			serverDone <- err
			return
		}
		firstID, err := requestCorrelationID(first)
		if err != nil {
			serverDone <- err
			return
		}
		secondID, err := requestCorrelationID(second)
		if err != nil {
			serverDone <- err
			return
		}
		if _, err := conn.Write(apiVersionsResponseFrame(secondID)); err != nil {
			serverDone <- err
			return
		}
		if _, err := conn.Write(apiVersionsResponseFrame(firstID)); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	endpoint, err := transport.ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	client := protocolclient.New(pool, codec.DefaultRegistry(), "test-client")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.APIVersions(ctx, endpoint)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("APIVersions() error = %v", err)
		}
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func TestClientRetriesAfterConnectionDrop(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	serverDone := make(chan error, 1)
	go func() {
		firstConn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		if _, err := transport.ReadFrame(firstConn, transport.DefaultMaxFrameLength); err != nil {
			_ = firstConn.Close()
			serverDone <- err
			return
		}
		_ = firstConn.Close()

		secondConn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer secondConn.Close()
		second, err := transport.ReadFrame(secondConn, transport.DefaultMaxFrameLength)
		if err != nil {
			serverDone <- err
			return
		}
		correlationID, err := requestCorrelationID(second)
		if err != nil {
			serverDone <- err
			return
		}
		if _, err := secondConn.Write(apiVersionsResponseFrame(correlationID)); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	endpoint, err := transport.ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	client := protocolclient.NewWithOptions(pool, codec.DefaultRegistry(), "test-client", protocolclient.Options{
		Retry: protocolclient.RetryOptions{MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.APIVersions(ctx, endpoint); err != nil {
		t.Fatalf("APIVersions() error = %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func requestCorrelationID(frame []byte) (int32, error) {
	reader := codec.NewReader(frame)
	if _, err := reader.ReadInt16(); err != nil {
		return 0, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return 0, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return 0, err
	}
	return reader.ReadInt32()
}

func apiVersionsResponseFrame(correlationID int32) []byte {
	writer := codec.NewWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(protocol.DefaultHeaderVersion)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteInt32(0)
	writer.WriteArrayLen(1)
	writer.WriteInt16(protocol.ApiKeyAPIVersions.Code())
	writer.WriteInt16(0)
	writer.WriteInt16(0)
	name := "stellflow-test-broker"
	writer.WriteNullableString(&name)
	writer.WriteNullableString(nil)
	writer.WriteStringArray(nil)
	payload, err := writer.Bytes()
	if err != nil {
		panic(err)
	}
	frame := make([]byte, 4, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	return append(frame, payload...)
}
