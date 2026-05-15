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
	"github.com/stellhub/stellflow-go-sdk/observability"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

func TestClientInjectsOpenTelemetryTraceHeader(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	traceIDCh := make(chan string, 1)
	spanIDCh := make(chan string, 1)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		frame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
		if err != nil {
			serverDone <- err
			return
		}
		correlationID, traceID, spanID, err := requestTraceHeader(frame)
		if err != nil {
			serverDone <- err
			return
		}
		traceIDCh <- traceID
		spanIDCh <- spanID
		if _, err := conn.Write(apiVersionsResponseFrame(correlationID)); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	endpoint, err := transport.ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	tracerProvider := sdktrace.NewTracerProvider()
	defer func() { _ = tracerProvider.Shutdown(context.Background()) }()
	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	client := protocolclient.NewWithOptions(pool, codec.DefaultRegistry(), "test-client", protocolclient.Options{
		Observability: observability.Options{TracerProvider: tracerProvider},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.APIVersions(ctx, endpoint); err != nil {
		t.Fatalf("APIVersions() error = %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
	if traceID := <-traceIDCh; traceID == "" {
		t.Fatal("trace id is empty")
	}
	if spanID := <-spanIDCh; spanID == "" {
		t.Fatal("span id is empty")
	}
}

func TestClientNegotiatesHighestSupportedAPIVersion(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	versionCh := make(chan int16, 1)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		apiVersionsFrame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
		if err != nil {
			serverDone <- err
			return
		}
		correlationID, err := requestCorrelationID(apiVersionsFrame)
		if err != nil {
			serverDone <- err
			return
		}
		if _, err := conn.Write(apiVersionsResponseFrameWithRanges(correlationID, []message.APIVersionRange{
			{APIKey: protocol.ApiKeyAPIVersions.Code(), MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.ApiKeyMetadata.Code(), MinVersion: 1, MaxVersion: 1},
		})); err != nil {
			serverDone <- err
			return
		}
		metadataFrame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
		if err != nil {
			serverDone <- err
			return
		}
		apiKey, apiVersion, correlationID, err := requestAPIKeyVersionCorrelation(metadataFrame)
		if err != nil {
			serverDone <- err
			return
		}
		if apiKey != protocol.ApiKeyMetadata {
			serverDone <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected metadata request"}
			return
		}
		versionCh <- apiVersion
		if _, err := conn.Write(metadataResponseFrame(correlationID)); err != nil {
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
	client := protocolclient.New(pool, registryWithMetadataV1(), "test-client")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Metadata(ctx, endpoint, []string{"orders"}); err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if version := <-versionCh; version != 1 {
		t.Fatalf("metadata api version = %d, want 1", version)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func TestClientFallsBackWhenNegotiatedVersionUnsupported(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	versionsCh := make(chan []int16, 1)
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		apiVersionsFrame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
		if err != nil {
			serverDone <- err
			return
		}
		correlationID, err := requestCorrelationID(apiVersionsFrame)
		if err != nil {
			serverDone <- err
			return
		}
		if _, err := conn.Write(apiVersionsResponseFrameWithRanges(correlationID, []message.APIVersionRange{
			{APIKey: protocol.ApiKeyAPIVersions.Code(), MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.ApiKeyMetadata.Code(), MinVersion: 1, MaxVersion: 1},
		})); err != nil {
			serverDone <- err
			return
		}
		firstMetadataFrame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
		if err != nil {
			serverDone <- err
			return
		}
		apiKey, firstVersion, firstCorrelationID, err := requestAPIKeyVersionCorrelation(firstMetadataFrame)
		if err != nil {
			serverDone <- err
			return
		}
		if apiKey != protocol.ApiKeyMetadata {
			serverDone <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected metadata request"}
			return
		}
		if _, err := conn.Write(errorResponseFrame(firstCorrelationID, protocol.ErrorCodeUnsupportedVersion)); err != nil {
			serverDone <- err
			return
		}
		secondMetadataFrame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
		if err != nil {
			serverDone <- err
			return
		}
		apiKey, secondVersion, secondCorrelationID, err := requestAPIKeyVersionCorrelation(secondMetadataFrame)
		if err != nil {
			serverDone <- err
			return
		}
		if apiKey != protocol.ApiKeyMetadata {
			serverDone <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected metadata fallback request"}
			return
		}
		versionsCh <- []int16{firstVersion, secondVersion}
		if _, err := conn.Write(metadataResponseFrame(secondCorrelationID)); err != nil {
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
	client := protocolclient.New(pool, registryWithMetadataV1(), "test-client")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Metadata(ctx, endpoint, []string{"orders"}); err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	versions := <-versionsCh
	if len(versions) != 2 || versions[0] != 1 || versions[1] != protocol.DefaultAPIVersion {
		t.Fatalf("metadata api versions = %v, want [1 %d]", versions, protocol.DefaultAPIVersion)
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

func requestAPIKeyVersionCorrelation(frame []byte) (protocol.ApiKey, int16, int32, error) {
	reader := codec.NewReader(frame)
	apiKeyCode, err := reader.ReadInt16()
	if err != nil {
		return protocol.ApiKeyUnknown, 0, 0, err
	}
	apiVersion, err := reader.ReadInt16()
	if err != nil {
		return protocol.ApiKeyUnknown, 0, 0, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return protocol.ApiKeyUnknown, 0, 0, err
	}
	correlationID, err := reader.ReadInt32()
	if err != nil {
		return protocol.ApiKeyUnknown, 0, 0, err
	}
	return protocol.ApiKeyFromCode(apiKeyCode), apiVersion, correlationID, nil
}

func requestTraceHeader(frame []byte) (int32, string, string, error) {
	reader := codec.NewReader(frame)
	if _, err := reader.ReadInt16(); err != nil {
		return 0, "", "", err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return 0, "", "", err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return 0, "", "", err
	}
	correlationID, err := reader.ReadInt32()
	if err != nil {
		return 0, "", "", err
	}
	if _, err := reader.ReadNullableString(); err != nil {
		return 0, "", "", err
	}
	traceID, err := reader.ReadNullableString()
	if err != nil {
		return 0, "", "", err
	}
	spanID, err := reader.ReadNullableString()
	if err != nil {
		return 0, "", "", err
	}
	if traceID == nil || spanID == nil {
		return 0, "", "", &protocol.DecodingError{Message: "trace header is missing"}
	}
	return correlationID, *traceID, *spanID, nil
}

func apiVersionsResponseFrame(correlationID int32) []byte {
	return apiVersionsResponseFrameWithRanges(correlationID, []message.APIVersionRange{
		{APIKey: protocol.ApiKeyAPIVersions.Code(), MinVersion: 0, MaxVersion: 0},
	})
}

func apiVersionsResponseFrameWithRanges(correlationID int32, ranges []message.APIVersionRange) []byte {
	writer := codec.NewWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(protocol.DefaultHeaderVersion)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
	writer.WriteInt32(0)
	writer.WriteArrayLen(len(ranges))
	for _, apiVersion := range ranges {
		writer.WriteInt16(apiVersion.APIKey)
		writer.WriteInt16(apiVersion.MinVersion)
		writer.WriteInt16(apiVersion.MaxVersion)
	}
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

func metadataResponseFrame(correlationID int32) []byte {
	return responseFrame(correlationID, protocol.ErrorCodeNone, func(writer *codec.Writer) {
		clusterID := "cluster-a"
		writer.WriteNullableString(&clusterID)
		writer.WriteInt32(1)
		writer.WriteArrayLen(0)
		writer.WriteArrayLen(0)
		writer.WriteInt32(0)
	})
}

func errorResponseFrame(correlationID int32, errorCode protocol.ErrorCode) []byte {
	return responseFrame(correlationID, errorCode, func(*codec.Writer) {})
}

func responseFrame(correlationID int32, errorCode protocol.ErrorCode, writeBody func(*codec.Writer)) []byte {
	writer := codec.NewWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(protocol.DefaultHeaderVersion)
	writer.WriteInt16(errorCode.Code())
	writer.WriteInt32(0)
	writeBody(writer)
	payload, err := writer.Bytes()
	if err != nil {
		panic(err)
	}
	frame := make([]byte, 4, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	return append(frame, payload...)
}

func registryWithMetadataV1() *codec.Registry {
	registry := codec.DefaultRegistry()
	registry.RegisterRequestEncoder(protocol.ApiKeyMetadata, 1, func(w *codec.Writer, body codec.RequestBody) error {
		typedBody, ok := body.(message.MetadataRequestBody)
		if !ok {
			return &protocol.EncodingError{Message: "expected metadata request body"}
		}
		codec.EncodeMetadataRequestBody(w, typedBody)
		return nil
	})
	registry.RegisterResponseDecoder(protocol.ApiKeyMetadata, 1, func(r *codec.Reader) (codec.ResponseBody, error) {
		return codec.DecodeMetadataResponseBody(r)
	})
	return registry
}
