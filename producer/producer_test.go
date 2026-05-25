package producer

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	imetadata "github.com/stellhub/stellflow-go-sdk/internal/metadata"
	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
)

func TestSendAsyncFlushBatchesRecords(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()
	endpoint, err := transport.ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	serverDone := make(chan error, 1)
	go serveProducerBroker(t, listener, endpoint, serverDone)

	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	protocolClient := protocolclient.New(pool, codec.DefaultRegistry(), "producer-test")
	metadataManager := imetadata.New(protocolClient, []transport.Endpoint{endpoint})
	client := NewWithOptions(protocolClient, metadataManager, Options{
		BatchSize:  10,
		BatchBytes: 1024 * 1024,
		Linger:     time.Hour,
		QueueSize:  10,
	})
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := client.SendAsync(ctx, Record{Topic: "orders", Value: []byte("a")})
	if err != nil {
		t.Fatalf("SendAsync(first) error = %v", err)
	}
	second, err := client.SendAsync(ctx, Record{Topic: "orders", Value: []byte("b")})
	if err != nil {
		t.Fatalf("SendAsync(second) error = %v", err)
	}
	if err := client.Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	firstMetadata, err := first.Await(ctx)
	if err != nil {
		t.Fatalf("first Await() error = %v", err)
	}
	secondMetadata, err := second.Await(ctx)
	if err != nil {
		t.Fatalf("second Await() error = %v", err)
	}
	if firstMetadata.Offset != 42 || secondMetadata.Offset != 43 {
		t.Fatalf("offsets = %d/%d, want 42/43", firstMetadata.Offset, secondMetadata.Offset)
	}
	if firstMetadata.Partition != 0 || secondMetadata.Partition != 0 {
		t.Fatalf("partitions = %d/%d, want 0/0", firstMetadata.Partition, secondMetadata.Partition)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func TestProducerAutoCreatesMissingTopicBeforeSend(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()
	endpoint, err := transport.ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	serverDone := make(chan error, 1)
	go serveAutoCreateProducerBroker(t, listener, endpoint, serverDone)

	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	protocolClient := protocolclient.New(pool, codec.DefaultRegistry(), "producer-test")
	metadataManager := imetadata.New(protocolClient, []transport.Endpoint{endpoint})
	client := NewWithOptions(protocolClient, metadataManager, Options{})
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metadata, err := client.Send(ctx, Record{Topic: "orders", Value: []byte("created")})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if metadata.Topic != "orders" || metadata.Partition != 0 || metadata.Offset != 42 {
		t.Fatalf("metadata = %+v", metadata)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func TestProducerCanDisableAutoCreateTopics(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()
	endpoint, err := transport.ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	serverDone := make(chan error, 1)
	go serveMissingTopicMetadataBroker(t, listener, serverDone)

	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	protocolClient := protocolclient.New(pool, codec.DefaultRegistry(), "producer-test")
	metadataManager := imetadata.New(protocolClient, []transport.Endpoint{endpoint})
	client := NewWithOptions(protocolClient, metadataManager, Options{DisableAutoCreateTopics: true})
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Send(ctx, Record{Topic: "orders", Value: []byte("missing")}); !imetadata.IsMissingPartitions(err) {
		t.Fatalf("Send() error = %v, want missing partitions", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func TestIdempotentProducerInitializesIdentityAndSequencesBatches(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()
	endpoint, err := transport.ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	serverDone := make(chan error, 1)
	go serveIdempotentProducerBroker(t, listener, endpoint, serverDone)

	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	protocolClient := protocolclient.New(pool, codec.DefaultRegistry(), "producer-test")
	metadataManager := imetadata.New(protocolClient, []transport.Endpoint{endpoint})
	client := NewWithOptions(protocolClient, metadataManager, Options{
		Idempotent:  true,
		BatchSize:   1,
		Linger:      time.Hour,
		QueueSize:   10,
		MaxInFlight: 2,
	})
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := client.Send(ctx, Record{Topic: "orders", Value: []byte("a")})
	if err != nil {
		t.Fatalf("Send(first) error = %v", err)
	}
	second, err := client.Send(ctx, Record{Topic: "orders", Value: []byte("b")})
	if err != nil {
		t.Fatalf("Send(second) error = %v", err)
	}
	if first.Offset != 42 || second.Offset != 42 {
		t.Fatalf("offsets = %d/%d, want both acknowledged base offset 42", first.Offset, second.Offset)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func TestTransactionAPIsSendControlRequests(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()
	endpoint, err := transport.ParseEndpoint(listener.Addr().String())
	if err != nil {
		t.Fatalf("ParseEndpoint() error = %v", err)
	}
	serverDone := make(chan error, 1)
	go serveTransactionBroker(t, listener, serverDone)

	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	protocolClient := protocolclient.New(pool, codec.DefaultRegistry(), "producer-test")
	metadataManager := imetadata.New(protocolClient, []transport.Endpoint{endpoint})
	client := NewWithOptions(protocolClient, metadataManager, Options{TransactionalID: "txn-a"})
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metadata, err := client.InitProducerID(ctx)
	if err != nil {
		t.Fatalf("InitProducerID() error = %v", err)
	}
	if metadata.ProducerID != 123 || metadata.ProducerEpoch != 2 {
		t.Fatalf("InitProducerID() = %+v", metadata)
	}
	begin, err := client.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}
	if begin.TransactionState == nil || *begin.TransactionState != "ONGOING" {
		t.Fatalf("BeginTransaction() = %+v", begin)
	}
	commit, err := client.CommitTransaction(ctx)
	if err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	if commit.TransactionState == nil || *commit.TransactionState != "COMMITTED" {
		t.Fatalf("CommitTransaction() = %+v", commit)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func TestKeyHashPartitionerIsStable(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	record := Record{Topic: "orders", Key: []byte("same-key")}
	first, err := KeyHashPartitioner(record, partitions)
	if err != nil {
		t.Fatalf("KeyHashPartitioner() error = %v", err)
	}
	for range 10 {
		got, err := KeyHashPartitioner(record, partitions)
		if err != nil {
			t.Fatalf("KeyHashPartitioner() error = %v", err)
		}
		if got != first {
			t.Fatalf("partition = %d, want stable %d", got, first)
		}
	}
}

func TestProducerCloseRejectsAsyncEnqueueAndFlushesQueuedFutures(t *testing.T) {
	client := NewWithOptions(nil, nil, Options{QueueSize: 1})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := client.Flush(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush() error = %v, want ErrClosed", err)
	}
	if _, err := client.SendAsync(ctx, Record{Topic: "orders", Value: []byte("a")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("SendAsync() error = %v, want ErrClosed", err)
	}
}

func TestProducerRejectsSendAfterClose(t *testing.T) {
	client := NewWithOptions(nil, nil, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := client.Send(ctx, Record{Topic: "orders", Value: []byte("a")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Send() error = %v, want ErrClosed", err)
	}
	if _, err := client.SendAsync(ctx, Record{Topic: "orders", Value: []byte("a")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("SendAsync() error = %v, want ErrClosed", err)
	}
}

func serveProducerBroker(t *testing.T, listener net.Listener, endpoint transport.Endpoint, done chan<- error) {
	t.Helper()
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
	if err := expectProducerAPIVersionsRequest(conn); err != nil {
		done <- err
		return
	}
	metadataFrame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		done <- err
		return
	}
	apiKey, correlationID, _, err := producerRequest(metadataFrame)
	if err != nil {
		done <- err
		return
	}
	if apiKey != protocol.ApiKeyMetadata {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected metadata request"}
		return
	}
	if _, err := conn.Write(producerMetadataResponseFrame(correlationID, endpoint)); err != nil {
		done <- err
		return
	}

	produceFrame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		done <- err
		return
	}
	apiKey, correlationID, body, err := producerRequest(produceFrame)
	if err != nil {
		done <- err
		return
	}
	if apiKey != protocol.ApiKeyProduce {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected produce request"}
		return
	}
	if recordCount, err := produceRecordCount(body); err != nil {
		done <- err
		return
	} else if recordCount != 2 {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected two batched records"}
		return
	}
	if _, err := conn.Write(produceResponseFrame(correlationID)); err != nil {
		done <- err
		return
	}
	done <- nil
}

func serveAutoCreateProducerBroker(t *testing.T, listener net.Listener, endpoint transport.Endpoint, done chan<- error) {
	t.Helper()
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
	if err := expectProducerAPIVersionsRequest(conn); err != nil {
		done <- err
		return
	}
	if err := expectMetadataRequestWithResponse(conn, missingTopicMetadataResponseFrame); err != nil {
		done <- err
		return
	}
	frame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		done <- err
		return
	}
	apiKey, correlationID, body, err := producerRequest(frame)
	if err != nil {
		done <- err
		return
	}
	if apiKey != protocol.ApiKeyCreateTopic {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected create topic request"}
		return
	}
	topic, partitionCount, err := createTopicRequest(body)
	if err != nil {
		done <- err
		return
	}
	if topic != "orders" || partitionCount != 2 {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "unexpected create topic request"}
		return
	}
	if _, err := conn.Write(createTopicResponseFrame(correlationID, topic, partitionCount)); err != nil {
		done <- err
		return
	}
	if err := expectMetadataRequest(conn, endpoint); err != nil {
		done <- err
		return
	}
	frame, err = transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		done <- err
		return
	}
	apiKey, correlationID, _, err = producerRequest(frame)
	if err != nil {
		done <- err
		return
	}
	if apiKey != protocol.ApiKeyProduce {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected produce request"}
		return
	}
	if _, err := conn.Write(produceResponseFrame(correlationID)); err != nil {
		done <- err
		return
	}
	done <- nil
}

func serveMissingTopicMetadataBroker(t *testing.T, listener net.Listener, done chan<- error) {
	t.Helper()
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
	if err := expectProducerAPIVersionsRequest(conn); err != nil {
		done <- err
		return
	}
	if err := expectMetadataRequestWithResponse(conn, missingTopicMetadataResponseFrame); err != nil {
		done <- err
		return
	}
	done <- nil
}

func serveIdempotentProducerBroker(t *testing.T, listener net.Listener, endpoint transport.Endpoint, done chan<- error) {
	t.Helper()
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
	if err := expectProducerAPIVersionsRequest(conn); err != nil {
		done <- err
		return
	}
	if err := expectMetadataRequest(conn, endpoint); err != nil {
		done <- err
		return
	}
	frame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		done <- err
		return
	}
	apiKey, correlationID, _, err := producerRequest(frame)
	if err != nil {
		done <- err
		return
	}
	if apiKey != protocol.ApiKeyInitProducerID {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected init producer id request"}
		return
	}
	if _, err := conn.Write(initProducerIDResponseFrame(correlationID, 123, 2)); err != nil {
		done <- err
		return
	}
	if err := expectProduceSequence(conn, 0); err != nil {
		done <- err
		return
	}
	if err := expectProduceSequence(conn, 1); err != nil {
		done <- err
		return
	}
	done <- nil
}

func serveTransactionBroker(t *testing.T, listener net.Listener, done chan<- error) {
	t.Helper()
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
	if err := expectProducerAPIVersionsRequest(conn); err != nil {
		done <- err
		return
	}
	if err := expectAPIKey(conn, protocol.ApiKeyInitProducerID, func(correlationID int32) []byte {
		return initProducerIDResponseFrame(correlationID, 123, 2)
	}); err != nil {
		done <- err
		return
	}
	if err := expectAPIKey(conn, protocol.ApiKeyBeginTxn, func(correlationID int32) []byte {
		return transactionResponseFrame(correlationID, 123, 2, "ONGOING")
	}); err != nil {
		done <- err
		return
	}
	if err := expectAPIKey(conn, protocol.ApiKeyEndTxn, func(correlationID int32) []byte {
		return transactionResponseFrame(correlationID, 123, 2, "COMMITTED")
	}); err != nil {
		done <- err
		return
	}
	done <- nil
}

func expectAPIKey(conn net.Conn, want protocol.ApiKey, response func(int32) []byte) error {
	frame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		return err
	}
	apiKey, correlationID, _, err := producerRequest(frame)
	if err != nil {
		return err
	}
	if apiKey != want {
		return &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "unexpected api key"}
	}
	_, err = conn.Write(response(correlationID))
	return err
}

func expectProducerAPIVersionsRequest(conn net.Conn) error {
	frame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		return err
	}
	apiKey, correlationID, _, err := producerRequest(frame)
	if err != nil {
		return err
	}
	if apiKey != protocol.ApiKeyAPIVersions {
		return &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected api versions request"}
	}
	_, err = conn.Write(producerAPIVersionsResponseFrame(correlationID))
	return err
}

func expectMetadataRequest(conn net.Conn, endpoint transport.Endpoint) error {
	return expectMetadataRequestWithResponse(conn, func(correlationID int32) []byte {
		return producerMetadataResponseFrame(correlationID, endpoint)
	})
}

func expectMetadataRequestWithResponse(conn net.Conn, response func(int32) []byte) error {
	metadataFrame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		return err
	}
	apiKey, correlationID, _, err := producerRequest(metadataFrame)
	if err != nil {
		return err
	}
	if apiKey != protocol.ApiKeyMetadata {
		return &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected metadata request"}
	}
	_, err = conn.Write(response(correlationID))
	return err
}

func createTopicRequest(body []byte) (string, int32, error) {
	reader := codec.NewReader(body)
	topic, err := reader.ReadNullableString()
	if err != nil {
		return "", 0, err
	}
	partitionCount, err := reader.ReadInt32()
	if err != nil {
		return "", 0, err
	}
	if topic == nil {
		return "", partitionCount, nil
	}
	return *topic, partitionCount, nil
}

func expectProduceSequence(conn net.Conn, baseSequence int32) error {
	produceFrame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		return err
	}
	apiKey, correlationID, body, err := producerRequest(produceFrame)
	if err != nil {
		return err
	}
	if apiKey != protocol.ApiKeyProduce {
		return &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected produce request"}
	}
	producerID, producerEpoch, sequence, err := produceBatchIdentity(body)
	if err != nil {
		return err
	}
	if producerID != 123 || producerEpoch != 2 || sequence != baseSequence {
		return &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "unexpected idempotent batch identity"}
	}
	_, err = conn.Write(produceResponseFrame(correlationID))
	return err
}

func producerRequest(frame []byte) (protocol.ApiKey, int32, []byte, error) {
	reader := codec.NewReader(frame)
	apiKeyCode, err := reader.ReadInt16()
	if err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	correlationID, err := reader.ReadInt32()
	if err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadNullableString(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadNullableString(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadNullableString(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadInt8(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadNullableString(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadNullableString(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadNullableString(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadInt8(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadNullableString(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	body, err := reader.ReadRawBytes(reader.Remaining())
	if err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	return protocol.ApiKeyFromCode(apiKeyCode), correlationID, body, nil
}

func produceRecordCount(body []byte) (int, error) {
	reader := codec.NewReader(body)
	if _, err := reader.ReadNullableString(); err != nil {
		return 0, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return 0, err
	}
	if _, err := reader.ReadInt32(); err != nil {
		return 0, err
	}
	topicCount, err := reader.ReadArrayLen()
	if err != nil {
		return 0, err
	}
	total := 0
	for range topicCount {
		if _, err := reader.ReadNullableString(); err != nil {
			return 0, err
		}
		partitionCount, err := reader.ReadArrayLen()
		if err != nil {
			return 0, err
		}
		for range partitionCount {
			if _, err := reader.ReadInt32(); err != nil {
				return 0, err
			}
			records, err := reader.ReadBytes()
			if err != nil {
				return 0, err
			}
			batches, err := codec.DecodeRecordBatchSet(records)
			if err != nil {
				return 0, err
			}
			for _, batch := range batches {
				total += len(batch.Records)
			}
		}
	}
	return total, nil
}

func produceBatchIdentity(body []byte) (int64, int16, int32, error) {
	reader := codec.NewReader(body)
	if _, err := reader.ReadNullableString(); err != nil {
		return 0, 0, 0, err
	}
	if _, err := reader.ReadInt16(); err != nil {
		return 0, 0, 0, err
	}
	if _, err := reader.ReadInt32(); err != nil {
		return 0, 0, 0, err
	}
	topicCount, err := reader.ReadArrayLen()
	if err != nil {
		return 0, 0, 0, err
	}
	if topicCount != 1 {
		return 0, 0, 0, &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected one topic"}
	}
	if _, err := reader.ReadNullableString(); err != nil {
		return 0, 0, 0, err
	}
	partitionCount, err := reader.ReadArrayLen()
	if err != nil {
		return 0, 0, 0, err
	}
	if partitionCount != 1 {
		return 0, 0, 0, &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected one partition"}
	}
	if _, err := reader.ReadInt32(); err != nil {
		return 0, 0, 0, err
	}
	records, err := reader.ReadBytes()
	if err != nil {
		return 0, 0, 0, err
	}
	batches, err := codec.DecodeRecordBatchSet(records)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(batches) != 1 {
		return 0, 0, 0, &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected one record batch"}
	}
	return batches[0].ProducerID, batches[0].ProducerEpoch, batches[0].BaseSequence, nil
}

func producerMetadataResponseFrame(correlationID int32, endpoint transport.Endpoint) []byte {
	return producerResponseFrame(correlationID, func(writer *codec.Writer) {
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
	})
}

func missingTopicMetadataResponseFrame(correlationID int32) []byte {
	return producerResponseFrame(correlationID, func(writer *codec.Writer) {
		clusterID := "cluster-a"
		topic := "orders"
		writer.WriteNullableString(&clusterID)
		writer.WriteInt32(1)
		writer.WriteArrayLen(0)
		writer.WriteArrayLen(1)
		writer.WriteInt16(protocol.ErrorCodeUnknownTopicOrPartition.Code())
		writer.WriteNullableString(&topic)
		writer.WriteBool(false)
		writer.WriteArrayLen(0)
		writer.WriteInt32(0)
		writer.WriteInt32(0)
	})
}

func createTopicResponseFrame(correlationID int32, topic string, partitionCount int32) []byte {
	return producerResponseFrame(correlationID, func(writer *codec.Writer) {
		writer.WriteNullableString(&topic)
		writer.WriteArrayLen(int(partitionCount))
		for partition := int32(0); partition < partitionCount; partition++ {
			writer.WriteInt32(partition)
			writer.WriteInt16(protocol.ErrorCodeNone.Code())
			writer.WriteInt32(7)
		}
	})
}

func producerAPIVersionsResponseFrame(correlationID int32) []byte {
	return producerResponseFrame(correlationID, func(writer *codec.Writer) {
		writer.WriteArrayLen(7)
		writeProducerAPIVersionRange(writer, protocol.ApiKeyAPIVersions)
		writeProducerAPIVersionRange(writer, protocol.ApiKeyMetadata)
		writeProducerAPIVersionRange(writer, protocol.ApiKeyProduce)
		writeProducerAPIVersionRange(writer, protocol.ApiKeyInitProducerID)
		writeProducerAPIVersionRange(writer, protocol.ApiKeyBeginTxn)
		writeProducerAPIVersionRange(writer, protocol.ApiKeyEndTxn)
		writeProducerAPIVersionRange(writer, protocol.ApiKeyCreateTopic)
		name := "stellflow-test-broker"
		writer.WriteNullableString(&name)
		writer.WriteNullableString(nil)
		writer.WriteStringArray(nil)
	})
}

func writeProducerAPIVersionRange(writer *codec.Writer, apiKey protocol.ApiKey) {
	writer.WriteInt16(apiKey.Code())
	writer.WriteInt16(0)
	writer.WriteInt16(0)
}

func initProducerIDResponseFrame(correlationID int32, producerID int64, producerEpoch int16) []byte {
	return producerResponseFrame(correlationID, func(writer *codec.Writer) {
		writer.WriteInt16(protocol.ErrorCodeNone.Code())
		writer.WriteInt64(producerID)
		writer.WriteInt16(producerEpoch)
	})
}

func transactionResponseFrame(correlationID int32, producerID int64, producerEpoch int16, state string) []byte {
	return producerResponseFrame(correlationID, func(writer *codec.Writer) {
		writer.WriteInt16(protocol.ErrorCodeNone.Code())
		writer.WriteInt64(producerID)
		writer.WriteInt16(producerEpoch)
		writer.WriteNullableString(&state)
	})
}

func produceResponseFrame(correlationID int32) []byte {
	return producerResponseFrame(correlationID, func(writer *codec.Writer) {
		topic := "orders"
		writer.WriteArrayLen(1)
		writer.WriteNullableString(&topic)
		writer.WriteArrayLen(1)
		writer.WriteInt32(0)
		writer.WriteInt16(protocol.ErrorCodeNone.Code())
		writer.WriteInt64(42)
		writer.WriteInt32(7)
		writer.WriteInt64(1000)
		writer.WriteInt64(5)
	})
}

func producerResponseFrame(correlationID int32, writeBody func(*codec.Writer)) []byte {
	writer := codec.NewWriter()
	writer.WriteInt32(correlationID)
	writer.WriteInt16(protocol.DefaultHeaderVersion)
	writer.WriteInt16(protocol.ErrorCodeNone.Code())
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
