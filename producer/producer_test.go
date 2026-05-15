package producer

import (
	"context"
	"encoding/binary"
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
