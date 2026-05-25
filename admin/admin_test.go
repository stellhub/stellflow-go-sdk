package admin

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

func TestCreateTopicSendsTopicAdminRequest(t *testing.T) {
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
	go serveCreateTopicBroker(t, listener, serverDone)

	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	protocolClient := protocolclient.New(pool, codec.DefaultRegistry(), "admin-test")
	metadataManager := imetadata.New(protocolClient, []transport.Endpoint{endpoint})
	client := New(protocolClient, metadataManager)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.CreateTopic(ctx, "orders", 3)
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if result.Topic != "orders" || !result.Created || len(result.Partitions) != 3 {
		t.Fatalf("CreateTopic() = %+v", result)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func serveCreateTopicBroker(t *testing.T, listener net.Listener, done chan<- error) {
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
	if err := expectAdminAPIKey(conn, protocol.ApiKeyAPIVersions, func(correlationID int32) []byte {
		return adminAPIVersionsResponseFrame(correlationID)
	}); err != nil {
		done <- err
		return
	}
	frame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		done <- err
		return
	}
	apiKey, correlationID, body, err := adminRequest(frame)
	if err != nil {
		done <- err
		return
	}
	if apiKey != protocol.ApiKeyCreateTopic {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "expected create topic request"}
		return
	}
	topic, partitionCount, err := readCreateTopicRequest(body)
	if err != nil {
		done <- err
		return
	}
	if topic != "orders" || partitionCount != 3 {
		done <- &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "unexpected create topic request"}
		return
	}
	if _, err := conn.Write(createTopicResponseFrame(correlationID, topic, partitionCount)); err != nil {
		done <- err
		return
	}
	done <- nil
}

func expectAdminAPIKey(conn net.Conn, want protocol.ApiKey, response func(int32) []byte) error {
	frame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
	if err != nil {
		return err
	}
	apiKey, correlationID, _, err := adminRequest(frame)
	if err != nil {
		return err
	}
	if apiKey != want {
		return &protocol.ClientError{Code: protocol.ErrorCodeInvalidRequest, Message: "unexpected api key"}
	}
	_, err = conn.Write(response(correlationID))
	return err
}

func adminRequest(frame []byte) (protocol.ApiKey, int32, []byte, error) {
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
	for range 3 {
		if _, err := reader.ReadNullableString(); err != nil {
			return protocol.ApiKeyUnknown, 0, nil, err
		}
	}
	if _, err := reader.ReadInt8(); err != nil {
		return protocol.ApiKeyUnknown, 0, nil, err
	}
	for range 3 {
		if _, err := reader.ReadNullableString(); err != nil {
			return protocol.ApiKeyUnknown, 0, nil, err
		}
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

func readCreateTopicRequest(body []byte) (string, int32, error) {
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

func adminAPIVersionsResponseFrame(correlationID int32) []byte {
	return adminResponseFrame(correlationID, func(writer *codec.Writer) {
		writer.WriteArrayLen(2)
		writeAdminAPIVersionRange(writer, protocol.ApiKeyAPIVersions)
		writeAdminAPIVersionRange(writer, protocol.ApiKeyCreateTopic)
		name := "stellflow-test-broker"
		writer.WriteNullableString(&name)
		writer.WriteNullableString(nil)
		writer.WriteStringArray(nil)
	})
}

func writeAdminAPIVersionRange(writer *codec.Writer, apiKey protocol.ApiKey) {
	writer.WriteInt16(apiKey.Code())
	writer.WriteInt16(0)
	writer.WriteInt16(0)
}

func createTopicResponseFrame(correlationID int32, topic string, partitionCount int32) []byte {
	return adminResponseFrame(correlationID, func(writer *codec.Writer) {
		writer.WriteNullableString(&topic)
		writer.WriteArrayLen(int(partitionCount))
		for partition := int32(0); partition < partitionCount; partition++ {
			writer.WriteInt32(partition)
			writer.WriteInt16(protocol.ErrorCodeNone.Code())
			writer.WriteInt32(1)
		}
	})
}

func adminResponseFrame(correlationID int32, writeBody func(*codec.Writer)) []byte {
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
