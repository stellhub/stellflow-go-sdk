package consumer

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	imetadata "github.com/stellhub/stellflow-go-sdk/internal/metadata"
	"github.com/stellhub/stellflow-go-sdk/internal/protocolclient"
	"github.com/stellhub/stellflow-go-sdk/internal/transport"
	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
)

type recordingRebalanceListener struct {
	assigned [][]TopicPartition
	revoked  [][]TopicPartition
}

func (l *recordingRebalanceListener) OnPartitionsAssigned(_ context.Context, partitions []TopicPartition) {
	l.assigned = append(l.assigned, partitions)
}

func (l *recordingRebalanceListener) OnPartitionsRevoked(_ context.Context, partitions []TopicPartition) {
	l.revoked = append(l.revoked, partitions)
}

func TestSubscribeJoinsGroupAndRestoresCommittedOffsets(t *testing.T) {
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
	go serveSubscribeBroker(t, listener, endpoint, serverDone)

	pool := transport.NewPool(transport.DefaultMaxFrameLength)
	defer pool.Close()
	protocolClient := protocolclient.New(pool, codec.DefaultRegistry(), "consumer-test")
	metadataManager := imetadata.New(protocolClient, []transport.Endpoint{endpoint})
	rbListener := &recordingRebalanceListener{}
	client := New(protocolClient, metadataManager, Options{
		GroupID:            "orders-group",
		MemberID:           "member-a",
		HeartbeatInterval:  time.Hour,
		RebalanceListener:  rbListener,
		EnableAutoCommit:   true,
		AutoCommitInterval: time.Hour,
		MaxPollInterval:    time.Hour,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Subscribe(ctx, []string{"orders"}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.groupSession == nil {
		t.Fatal("groupSession is nil")
	}
	if client.groupSession.GenerationID != 3 {
		t.Fatalf("GenerationID = %d, want 3", client.groupSession.GenerationID)
	}
	key := imetadata.TopicPartition{Topic: "orders", Partition: 0}
	if got := client.nextOffsets[key]; got != 12 {
		t.Fatalf("nextOffsets[%+v] = %d, want 12", key, got)
	}
	if len(client.assignment) != 1 || client.assignment[0].Topic != "orders" || client.assignment[0].Partition != 0 {
		t.Fatalf("assignment = %+v, want orders[0]", client.assignment)
	}
	if len(rbListener.assigned) != 1 || len(rbListener.assigned[0]) != 1 {
		t.Fatalf("assigned callbacks = %+v, want one partition", rbListener.assigned)
	}
	if len(rbListener.revoked) != 0 {
		t.Fatalf("revoked callbacks = %+v, want none", rbListener.revoked)
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("server error = %v", err)
	}
}

func TestAssignSeekPauseResume(t *testing.T) {
	client := New(nil, nil, Options{})
	if err := client.Assign([]TopicPartition{{Topic: "orders", Partition: 1, Offset: 10}}); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	if err := client.Seek("orders", 1, 42); err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	key := imetadata.TopicPartition{Topic: "orders", Partition: 1}
	client.mu.Lock()
	if got := client.nextOffsets[key]; got != 42 {
		client.mu.Unlock()
		t.Fatalf("next offset = %d, want 42", got)
	}
	client.mu.Unlock()

	client.Pause([]TopicPartition{{Topic: "orders", Partition: 1}})
	client.mu.Lock()
	_, paused := client.paused[key]
	client.mu.Unlock()
	if !paused {
		t.Fatal("partition is not paused")
	}

	client.Resume([]TopicPartition{{Topic: "orders", Partition: 1}})
	client.mu.Lock()
	_, paused = client.paused[key]
	client.mu.Unlock()
	if paused {
		t.Fatal("partition is still paused")
	}
}

func TestSeekRejectsUnassignedPartition(t *testing.T) {
	client := New(nil, nil, Options{})
	if err := client.Assign([]TopicPartition{{Topic: "orders", Partition: 1, Offset: 10}}); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	if err := client.Seek("orders", 2, 42); err == nil {
		t.Fatal("Seek() error = nil, want unassigned error")
	}
}

func serveSubscribeBroker(t *testing.T, listener net.Listener, endpoint transport.Endpoint, done chan<- error) {
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

	expected := []protocol.ApiKey{
		protocol.ApiKeyFindCoordinator,
		protocol.ApiKeyJoinGroup,
		protocol.ApiKeySyncGroup,
		protocol.ApiKeyMetadata,
		protocol.ApiKeyOffsetFetch,
	}
	for _, wantAPIKey := range expected {
		frame, err := transport.ReadFrame(conn, transport.DefaultMaxFrameLength)
		if err != nil {
			done <- err
			return
		}
		gotAPIKey, correlationID, err := testRequestHeader(frame)
		if err != nil {
			done <- err
			return
		}
		if gotAPIKey != wantAPIKey {
			done <- fmt.Errorf("apiKey = %d, want %d", gotAPIKey.Code(), wantAPIKey.Code())
			return
		}
		response := subscribeResponseFrame(correlationID, func(w *codec.Writer) {
			writeSubscribeResponseBody(w, wantAPIKey, endpoint)
		})
		if _, err := conn.Write(response); err != nil {
			done <- err
			return
		}
	}
	done <- nil
}

func testRequestHeader(frame []byte) (protocol.ApiKey, int32, error) {
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

func subscribeResponseFrame(correlationID int32, writeBody func(*codec.Writer)) []byte {
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

func writeSubscribeResponseBody(w *codec.Writer, apiKey protocol.ApiKey, endpoint transport.Endpoint) {
	switch apiKey {
	case protocol.ApiKeyFindCoordinator:
		w.WriteInt16(protocol.ErrorCodeNone.Code())
		w.WriteInt32(1)
		w.WriteNullableString(&endpoint.Host)
		w.WriteInt32(int32(endpoint.Port))
	case protocol.ApiKeyJoinGroup:
		memberID := "member-a"
		leaderID := "member-a"
		w.WriteInt16(protocol.ErrorCodeNone.Code())
		w.WriteInt32(3)
		w.WriteNullableString(&memberID)
		w.WriteNullableString(&leaderID)
	case protocol.ApiKeySyncGroup:
		w.WriteInt16(protocol.ErrorCodeNone.Code())
	case protocol.ApiKeyMetadata:
		clusterID := "cluster-a"
		topic := "orders"
		w.WriteNullableString(&clusterID)
		w.WriteInt32(1)
		w.WriteArrayLen(1)
		w.WriteInt32(1)
		w.WriteNullableString(&endpoint.Host)
		w.WriteInt32(int32(endpoint.Port))
		w.WriteNullableString(nil)
		w.WriteArrayLen(1)
		w.WriteInt16(protocol.ErrorCodeNone.Code())
		w.WriteNullableString(&topic)
		w.WriteBool(false)
		w.WriteArrayLen(1)
		w.WriteInt16(protocol.ErrorCodeNone.Code())
		w.WriteInt32(0)
		w.WriteInt32(1)
		w.WriteInt32(7)
		w.WriteInt32Array([]int32{1})
		w.WriteInt32Array([]int32{1})
		w.WriteInt32Array([]int32{})
		w.WriteInt32(0)
		w.WriteInt32(0)
	case protocol.ApiKeyOffsetFetch:
		topic := "orders"
		metadata := "stored"
		w.WriteArrayLen(1)
		w.WriteNullableString(&topic)
		w.WriteArrayLen(1)
		w.WriteInt32(0)
		w.WriteInt64(12)
		w.WriteNullableString(&metadata)
		w.WriteInt16(protocol.ErrorCodeNone.Code())
	default:
		panic(fmt.Sprintf("unexpected api key %d", apiKey.Code()))
	}
}
