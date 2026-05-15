package codec_test

import (
	"reflect"
	"testing"

	"github.com/stellhub/stellflow-go-sdk/protocol"
	"github.com/stellhub/stellflow-go-sdk/protocol/codec"
	"github.com/stellhub/stellflow-go-sdk/protocol/message"
)

const (
	goldenProduceRequestHex         = "ffffffff000075300000000100066f72646572730000000100000000000000600000000000000058ffffffff019fca885500000000000000000000000003e800000000000003e8ffffffffffffffffffffffffffff000000010000002300000000000000000000000000000000016b0000000176000000010001680000000178"
	goldenProduceResponseHex        = "0000000100066f726465727300000001000000000000000000000000002a0000000700000000000003e80000000000000005"
	goldenFetchRequestHex           = "ffffffff000001f4000000010010000000000000000000000100066f7264657273000000010000000000000007000000000000002a000000000000000500100000"
	goldenFetchResponseHex          = "000000000000000100066f726465727300000001000000000000000000000000002b0000000000000005000000000000002b00000000000000600000000000000058ffffffff019fca885500000000000000000000000003e800000000000003e8ffffffffffffffffffffffffffff000000010000002300000000000000000000000000000000016b0000000176000000010001680000000178"
	goldenListRequestHex            = "ffffffff000000000100066f7264657273000000010000000000000007ffffffffffffffff00000001"
	goldenListResponseHex           = "0000000100066f72646572730000000100000000000000000007ffffffffffffffff000000000000002b00000001000000000000002b"
	goldenInitProducerIDRequestHex  = "000574786e2d61"
	goldenInitProducerIDResponseHex = "0000000000000000007b0002"
	goldenBeginTxnRequestHex        = "000574786e2d61000000000000007b0002"
	goldenEndTxnRequestHex          = "000574786e2d61000000000000007b000201"
	goldenTransactionResponseHex    = "0000000000000000007b00020009434f4d4d4954544544"
)

func TestProduceRequestGoldenBytes(t *testing.T) {
	writer := codec.NewWriter()
	codec.EncodeProduceRequestBody(writer, sampleProduceRequestBody(t))
	got, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	want := mustDecodeHex(t, goldenProduceRequestHex)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ProduceRequest bytes = %x, want %x", got, want)
	}
}

func TestDecodeProduceResponseGoldenBytes(t *testing.T) {
	got, err := codec.DecodeProduceResponseBody(codec.NewReader(mustDecodeHex(t, goldenProduceResponseHex)))
	if err != nil {
		t.Fatalf("DecodeProduceResponseBody() error = %v", err)
	}
	topic := "orders"
	want := message.ProduceResponseBody{Responses: []message.ProduceTopicResponse{
		{
			Topic: &topic,
			Partitions: []message.ProducePartitionResponse{
				{
					Partition:          0,
					ErrorCode:          protocol.ErrorCodeNone,
					BaseOffset:         42,
					CurrentLeaderEpoch: 7,
					LogAppendTimeMs:    1000,
					LogStartOffset:     5,
				},
			},
		},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeProduceResponseBody() = %+v, want %+v", got, want)
	}
}

func TestFetchRequestGoldenBytes(t *testing.T) {
	writer := codec.NewWriter()
	codec.EncodeFetchRequestBody(writer, message.FetchRequestBody{
		ReplicaID:      -1,
		MaxWaitMs:      500,
		MinBytes:       1,
		MaxBytes:       1048576,
		IsolationLevel: 0,
		SessionID:      0,
		TopicPartitions: []message.FetchTopicRequest{
			{
				Topic: "orders",
				Partitions: []message.FetchPartitionRequest{
					{
						Partition:          0,
						CurrentLeaderEpoch: 7,
						FetchOffset:        42,
						LogStartOffset:     5,
						PartitionMaxBytes:  1048576,
					},
				},
			},
		},
	})
	got, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	want := mustDecodeHex(t, goldenFetchRequestHex)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FetchRequest bytes = %x, want %x", got, want)
	}
}

func TestDecodeFetchResponseGoldenBytes(t *testing.T) {
	got, err := codec.DecodeFetchResponseBody(codec.NewReader(mustDecodeHex(t, goldenFetchResponseHex)))
	if err != nil {
		t.Fatalf("DecodeFetchResponseBody() error = %v", err)
	}
	topic := "orders"
	wantRecords := mustDecodeHex(t, goldenRecordBatchHex)
	want := message.FetchResponseBody{
		SessionID: 0,
		Responses: []message.FetchTopicResponse{
			{
				Topic: &topic,
				Partitions: []message.FetchPartitionResponse{
					{
						Partition:           0,
						ErrorCode:           protocol.ErrorCodeNone,
						HighWatermark:       43,
						LogStartOffset:      5,
						LastStableOffset:    43,
						AbortedTransactions: []message.AbortedTransaction{},
						Records:             wantRecords,
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeFetchResponseBody() = %+v, want %+v", got, want)
	}
	if _, err := codec.DecodeRecordBatchSet(got.Responses[0].Partitions[0].Records); err != nil {
		t.Fatalf("DecodeRecordBatchSet(fetch records) error = %v", err)
	}
}

func TestListOffsetsRequestGoldenBytes(t *testing.T) {
	writer := codec.NewWriter()
	codec.EncodeListOffsetsRequestBody(writer, message.ListOffsetsRequestBody{
		ReplicaID:      -1,
		IsolationLevel: 0,
		Topics: []message.ListOffsetsTopicRequest{
			{
				Topic: "orders",
				Partitions: []message.ListOffsetsPartitionRequest{
					{
						Partition:          0,
						CurrentLeaderEpoch: 7,
						Timestamp:          message.ListOffsetsLatestTimestamp,
						MaxNumOffsets:      1,
					},
				},
			},
		},
	})
	got, err := writer.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	want := mustDecodeHex(t, goldenListRequestHex)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListOffsetsRequest bytes = %x, want %x", got, want)
	}
}

func TestDecodeListOffsetsResponseGoldenBytes(t *testing.T) {
	got, err := codec.DecodeListOffsetsResponseBody(codec.NewReader(mustDecodeHex(t, goldenListResponseHex)))
	if err != nil {
		t.Fatalf("DecodeListOffsetsResponseBody() error = %v", err)
	}
	topic := "orders"
	want := message.ListOffsetsResponseBody{Topics: []message.ListOffsetsTopicResponse{
		{
			Topic: &topic,
			Partitions: []message.ListOffsetsPartitionResponse{
				{
					Partition:   0,
					ErrorCode:   protocol.ErrorCodeNone,
					LeaderEpoch: 7,
					Timestamp:   message.ListOffsetsLatestTimestamp,
					Offset:      43,
					Offsets:     []int64{43},
				},
			},
		},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DecodeListOffsetsResponseBody() = %+v, want %+v", got, want)
	}
}

func TestTransactionGoldenBytes(t *testing.T) {
	transactionalID := "txn-a"
	initWriter := codec.NewWriter()
	codec.EncodeInitProducerIDRequestBody(initWriter, message.InitProducerIDRequestBody{TransactionalID: &transactionalID})
	initBytes, err := initWriter.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	if !reflect.DeepEqual(initBytes, mustDecodeHex(t, goldenInitProducerIDRequestHex)) {
		t.Fatalf("InitProducerIDRequest bytes = %x", initBytes)
	}
	initResponse, err := codec.DecodeInitProducerIDResponseBody(codec.NewReader(mustDecodeHex(t, goldenInitProducerIDResponseHex)))
	if err != nil {
		t.Fatalf("DecodeInitProducerIDResponseBody() error = %v", err)
	}
	if initResponse.ErrorCode != protocol.ErrorCodeNone || initResponse.ProducerID != 123 || initResponse.ProducerEpoch != 2 {
		t.Fatalf("InitProducerID response = %+v", initResponse)
	}

	beginWriter := codec.NewWriter()
	codec.EncodeTransactionRequestBody(beginWriter, protocol.ApiKeyBeginTxn, message.TransactionRequestBody{
		TransactionalID: &transactionalID,
		ProducerID:      123,
		ProducerEpoch:   2,
	})
	beginBytes, err := beginWriter.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	if !reflect.DeepEqual(beginBytes, mustDecodeHex(t, goldenBeginTxnRequestHex)) {
		t.Fatalf("BeginTxnRequest bytes = %x", beginBytes)
	}

	endWriter := codec.NewWriter()
	codec.EncodeTransactionRequestBody(endWriter, protocol.ApiKeyEndTxn, message.TransactionRequestBody{
		TransactionalID: &transactionalID,
		ProducerID:      123,
		ProducerEpoch:   2,
		Commit:          true,
	})
	endBytes, err := endWriter.Bytes()
	if err != nil {
		t.Fatalf("Bytes() error = %v", err)
	}
	if !reflect.DeepEqual(endBytes, mustDecodeHex(t, goldenEndTxnRequestHex)) {
		t.Fatalf("EndTxnRequest bytes = %x", endBytes)
	}

	transactionResponse, err := codec.DecodeTransactionResponseBody(codec.NewReader(mustDecodeHex(t, goldenTransactionResponseHex)))
	if err != nil {
		t.Fatalf("DecodeTransactionResponseBody() error = %v", err)
	}
	state := "COMMITTED"
	want := message.TransactionResponseBody{
		ErrorCode:        protocol.ErrorCodeNone,
		ProducerID:       123,
		ProducerEpoch:    2,
		TransactionState: &state,
	}
	if !reflect.DeepEqual(transactionResponse, want) {
		t.Fatalf("TransactionResponse = %+v, want %+v", transactionResponse, want)
	}
}

func TestDefaultRegistryCoversDataPlaneCodecs(t *testing.T) {
	registry := codec.DefaultRegistry()
	cases := []struct {
		name     string
		apiKey   protocol.ApiKey
		request  codec.RequestBody
		response string
		wantType any
	}{
		{"produce", protocol.ApiKeyProduce, sampleProduceRequestBody(t), goldenProduceResponseHex, message.ProduceResponseBody{}},
		{"fetch", protocol.ApiKeyFetch, sampleFetchRequestBody(), goldenFetchResponseHex, message.FetchResponseBody{}},
		{"list_offsets", protocol.ApiKeyListOffsets, sampleListOffsetsRequestBody(), goldenListResponseHex, message.ListOffsetsResponseBody{}},
		{"init_producer_id", protocol.ApiKeyInitProducerID, sampleInitProducerIDRequestBody(), goldenInitProducerIDResponseHex, message.InitProducerIDResponseBody{}},
		{"begin_txn", protocol.ApiKeyBeginTxn, sampleTransactionRequestBody(false), goldenTransactionResponseHex, message.TransactionResponseBody{}},
		{"end_txn", protocol.ApiKeyEndTxn, sampleTransactionRequestBody(true), goldenTransactionResponseHex, message.TransactionResponseBody{}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := registry.EncodeRequestBody(tt.apiKey, protocol.DefaultAPIVersion, tt.request)
			if err != nil {
				t.Fatalf("EncodeRequestBody() error = %v", err)
			}
			if len(encoded) == 0 {
				t.Fatal("EncodeRequestBody() returned empty bytes")
			}
			body, err := registry.DecodeResponseBody(tt.apiKey, protocol.DefaultAPIVersion, mustDecodeHex(t, tt.response))
			if err != nil {
				t.Fatalf("DecodeResponseBody() error = %v", err)
			}
			if reflect.TypeOf(body) != reflect.TypeOf(tt.wantType) {
				t.Fatalf("DecodeResponseBody() type = %T, want %T", body, tt.wantType)
			}
		})
	}
}

func sampleInitProducerIDRequestBody() message.InitProducerIDRequestBody {
	transactionalID := "txn-a"
	return message.InitProducerIDRequestBody{TransactionalID: &transactionalID}
}

func sampleTransactionRequestBody(commit bool) message.TransactionRequestBody {
	transactionalID := "txn-a"
	return message.TransactionRequestBody{
		TransactionalID: &transactionalID,
		ProducerID:      123,
		ProducerEpoch:   2,
		Commit:          commit,
	}
}

func sampleProduceRequestBody(t *testing.T) message.ProduceRequestBody {
	t.Helper()
	return message.ProduceRequestBody{
		Acks:      -1,
		TimeoutMs: 30000,
		TopicData: []message.ProduceTopicData{
			{
				Topic: "orders",
				Partitions: []message.ProducePartitionData{
					{Partition: 0, Records: mustDecodeHex(t, goldenRecordBatchHex)},
				},
			},
		},
	}
}

func sampleFetchRequestBody() message.FetchRequestBody {
	return message.FetchRequestBody{
		ReplicaID:      -1,
		MaxWaitMs:      500,
		MinBytes:       1,
		MaxBytes:       1048576,
		IsolationLevel: 0,
		SessionID:      0,
		TopicPartitions: []message.FetchTopicRequest{
			{
				Topic: "orders",
				Partitions: []message.FetchPartitionRequest{
					{Partition: 0, CurrentLeaderEpoch: 7, FetchOffset: 42, LogStartOffset: 5, PartitionMaxBytes: 1048576},
				},
			},
		},
	}
}

func sampleListOffsetsRequestBody() message.ListOffsetsRequestBody {
	return message.ListOffsetsRequestBody{
		ReplicaID:      -1,
		IsolationLevel: 0,
		Topics: []message.ListOffsetsTopicRequest{
			{
				Topic: "orders",
				Partitions: []message.ListOffsetsPartitionRequest{
					{Partition: 0, CurrentLeaderEpoch: 7, Timestamp: message.ListOffsetsLatestTimestamp, MaxNumOffsets: 1},
				},
			},
		},
	}
}
