# stellflow-go-sdk

`stellflow-go-sdk` 是 [Stellflow Service](https://github.com/stellhub/stellflow-service) 的 Go 客户端实现，用于对接 Stellflow Broker 数据面的自定义二进制协议，并为 Go 应用提供 Producer、Consumer、Admin 与协议编解码能力。

当前仓库处于 Go SDK 起步阶段。README 先固化目标边界、协议依据、包结构和实现顺序，后续代码应以这里的约定为准，并与 [stellflow-java-sdk](https://github.com/stellhub/stellflow-java-sdk) 的客户端语义保持跨语言一致。

## 项目定位

Stellflow 保留 Kafka 风格的 Topic / Partition / Replica / ISR / Offset / Consumer Group 语义，但 Broker / Client 通信使用 Stellflow 自定义二进制协议，而不是 Kafka 原生协议。

Go SDK 的目标不是简单移植 Java SDK 的内部实现，而是在 Go 语言习惯下实现同一套协议契约：

- `producer`：消息批量发送、分区路由、acks、超时、重试与后续幂等能力。
- `consumer`：元数据发现、Fetch 长轮询、位点管理、消费组协调与位点提交。
- `admin`：API 版本探测、集群描述、Topic 描述、offset 查询与后续管理接口。
- `protocol`：协议头、请求体、响应体、错误码、API Key、RecordBatch 与兼容性测试基线。
- `transport`：基于 TCP 长连接的数据面网络层，负责帧编解码、请求关联、连接池与重连。

## 设计参考

本仓库实现时需要同时参考服务端协议与 Java 客户端设计：

| 来源 | 用途 |
| --- | --- |
| [stellflow-service](https://github.com/stellhub/stellflow-service) | Broker 数据面协议、API Key、请求响应字段、服务端行为语义 |
| [stellflow-java-sdk](https://github.com/stellhub/stellflow-java-sdk) | 客户端分层、Producer / Consumer / Admin API、Metadata 路由、重试与可观测性模型 |

本地开发时推荐直接对照：

- `E:\PersonalCode\JavaProject\stellflow-service`
- `E:\PersonalCode\JavaProject\stellflow-java-sdk`

## 协议基线

Go SDK 必须与服务端数据面协议保持一致：

- 数据面使用 TCP 长连接。
- 线上帧格式为 `frameLength + header + body`。
- 多字节整数统一使用大端序，也就是 network byte order。
- `headerVersion` 当前正式基线为 `2`。
- 核心 API 当前以 `apiVersion = 0` 作为第一版实现目标。
- 客户端连接 Broker 后应先发送 `ApiVersions` 完成能力协商，再发送 `Metadata / Produce / Fetch` 等业务请求。
- 配置和文档可使用 `stellflow://127.0.0.1:9092` 表达 endpoint，但真实 TCP 二进制协议不携带该文本前缀。

请求头字段顺序必须固定：

```text
apiKey
-> apiVersion
-> headerVersion
-> correlationId
-> clientId
-> traceId
-> spanId
-> traceFlags
-> tenantId
-> quotaKey
-> authContextId
-> trafficClass
-> trafficTag
-> flags
```

响应头字段顺序必须固定：

```text
correlationId
-> headerVersion
-> errorCode
-> throttleTimeMs
```

## API Key 范围

首期 Go SDK 应优先覆盖服务端当前核心数据面 API：

| apiKey | 名称 | Go SDK 目标 |
| --- | --- | --- |
| `0` | `ApiVersions` | 能力协商 |
| `1` | `Metadata` | Broker / Topic / Partition 路由发现 |
| `2` | `Produce` | Producer 写入消息批 |
| `3` | `Fetch` | Consumer 拉取消息批 |
| `4` | `ListOffsets` | earliest / latest / timestamp offset 查询 |
| `5` | `OffsetCommit` | 提交消费组位点 |
| `6` | `OffsetFetch` | 查询消费组位点 |
| `7` | `FindCoordinator` | 查找消费组协调器 |
| `8` | `Heartbeat` | 消费组心跳 |
| `9` | `JoinGroup` | 加入消费组 |
| `10` | `SyncGroup` | 同步分区分配 |

服务端还预留了以下能力，Go SDK 可在核心链路稳定后扩展：

| apiKey | 名称 |
| --- | --- |
| `11` | `InitProducerId` |
| `12` | `BeginTransaction` |
| `13` | `EndTransaction` |
| `50` | `CreateTopic` |
| `51` | `DeleteTopic` |
| `52` | `AlterPartition` |
| `53` | `DescribeCluster` |
| `54` | `HealthCheck` |
| `55` | `DecommissionBroker` |

## 客户端分层

建议按以下层次实现：

```text
public api
  -> producer / consumer / admin
  -> metadata manager
  -> protocol client
  -> connection pool
  -> frame codec
  -> tcp transport
```

### Public API

Public API 层面向业务代码，隐藏二进制协议细节：

- Producer 暴露同步或异步 `Send` 能力，并返回 `RecordMetadata`。
- Consumer 暴露 `Subscribe`、`Assign`、`Poll`、`Commit`、`Seek` 等基础能力。
- Admin 暴露 `APIVersions`、`DescribeCluster`、`DescribeTopics`、`ListOffsets` 等接口。
- Public API 不直接暴露 `frameLength`、`headerVersion`、`apiKey` 数字常量和底层 buffer。

### Metadata

Metadata 是客户端路由事实来源：

- `bootstrap.servers` 只用于初次连接。
- 启动后先请求 `ApiVersions`，再请求 `Metadata`。
- Producer 根据 topic / partition leader 路由 Produce。
- Consumer 根据 partition leader 路由 Fetch。
- 收到 `NOT_LEADER_OR_FOLLOWER`、`LEADER_NOT_AVAILABLE`、`BROKER_NOT_AVAILABLE`、`UNSUPPORTED_VERSION` 等错误时刷新 metadata 或能力缓存。

Metadata 缓存至少需要包含：

- `clusterId`
- broker id 到 endpoint 的映射
- topic / partition 到 leader broker 的映射
- partition leader epoch、replicas、ISR
- broker endpoint 到 API version 范围的能力缓存

### Protocol Client

Protocol Client 是统一请求执行器：

- 构造 `RequestHeader`。
- 按 `apiKey + apiVersion` 查找 body codec。
- 编码 `frameLength + header + body`。
- 维护 in-flight 请求表。
- 解码响应头并校验 `correlationId`。
- 将请求级和分区级 `errorCode` 映射为 Go error。

### Transport

Go SDK 底层应使用 Go 标准库网络能力实现 TCP 长连接：

- 每个 Broker endpoint 维护可复用连接。
- 一个连接允许多个 in-flight 请求，通过 `correlationId` 关联响应。
- 请求必须支持 `context.Context` 取消、超时和链路透传。
- 连接断开、读写超时、Broker 不可用需要反馈给上层重试策略。
- 响应可能乱序返回，客户端不能假设先发先回。

## Producer 设计要点

Producer 以 RecordBatch 为一等传输单位：

- 单条消息进入 accumulator 后按 topic / partition 聚合。
- `ProduceRequestBody.records` 传输连续 `RecordBatch` 原始字节，不使用 JSON。
- 普通非幂等写入可使用 `producerId = -1`、`producerEpoch = -1`、`baseSequence = -1`。
- 未指定 partition 时，先通过 Metadata 获取分区列表，再按分区器选择分区。
- 默认分区策略建议与 Java SDK 对齐：key 非空时使用 key hash，key 为空时按 topic round-robin。
- `acks` 支持 `0`、`1`、`-1`。
- 分区级返回以 `ProducePartitionResponse.errorCode` 为准，同一请求可能部分成功、部分失败。

## Consumer 设计要点

Consumer 主链路：

1. 通过 `Metadata` 获取 topic / partition leader。
2. 通过 `ListOffsets` 解析 earliest、latest 或 timestamp 起始位点。
3. 通过 `Fetch` 按分区批量拉取 RecordBatchSet。
4. 解码 batch，并根据 high watermark 或 last stable offset 控制可见性。
5. 使用 `OffsetCommit` / `OffsetFetch` 管理消费组位点。

消费组协调基础流程：

1. `FindCoordinator`
2. `JoinGroup`
3. `SyncGroup`
4. 周期性 `Heartbeat`
5. `OffsetCommit` / `OffsetFetch`

Go Consumer 应优先支持：

- `Subscribe(ctx, topics)`：加入消费组并启动心跳。
- `Assign(partitions)`：手动分配分区，不加入消费组。
- `Poll(ctx)`：按当前 assignment 拉取消息。
- `Commit(ctx)`：提交已消费到的 offset，也就是最后一条已返回消息的 `offset + 1`。

## Admin 设计要点

AdminClient 复用协议网络层、连接池和 Metadata 路由缓存。首期建议实现：

- `APIVersions(ctx)`：查询 Broker 支持的 API 版本范围。
- `Metadata(ctx, topics)`：查询原始 Metadata 响应。
- `DescribeCluster(ctx)`：返回 cluster id、controller broker 与 broker 列表。
- `DescribeTopics(ctx, topics)`：返回 topic、partition、leader、replica、ISR 信息。
- `ListOffsets(ctx, requests)`：支持 earliest、latest 和 timestamp offset 查询。

Topic 创建、删除、分区调整、Broker 健康检查和 Broker 下线等管理类 API 应在对应请求 / 响应 codec 完成后再暴露，不要先提供伪接口。

## 包结构建议

```text
.
├── admin
├── consumer
├── internal
│   ├── metadata
│   ├── retry
│   └── transport
├── protocol
│   ├── codec
│   └── message
├── producer
└── stellflow
```

约定：

- `protocol` 包只表达 wire protocol，不依赖 Producer、Consumer 或 Admin 业务对象。
- `internal/transport` 负责 TCP、frame、连接池和 in-flight 请求。
- `internal/metadata` 负责 broker / partition 路由缓存。
- `producer`、`consumer`、`admin` 暴露面向业务的类型。
- 顶层 `stellflow` 包提供 `ClientFactory`、`Options`、公共错误和便捷入口。

## 目标用法

下面是 Go SDK 稳定后的目标 API 形态。实现过程中可以微调命名，但应保持 `context.Context` 贯穿请求链路。

### Producer

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/stellhub/stellflow-go-sdk/producer"
	"github.com/stellhub/stellflow-go-sdk/stellflow"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	factory, err := stellflow.NewClientFactory(stellflow.Options{
		BootstrapServers: []string{"stellflow://127.0.0.1:9092"},
		ClientID:         "orders-service",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer factory.Close()

	p, err := factory.NewProducer()
	if err != nil {
		log.Fatal(err)
	}

	metadata, err := p.Send(ctx, producer.Record{
		Topic: "orders.created",
		Key:   []byte("order-1001"),
		Value: []byte(`{"amount":199}`),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("sent topic=%s partition=%d offset=%d", metadata.Topic, metadata.Partition, metadata.Offset)
}
```

### Consumer

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/stellhub/stellflow-go-sdk/consumer"
	"github.com/stellhub/stellflow-go-sdk/stellflow"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory, err := stellflow.NewClientFactory(stellflow.Options{
		BootstrapServers: []string{"stellflow://127.0.0.1:9092"},
		ClientID:         "orders-worker",
		Consumer: consumer.Options{
			GroupID: "orders-worker-group",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer factory.Close()

	c, err := factory.NewConsumer()
	if err != nil {
		log.Fatal(err)
	}

	if err := c.Subscribe(ctx, []string{"orders.created"}); err != nil {
		log.Fatal(err)
	}

	pollCtx, cancelPoll := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPoll()

	records, err := c.Poll(pollCtx)
	if err != nil {
		log.Fatal(err)
	}
	for _, record := range records {
		log.Printf("received topic=%s partition=%d offset=%d value=%s", record.Topic, record.Partition, record.Offset, string(record.Value))
	}

	if err := c.Commit(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## 错误处理

Stellflow 协议有两层错误：

- 响应头 `errorCode`：请求级错误，例如非法请求、版本不支持、认证失败。
- 响应体内分区级 `errorCode`：局部错误，例如非 leader、未知分区、offset 越界。

Go SDK 应提供可判定的错误类型或辅助函数，例如：

- `IsRetriable(err error) bool`
- `IsUnsupportedVersion(err error) bool`
- `IsUnknownTopicOrPartition(err error) bool`
- `IsOffsetOutOfRange(err error) bool`
- `IsNotLeaderOrFollower(err error) bool`

重试策略必须区分可重试和不可重试错误。网络断开、超时、`BROKER_NOT_AVAILABLE`、`LEADER_NOT_AVAILABLE`、`NOT_LEADER_OR_FOLLOWER` 可进入重试；`MESSAGE_TOO_LARGE`、`INVALID_RECORD`、`AUTHORIZATION_FAILED` 不应盲目重试。

## 可观测性

SDK core 不依赖具体框架。Go 侧建议通过标准库日志、OpenTelemetry API 和可注入 hook 暴露观测能力：

- 协议请求总数、错误数、耗时和 in-flight 数量。
- TCP 连接数、重连次数、读写超时。
- Producer 成功写入 record 数。
- Consumer 成功拉取 record 数。
- OffsetCommit 成功次数。
- Join / Sync / Heartbeat 操作次数。

`clientId`、`tenantId`、`trafficTag` 等字段可能是高基数字段，默认不要作为全局指标标签直接暴露。

## 实现顺序

建议按以下顺序落地：

1. `protocol` 基础包：`ApiKey`、`ErrorCode`、header、基础 serde、codec registry。
2. `protocol/message`：ApiVersions、Metadata、Produce、Fetch、ListOffsets 请求和响应结构。
3. `protocol/codec`：基础类型大端序编解码、RecordBatch、请求体和响应体 codec。
4. `internal/transport`：frame encoder / decoder、TCP connection、in-flight request table。
5. `internal/metadata`：ApiVersions 协商、Metadata 刷新、partition leader 路由缓存。
6. `producer`：RecordBatchSet 聚合、分区选择、ProduceResponse 解析和重试。
7. `consumer`：ListOffsets、Fetch、RecordBatch 解码、手动 assign 消费。
8. `consumer` group：FindCoordinator、JoinGroup、SyncGroup、Heartbeat、OffsetCommit、OffsetFetch。
9. `admin`：APIVersions、DescribeCluster、DescribeTopics、ListOffsets。
10. 可观测性、兼容性测试和跨语言 golden file 测试。

## 开发要求

- Go 版本以 `go.mod` 为准。
- 所有 Go 代码必须通过 `gofmt`。
- 优先传递 `context.Context`，避免在底层吞掉取消和超时。
- 避免不必要的 interface，先让具体类型稳定。
- 协议代码必须有单元测试和跨语言样例对齐测试。
- 修改协议字段时必须同步更新 README、协议测试和服务端协议文档引用。

## 本地联调

启动服务端 Broker 后，默认数据面 endpoint 为：

```text
stellflow://127.0.0.1:9092
```

首个 Go SDK 冒烟测试建议从 `ApiVersions` 开始：

```powershell
go test ./...
```

在 `ApiVersions`、`Metadata` 和基础 codec 完成后，再推进 Producer / Consumer 与服务端的端到端测试。
