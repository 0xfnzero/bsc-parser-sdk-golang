<div align="center">
  <h1>BSC Parser SDK Go 版</h1>
  <h3><em>并发、低延迟的 BNB Smart Chain 解析基础设施</em></h3>
</div>

<p align="center"><strong>用于解码 BSC calldata、日志并消费服务商中立数据流的 Go SDK，解析热路径不执行 RPC 查询。</strong></p>

<p align="center">
  <a href="./README_CN.md">中文</a> |
  <a href="./README.md">English</a> |
  <a href="https://fnzero.dev/">官网</a> |
  <a href="https://t.me/fnzero_group">Telegram</a> |
  <a href="https://discord.gg/vuazbGkqQE">Discord</a>
</p>

## BSC SDK 家族

| 类别 | Node.js / TypeScript | Go |
| --- | --- | --- |
| 统一交易 | [bsc-trade-sdk-nodejs](https://github.com/0xfnzero/bsc-trade-sdk-nodejs) | [bsc-trade-sdk-golang](https://github.com/0xfnzero/bsc-trade-sdk-golang) |
| 交易解析 | [bsc-parser-sdk-nodejs](https://github.com/0xfnzero/bsc-parser-sdk-nodejs) | **[bsc-parser-sdk-golang](https://github.com/0xfnzero/bsc-parser-sdk-golang)** |
| 协议 SDK | [four-trading-sdk](https://github.com/0xfnzero/four-trading-sdk) | [pancakeswap-sdk](https://github.com/0xfnzero/pancakeswap-sdk) |

- Four.meme TokenManager2 calldata 和事件解析
- PancakeSwap V2 router calldata 解码
- PancakeSwap V2 pair/factory 事件解析
- 解析热路径不做 RPC 查询
- 预编译 selector/topic 分发，适合低延迟消费场景
- 服务商中立的 stream adapter、ordered/streaming 缓冲、micro-batch 和增量 JSON 解码

calldata 和日志可来自调用方自己的 WebSocket、gRPC、区块 relay 或第三方高速流适配器；parser 包不负责网络接入。

`StreamOrderStreaming` 会立即发射连续的交易索引（日志事件使用日志索引），缓存索引缺口，并丢弃重复索引或早于当前区块水位的事件。超时或显式 flush 会将当前水位推进到被强制发射的事件之后。

事件进入 stream buffer 时会复制 `Topics`、`Data`、字节类 `Raw` 值（`[]byte` 和 `json.RawMessage`），以及 JSON-like `Metadata` 值（`map[string]any`、`[]any`、字节切片与 `json.RawMessage`）；其他自定义 `Raw` 和 `Metadata` 值仍保留引用语义。

示例：

```go
manager := bscparser.NewStreamManager(bscparser.StreamManagerConfig{
    OrderMode: bscparser.StreamOrderMicroBatch,
    Sources: []bscparser.StreamSource{
        bscparser.AdapterStreamSource{
            SourceName: "provider-grpc",
            SourceType: bscparser.StreamSourceGRPC,
            Run: func(ctx context.Context, emit func(bscparser.StreamEvent)) error {
                // 在这里读取服务商专有 gRPC/WebSocket/区块流。
                emit(bscparser.StreamEvent{EventType: bscparser.StreamEventLog, BlockNumber: 123})
                return nil
            },
        },
    },
})
```

```go
parser := bscparser.MustParser()
decoded, err := parser.DecodePancakeRouterCalldata(rawCalldata)
if err == nil && decoded.Protocol == bscparser.ProtocolPancake {
    fmt.Println(decoded.Signature, decoded.Args["path"])
}
```
