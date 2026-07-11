<div align="center">
  <h1>BSC Parser SDK for Go</h1>
  <h3><em>Concurrent, low-latency BNB Smart Chain parsing infrastructure</em></h3>
</div>

<p align="center"><strong>A Go SDK for decoding BSC calldata and logs and consuming provider-neutral streams without RPC reads in parser hot paths.</strong></p>

<p align="center">
  <a href="./README.md">English</a> |
  <a href="./README_CN.md">中文</a> |
  <a href="https://fnzero.dev/">Website</a> |
  <a href="https://t.me/fnzero_group">Telegram</a> |
  <a href="https://discord.gg/vuazbGkqQE">Discord</a>
</p>

## BSC SDK Family

| Category | Primary repository | Alternative or companion |
| --- | --- | --- |
| Unified trading | [bsc-trade-sdk-nodejs](https://github.com/0xfnzero/bsc-trade-sdk-nodejs) | [bsc-trade-sdk-golang](https://github.com/0xfnzero/bsc-trade-sdk-golang) |
| Transaction parsing | [bsc-parser-sdk-nodejs](https://github.com/0xfnzero/bsc-parser-sdk-nodejs) | **[bsc-parser-sdk-golang](https://github.com/0xfnzero/bsc-parser-sdk-golang)** |
| Protocol SDKs | [four-trading-sdk](https://github.com/0xfnzero/four-trading-sdk) | [pancakeswap-sdk](https://github.com/0xfnzero/pancakeswap-sdk) |

## Installation

```bash
go get github.com/0xfnzero/bsc-parser-sdk-golang
```

Import the parser package as `github.com/0xfnzero/bsc-parser-sdk-golang/bscparser`. Go 1.24 or newer is required.

## Quick Start

Features:

- Four.meme TokenManager2 calldata decoding and event parsing
- PancakeSwap V2 router calldata decoding
- PancakeSwap V2 pair/factory event parsing
- No RPC calls in parser hot paths
- Precompiled selector/topic dispatch suitable for low-latency consumers
- Provider-neutral stream adapters, ordered/streaming buffers, micro-batching, and incremental JSON decoding

Feed calldata and logs from your own WebSocket, gRPC, block relay, or third-party low-latency stream adapter. The parser package deliberately does not own network ingestion.

`StreamOrderStreaming` emits contiguous transaction indexes immediately (log indexes for log events), buffers gaps, and drops duplicate indexes or events from older blocks. A timeout or explicit flush advances the current watermark past any forced-out events.

Stream buffers snapshot `Topics`, `Data`, byte-like `Raw` values (`[]byte` and `json.RawMessage`), and JSON-like `Metadata` values (`map[string]any`, `[]any`, byte slices, and `json.RawMessage`) when an event is pushed. Other custom `Raw` and `Metadata` values retain reference semantics.

Example:

```go
manager := bscparser.NewStreamManager(bscparser.StreamManagerConfig{
    OrderMode: bscparser.StreamOrderMicroBatch,
    Sources: []bscparser.StreamSource{
        bscparser.AdapterStreamSource{
            SourceName: "provider-grpc",
            SourceType: bscparser.StreamSourceGRPC,
            Run: func(ctx context.Context, emit func(bscparser.StreamEvent)) error {
                // Read from provider-specific gRPC/WebSocket/block feed here.
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

## Development

```bash
go test -race ./...
go vet ./...
```

## Security Boundary

The parser performs no signing or transaction submission. Validate source authenticity, contract addresses, and decoded intent before using stream events to trigger trades.

Licensed under the [MIT License](./LICENSE).
