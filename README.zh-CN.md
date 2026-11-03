# Flare Core

[English](README.md) · 中文

> ## ℹ 这是通信基础设施，不是开箱即用的 IM 产品
>
> 说在前面，免得你 clone 完才发现登不上去：**开源部分不含账号体系**
> （没有注册登录、好友关系、群角色/审批/禁言、朋友圈）。
>
> 但它自带完整且可插拔的鉴权契约，两条路都在开源侧：
>
> - **`CoreJwtTokenValidator`** —— 本地验 JWT。手签一个 token 就能跑起来做
>   demo / POC，**不需要任何用户体系**。
> - **`HttpHookTokenValidator`** —— 把 token POST 到你自己的接口，
>   **这是接入自有用户体系的入口**。
>
> 业务规则同理：`flare-core-server/crates/flare-im-hooks` 提供 9 个扩展点
> （PreSend / PostSend / Delivery / Recall / MessageRead / MessageReaction /
> ConversationLifecycle / ConversationMember / GetConversationParticipants）。
>
> 要上生产，你需要自行实现用户体系并按上述契约接入 —— 与 Sendbird /
> Twilio Conversations 的「自带身份」模型一致，区别是 Flare 可自托管、
> 协议与核心可审计。
>
> 边界详情见 [GOVERNANCE.md](.github/GOVERNANCE.md)。


[![Crates.io](https://img.shields.io/crates/v/flare-core.svg)](https://crates.io/crates/flare-core)
[![Documentation](https://docs.rs/flare-core/badge.svg)](https://docs.rs/flare-core)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.94%2B-orange.svg)](https://www.rust-lang.org/)
[![GitHub](https://img.shields.io/github/stars/flare-im/flare-core?style=social&label=GitHub)](https://github.com/flare-im/flare-transport)

[![WebSocket](https://img.shields.io/badge/WebSocket-ws%2Fwss-4CAF50)](https://github.com/flare-im/flare-transport)
[![QUIC](https://img.shields.io/badge/QUIC-UDP%2FTLS-2196F3)](https://github.com/flare-im/flare-transport)
[![TCP](https://img.shields.io/badge/TCP-length--prefixed-607D8B)](https://github.com/flare-im/flare-transport)
[![Tokio](https://img.shields.io/badge/Tokio-async-000000?logo=rust&logoColor=white)](https://tokio.rs/)
[![Protobuf](https://img.shields.io/badge/Protobuf-negotiation-9C27B0)](https://github.com/flare-im/flare-transport)
[![WASM](https://img.shields.io/badge/WASM-web%20client-FF9800)](https://github.com/flare-im/flare-transport)

`flare-core` 是一个面向生产的 Rust 长连接工具库。它为即时通讯网关、聊天室、
推送通道、协作工具、低延迟应用后端等实时系统提供传输层基座。

该 crate 聚焦传输层关注点：WebSocket、QUIC、TCP、连接协商、心跳、重连、
序列化、压缩、加密以及可扩展的消息管道。诸如序列号分配、收件箱同步、
推送策略、业务规则等 IM 产品语义，应当放在更上层的 crate 或服务中。

API 文档：[docs.rs/flare-core](https://docs.rs/flare-core)

## 亮点

- **传输**：WebSocket、QUIC、可选 TCP，以及原生协议竞速。
- **协商**：面向格式、压缩、加密对齐的 CONNECT / CONNECT_ACK / NEGOTIATION_READY 流程。
- **编解码**：Protobuf 与 JSON，序列化器可插拔。
- **可靠性基础**：心跳策略、主动探测、重连钩子、连接快照，以及慢消费者隔离。
- **安全钩子**：token 鉴权、TLS 支持、证书固定，以及启用后的 AES-256-GCM 加密。
- **运行时目标**：原生 Tokio 应用与 wasm32 WebSocket 客户端。
- **扩展点**：自定义序列化器、压缩器、加密器、中间件、观察者，以及服务端事件处理器。

## 安装

```toml
[dependencies]
flare-core = "1.1"
```

仅服务端网关：

```toml
flare-core = { version = "1.1", default-features = false, features = [
    "server",
    "websocket",
    "quic",
    "compression-gzip",
    "encryption-aes-gcm",
] }
```

原生客户端：

```toml
flare-core = { version = "1.1", default-features = false, features = [
    "client",
    "websocket",
    "quic",
    "compression-gzip",
    "encryption-aes-gcm",
] }
```

TCP 与完整特性集：

```toml
flare-core = { version = "1.1", features = ["tcp"] }
flare-core = { version = "1.1", features = ["full"] }
```

WASM WebSocket 客户端：

```toml
flare-core = { version = "1.1", default-features = false, features = ["wasm"] }
```

```bash
cargo build --target wasm32-unknown-unknown --no-default-features --features wasm
```

## 特性开关（Feature Flags）

| Feature | Default | Description |
|---------|:-------:|-------------|
| `client` | yes | 客户端构建器、传输、协商、重连与发送 API。 |
| `server` | yes | 原生服务端构建器、连接管理与事件处理。 |
| `websocket` | yes | WebSocket 传输。 |
| `quic` | yes | 原生 QUIC 传输。 |
| `tcp` | no | 采用定长前缀帧的 TCP 传输。 |
| `wasm` | no | wasm32 WebSocket 客户端栈。 |
| `compression-gzip` | yes | Gzip 压缩支持。 |
| `encryption-aes-gcm` | yes | AES-256-GCM 加密支持。 |
| `full` | no | 默认能力加上 TCP。 |

运行时可用 `flare_core::common::FeatureSet::current()` 查看已编译的能力集合。

## 架构

```text
Application   ServerEventHandler | MessageListener | Authenticator
      |
Core          ServerCore | ClientCore | ConnectionManager | MessagePipeline
      |
Transport     HybridServer | HybridClient | WebSocket | QUIC | TCP
```

连接生命周期：

1. 建立传输连接。
2. 发送 CONNECT 元数据，用于序列化、压缩、加密与鉴权。
3. 接收 CONNECT_ACK 并对齐双方的解析器配置。
4. 发出 NEGOTIATION_READY 并开始心跳处理。
5. 交换应用帧。
6. 断开、重连或清理连接状态。

构建器家族：

| Mode | Builder | Integration style | Typical use |
|------|---------|-------------------|-------------|
| Simple | `ServerBuilder` / `ClientBuilder` | closures | 原型与小型 demo |
| Observer | `Observer*Builder` | observer traits | 感知连接的集成 |
| Flare | `FlareServerBuilder` / `FlareClientBuilder` | traits and pipeline | 面向生产的集成 |

## 快速开始

最小的 Flare 模式服务端。样例需要的全部依赖：

```toml
[dependencies]
flare-core = "1.1"
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
```

```rust
use async_trait::async_trait;
use flare_core::common::error::Result;
use flare_core::common::protocol::{Frame, PayloadCommand};
use flare_core::server::events::handler::ServerEventHandler;
use flare_core::server::FlareServerBuilder;
use std::sync::Arc;

struct Handler;

#[async_trait]
impl ServerEventHandler for Handler {
    async fn handle_message(
        &self,
        _command: &PayloadCommand,
        _connection_id: &str,
    ) -> Result<Option<Frame>> {
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let server = FlareServerBuilder::new("0.0.0.0:8080", Arc::new(Handler)).build()?;

    // start() 启动监听后即返回，不阻塞；进程需要自己保持存活。
    server.start().await?;
    tokio::signal::ctrl_c().await.expect("等待 Ctrl-C");
    server.stop().await
}
```

从仓库检出运行聊天示例：

```bash
RUST_LOG=info cargo run --example flare_chat_server
RUST_LOG=info cargo run --example flare_chat_client -- user1
```

TCP 示例：

```bash
cargo run --example flare_chat_server --features tcp
RUST_LOG=info cargo run --example tcp_client --features tcp
```

更多示例见仓库文档：
[examples/README.md](https://github.com/flare-im/flare-transport/blob/main/examples/README.md)。

## 原生与 WASM 支持

| Capability | Native | WASM |
|------------|:------:|:----:|
| WebSocket client | yes | yes |
| QUIC client | yes | no |
| TCP client | yes, with `tcp` | no |
| Native protocol racing | yes | no |
| `FlareClientBuilder` | yes | yes, WebSocket only |
| Hybrid server / QUIC server | yes | no |
| Negotiated heartbeat | yes | yes |

浏览器 demo 见
[examples/wasm_websocket_client](https://github.com/flare-im/flare-transport/tree/main/examples/wasm_websocket_client)。

## 验证

仓库的验证脚本会运行格式化、静态检查、原生测试、特性矩阵检查、wasm 检查
以及示例构建：

```bash
./scripts/verify.sh
```

若只需聚焦的发布前检查：

```bash
cargo test --lib --tests --examples --all-features
cargo doc --no-deps --all-features
cargo publish --dry-run
```

注意：底层模块注释中的历史 doctest 片段目前尚未作为发布门禁。公开 README 与
crate 级文档为 crates.io 和 docs.rs 保持英文。

## 性能基线

当前基线覆盖帧编码、消息解析、管道处理、连接生命周期，以及内存内扇出。它不建模
更上层的 IM 语义，例如序列号分配、同步、收件箱存储或推送投递。

已发布基线的测试环境：

| Item | Value |
|------|-------|
| CPU | Apple M1 Pro, 10 cores |
| Memory | 16 GiB |
| OS | macOS Darwin 25.3.0 |
| Rust | 1.94.1 |
| Build | release mode, single-process benchmark |

汇总：

| Benchmark | Throughput |
|-----------|-----------:|
| Protobuf 256B round-trip | 1,017,824 ops/s |
| JSON 256B round-trip | 197,954 ops/s |
| Protobuf + Gzip 1KB round-trip | 51,015 ops/s |
| Pipeline parse + validation | 1,405,371 ops/s |
| Connection add + active + remove | 1,457,953 ops/s |
| Broadcast 1,000 x 256B bytes | ~4,188 broadcasts/s |
| Broadcast 1,000 x 256B frame | ~2,789 broadcasts/s |
| Timeout cleanup, 1,000 connections | ~0.727 ms/op |

完整报告：
[docs/performance-baseline.md](https://github.com/flare-im/flare-transport/blob/main/docs/performance-baseline.md)。

## 文档

| Resource | Link |
|----------|------|
| API reference | [docs.rs/flare-core](https://docs.rs/flare-core) |
| Examples | [examples/README.md](https://github.com/flare-im/flare-transport/blob/main/examples/README.md) |
| Performance report | [docs/performance-baseline.md](https://github.com/flare-im/flare-transport/blob/main/docs/performance-baseline.md) |
| Issues | [GitHub Issues](https://github.com/flare-im/flare-transport/issues) |

## 许可证

依据 [Apache License 2.0](LICENSE) 授权。

---

## 下一步

| 想做什么 | 去哪里 |
|---|---|
| **五分钟跑起来** | [QUICKSTART](https://github.com/flare-im/flare-core-server/blob/main/QUICKSTART.md) —— 起服务、手签 token、调通接口，**不需要自建用户体系** |
| 接入自己的用户系统 | 实现 `TokenValidator`（`CoreJwtTokenValidator` 本地验签 / `HttpHookTokenValidator` 调你的接口） |
| 加自己的业务规则 | `flare-im-hooks` 的 9 个扩展点：PreSend / PostSend / Delivery / Recall / MessageRead / MessageReaction / ConversationLifecycle / ConversationMember / GetConversationParticipants |
| 做界面 | [`@flare-im/vue-ui`](https://www.npmjs.com/package/@flare-im/vue-ui) —— 107 个组件，四端一致的契约 |
| 报安全问题 | [SECURITY.md](.github/SECURITY.md)，**请勿开公开 issue** |

## 需要账号体系与社交能力时

开源部分是**通信基础设施**。如果你需要的是现成的账号、好友关系、群治理（角色 / 入群审批 / 禁言）、朋友圈，
这些在商业模块里 —— 自研这一层通常要数月，且都是与通信无关的重复劳动。

企业场景另有 SSO / 组织架构 / 审计导出 / 数据驻留 / SLA 支持。

咨询：`flare1522@163.com`

> 边界划分与不变承诺见 [GOVERNANCE](https://github.com/flare-im/flare-core-server/blob/main/.github/GOVERNANCE.md)。
> 简言之：**已开源的不会被收回，鉴权与 hooks 契约永远开源、不会为逼迫付费而阉割。**
