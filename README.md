# Flare Transport

English · [Simplified Chinese](README.zh-CN.md)

> ## ℹ This is communication infrastructure, not a ready-to-use IM product
>
> Up front, so you don't discover after cloning that you can't log in:
> **the open-source part does not include an account system** (no
> registration/login, friend relationships, group roles/approval/muting, or
> moments/feed).
>
> It does ship a complete, pluggable authentication contract, and both paths
> live on the open-source side:
>
> - **`CoreJwtTokenValidator`** — validates JWTs locally. Hand-sign a token and
>   you can run a demo / POC, **without any user system**.
> - **`HttpHookTokenValidator`** — POSTs the token to your own endpoint. **This
>   is the entry point for integrating your own user system.**
>
> Business rules work the same way: `flare-core-server/crates/flare-im-hooks`
> provides 9 extension points (PreSend / PostSend / Delivery / Recall /
> MessageRead / MessageReaction / ConversationLifecycle / ConversationMember /
> GetConversationParticipants).
>
> To go to production, you implement your own user system and wire it in via
> the contracts above — the same "bring your own identity" model as Sendbird /
> Twilio Conversations, the difference being that Flare can be self-hosted and
> its protocol and core are auditable.
>
> See [GOVERNANCE.md](.github/GOVERNANCE.md) for the boundary details.


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

`flare-core` is a production-oriented long-connection toolkit for Rust.
It provides the transport foundation for realtime systems such as instant
messaging gateways, chat rooms, push channels, collaboration tools, and
low-latency application backends.

The crate focuses on transport-level concerns: WebSocket, QUIC, TCP,
connection negotiation, heartbeats, reconnection, serialization,
compression, encryption, and extensible message pipelines. IM product
semantics such as sequence allocation, inbox sync, push policy, and business
rules should live in higher-level crates or services.

API documentation: [docs.rs/flare-core](https://docs.rs/flare-core)

## Highlights

- **Transports**: WebSocket, QUIC, optional TCP, and native protocol racing.
- **Negotiation**: CONNECT / CONNECT_ACK / NEGOTIATION_READY flow for format,
  compression, and encryption alignment.
- **Codecs**: Protobuf and JSON with pluggable serializers.
- **Reliability basics**: heartbeat policy, active detection, reconnect hooks,
  connection snapshots, and slow-consumer isolation.
- **Security hooks**: token authentication, TLS support, certificate pinning,
  and AES-256-GCM encryption when enabled.
- **Runtime targets**: native Tokio applications and wasm32 WebSocket clients.
- **Extension points**: custom serializers, compressors, encryptors,
  middleware, observers, and server event handlers.

## Installation

```toml
[dependencies]
flare-core = "1.1"
```

Server-only gateway:

```toml
flare-core = { version = "1.1", default-features = false, features = [
    "server",
    "websocket",
    "quic",
    "compression-gzip",
    "encryption-aes-gcm",
] }
```

Native client:

```toml
flare-core = { version = "1.1", default-features = false, features = [
    "client",
    "websocket",
    "quic",
    "compression-gzip",
    "encryption-aes-gcm",
] }
```

TCP and full feature sets:

```toml
flare-core = { version = "1.1", features = ["tcp"] }
flare-core = { version = "1.1", features = ["full"] }
```

WASM WebSocket client:

```toml
flare-core = { version = "1.1", default-features = false, features = ["wasm"] }
```

```bash
cargo build --target wasm32-unknown-unknown --no-default-features --features wasm
```

## Feature Flags

| Feature | Default | Description |
|---------|:-------:|-------------|
| `client` | yes | Client builders, transports, negotiation, reconnect, and send APIs. |
| `server` | yes | Native server builders, connection management, and event handling. |
| `websocket` | yes | WebSocket transport. |
| `quic` | yes | Native QUIC transport. |
| `tcp` | no | TCP transport with length-prefixed frames. |
| `wasm` | no | wasm32 WebSocket client stack. |
| `compression-gzip` | yes | Gzip compression support. |
| `encryption-aes-gcm` | yes | AES-256-GCM encryption support. |
| `full` | no | Default capabilities plus TCP. |

At runtime, use `flare_core::common::FeatureSet::current()` to inspect the
compiled capability set.

## Architecture

```text
Application   ServerEventHandler | MessageListener | Authenticator
      |
Core          ServerCore | ClientCore | ConnectionManager | MessagePipeline
      |
Transport     HybridServer | HybridClient | WebSocket | QUIC | TCP
```

Connection lifecycle:

1. Establish a transport connection.
2. Send CONNECT metadata for serialization, compression, encryption, and
   authentication.
3. Receive CONNECT_ACK and align both parser profiles.
4. Emit NEGOTIATION_READY and start heartbeat processing.
5. Exchange application frames.
6. Disconnect, reconnect, or clean up connection state.

Builder families:

| Mode | Builder | Integration style | Typical use |
|------|---------|-------------------|-------------|
| Simple | `ServerBuilder` / `ClientBuilder` | closures | prototypes and small demos |
| Observer | `Observer*Builder` | observer traits | connection-aware integrations |
| Flare | `FlareServerBuilder` / `FlareClientBuilder` | traits and pipeline | production-facing integrations |

## Quick Start

Minimal Flare-mode server. Everything the sample needs:

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

    // `start` returns once the listener is up; it does not block.
    // The process has to keep itself alive.
    server.start().await?;
    tokio::signal::ctrl_c().await.expect("listen for ctrl-c");
    server.stop().await
}
```

Run the chat examples from the repository checkout:

```bash
RUST_LOG=info cargo run --example flare_chat_server
RUST_LOG=info cargo run --example flare_chat_client -- user1
```

TCP example:

```bash
cargo run --example flare_chat_server --features tcp
RUST_LOG=info cargo run --example tcp_client --features tcp
```

More examples are documented in the repository:
[examples/README.md](https://github.com/flare-im/flare-transport/blob/main/examples/README.md).

## Native And WASM Support

| Capability | Native | WASM |
|------------|:------:|:----:|
| WebSocket client | yes | yes |
| QUIC client | yes | no |
| TCP client | yes, with `tcp` | no |
| Native protocol racing | yes | no |
| `FlareClientBuilder` | yes | yes, WebSocket only |
| Hybrid server / QUIC server | yes | no |
| Negotiated heartbeat | yes | yes |

For browser demos, see
[examples/wasm_websocket_client](https://github.com/flare-im/flare-transport/tree/main/examples/wasm_websocket_client).

## Verification

The repository verification script runs formatting, linting, native tests,
feature matrix checks, wasm checks, and example builds:

```bash
./scripts/verify.sh
```

For a focused pre-publish check:

```bash
cargo test --lib --tests --examples --all-features
cargo doc --no-deps --all-features
cargo publish --dry-run
```

Note: historical doctest snippets in lower-level module comments are not used
as the release gate yet. The public README and crate-level docs are kept in
English for crates.io and docs.rs.

## Performance Baseline

The current baseline covers frame encoding, message parsing, pipeline
processing, connection lifecycle, and in-memory fanout. It does not model
higher-level IM semantics such as sequence allocation, sync, inbox storage, or
push delivery.

Test environment for the published baseline:

| Item | Value |
|------|-------|
| CPU | Apple M1 Pro, 10 cores |
| Memory | 16 GiB |
| OS | macOS Darwin 25.3.0 |
| Rust | 1.94.1 |
| Build | release mode, single-process benchmark |

Summary:

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

Full report:
[docs/performance-baseline.md](https://github.com/flare-im/flare-transport/blob/main/docs/performance-baseline.md).

## Documentation

| Resource | Link |
|----------|------|
| API reference | [docs.rs/flare-core](https://docs.rs/flare-core) |
| Examples | [examples/README.md](https://github.com/flare-im/flare-transport/blob/main/examples/README.md) |
| Performance report | [docs/performance-baseline.md](https://github.com/flare-im/flare-transport/blob/main/docs/performance-baseline.md) |
| Issues | [GitHub Issues](https://github.com/flare-im/flare-transport/issues) |

## License

Licensed under the [Apache License 2.0](LICENSE).

---

## Next Steps

| Goal | Start here |
| --- | --- |
| Start a Core messaging server | [Core quickstart](https://github.com/flare-im/flare-core-server/blob/main/docs/QUICKSTART.md) |
| Connect your own identity provider | [Core authentication and token issuance](https://github.com/flare-im/flare-core-server/blob/main/docs/AUTH-TOKEN-ISSUANCE.md) |
| Add business rules around messaging | [Core hook contract](https://github.com/flare-im/flare-core-server/blob/main/docs/HOOK-PLUGIN-CONTRACT.md) |
| Build an IM interface | [Flare UI](https://github.com/flare-im/flare-im-design) |
| Report a security issue | [Security policy](.github/SECURITY.md) |

## Identity and Social Features

The open-source Core provides messaging infrastructure. You can connect your own identity and business logic through the Core authentication and hook contracts. Flare Social is maintained separately and provides accounts, contacts, group management, and moments. See the [Core governance policy](https://github.com/flare-im/flare-core-server/blob/main/.github/GOVERNANCE.md) for the open-source boundary.

## Repository Scope

Use this repository for transport and discovery. Start from [the wire contracts](https://github.com/flare-im/flare-protocol) for message encoding or [the Core server](https://github.com/flare-im/flare-core-server) for end-to-end messaging.
