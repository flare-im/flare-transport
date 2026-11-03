//! Production-oriented long-connection primitives for realtime Rust systems.
//!
//! `flare-core` provides the transport foundation used by Flare IM and other
//! realtime applications. It focuses on connection-oriented infrastructure:
//! WebSocket, QUIC, optional TCP, negotiation, heartbeat policy, reconnection,
//! message parsing, compression, encryption, and extensible middleware.
//!
//! The crate intentionally stays transport-centric and business-neutral.
//! Product semantics such as message sequence allocation, inbox sync, push
//! delivery, moderation, and tenant-specific policy should live in higher-level
//! services or extension crates.
//!
//! # Main Modules
//!
//! - [`common`] contains protocol frames, parsers, codecs, compression,
//!   encryption, errors, feature discovery, and shared configuration types.
//! - [`transport`] contains transport events, framing helpers, and concrete
//!   transport implementations.
//! - [`client`] contains client builders, connection orchestration, heartbeat
//!   handling, reconnect support, and native or wasm WebSocket clients.
//! - `server` contains native server builders, connection management,
//!   negotiation, event handling, and transport listeners.
//!
//! # Feature Flags
//!
//! - `client`: client builders and transport clients.
//! - `server`: native server builders and connection management.
//! - `websocket`: WebSocket transport.
//! - `quic`: native QUIC transport.
//! - `tcp`: optional length-prefixed TCP transport.
//! - `wasm`: wasm32 WebSocket client support.
//! - `compression-gzip`: Gzip compression support.
//! - `encryption-aes-gcm`: AES-256-GCM encryption support.
//! - `full`: default feature set plus TCP.
//!
//! # Runtime Model
//!
//! A typical connection goes through transport establishment, CONNECT
//! negotiation, parser alignment, NEGOTIATION_READY, heartbeat startup, message
//! exchange, and disconnect or reconnect cleanup. The public builders hide most
//! of this lifecycle while keeping failure semantics explicit through typed
//! errors and connection events.
//!
//! # Choosing An Entry Point
//!
//! - Start with `client::FlareClientBuilder` or `server::FlareServerBuilder`
//!   for production integrations that need traits, middleware, and connection
//!   lifecycle hooks.
//! - Use `client::ClientBuilder` or `server::ServerBuilder` for compact
//!   examples and closure-based prototypes.
//! - Use [`transport`] directly only when building a custom runtime adapter or
//!   transport-level integration.
//!
//! # Minimal Server Sketch
//!
//! ```no_run
//! use async_trait::async_trait;
//! use flare_core::common::error::Result;
//! use flare_core::common::protocol::{Frame, PayloadCommand};
//! use flare_core::server::events::handler::ServerEventHandler;
//! use flare_core::server::FlareServerBuilder;
//! use std::sync::Arc;
//!
//! struct Handler;
//!
//! #[async_trait]
//! impl ServerEventHandler for Handler {
//!     async fn handle_message(
//!         &self,
//!         _command: &PayloadCommand,
//!         _connection_id: &str,
//!     ) -> Result<Option<Frame>> {
//!         Ok(None)
//!     }
//! }
//!
//! # #[cfg(all(feature = "server", not(target_arch = "wasm32")))]
//! # async fn run() -> Result<()> {
//! let server = FlareServerBuilder::new("0.0.0.0:8080", Arc::new(Handler)).build()?;
//! server.start().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Documentation
//!
//! See the repository README for installation, examples, performance baselines,
//! and release verification guidance:
//! <https://github.com/flare-im/flare-transport>.

pub mod common;

#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub mod server;

#[cfg(feature = "client")]
pub mod client;
pub mod transport;

#[cfg(all(
    feature = "client",
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub use client::HybridClient;
#[cfg(all(
    feature = "server",
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub use server::HybridServer;

#[cfg(all(feature = "client", not(target_arch = "wasm32")))]
pub use client::{ClientBuilder, ObserverClient, ObserverClientBuilder, SimpleClient};
#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub use server::{
    DefaultServerHandle, MessageContext, ObserverServer, ObserverServerBuilder, ServerBuilder,
    ServerHandle, SimpleServer,
};

#[cfg(all(feature = "client", target_arch = "wasm32"))]
pub use client::{
    ClientBuilder, FlareClient, FlareClientBuilder, MessageListener, SimpleClient, WebSocketClient,
};

pub use common::conversation::*;
