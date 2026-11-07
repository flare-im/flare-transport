//! Client-side builders, transports, heartbeat management, and connection state.
//!
//! Native builds support WebSocket, QUIC, TCP, and hybrid protocol racing depending
//! on enabled features. WASM builds support the browser WebSocket client stack.
//!
//! Main entry points:
//!
//! - `FlareClientBuilder` for production integrations with a message listener
//!   and pipeline-oriented lifecycle.
//! - `ClientBuilder` for closure-based examples and simple applications.
//! - `ObserverClientBuilder` for native integrations that need explicit
//!   connection observation and protocol racing.
//! - `HybridClient` for native transport racing when multiple protocols are
//!   available.
//!
//! WASM integrators: read [`crate::transport::connection`] for `Send`/`Sync` and LocalSet rules,
//! and use the wasm async helpers re-exported by this module for browser entry points.

#[cfg(not(any(
    feature = "websocket",
    all(feature = "quic", not(target_arch = "wasm32")),
    all(feature = "tcp", not(target_arch = "wasm32"))
)))]
compile_error!(
    "feature `client` requires at least one transport feature: `websocket`, native `quic`, or native `tcp`"
);

pub mod builder;
pub mod config;
pub mod connection;
#[cfg(not(target_arch = "wasm32"))]
pub mod dial;
pub mod events;
pub mod heartbeat;
pub mod link;
pub mod race;
pub mod router;
pub mod runtime;
pub mod transports;
#[cfg(target_arch = "wasm32")]
pub mod wasm_tokio;

pub use builder::{ClientBuilder, FlareClient, FlareClientBuilder, MessageListener, SimpleClient};
pub use config::ClientConfig;
pub use connection::{ConnectionState, ConnectionStateManager};
pub use events::{ClientEventHandler, DefaultClientMessageObserver};
pub use heartbeat::HeartbeatManager;
pub use link::{
    AttemptFailure, AttemptReport, CancelledAttempt, CloseKind, ConnectAckInfo, ConnectFailure,
    ConnectReport, DisconnectReason, ServerClose, ServerRejection,
};
pub use race::{DEFAULT_CONNECT_TIMEOUT, DEFAULT_RACE_STAGGER};
pub use router::{AsyncHandler, MessageHandler, MessageRouter, SimpleHandler};
pub use transports::Client;
#[cfg(feature = "websocket")]
pub use transports::WebSocketClient;

#[cfg(target_arch = "wasm32")]
pub use wasm_tokio::{
    ensure_initialized as ensure_wasm_tokio, run_async as run_wasm_async,
    spawn_detached as spawn_wasm_detached,
};

#[cfg(not(target_arch = "wasm32"))]
pub use builder::{ObserverClient, ObserverClientBuilder};
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub use transports::HybridClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "quic"))]
pub use transports::QUICClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "tcp"))]
pub use transports::TCPClient;

pub use crate::common::config_types::TransportProtocol;
pub use crate::common::error::ClientError;
pub use crate::common::error::Result;
