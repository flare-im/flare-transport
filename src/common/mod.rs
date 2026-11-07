//! Shared protocol, codec, error, and runtime support for `flare-core`.
//!
//! This module is the stable foundation used by both client and server builds.
//! It intentionally contains transport-neutral building blocks:
//!
//! - [`protocol`] defines the public frame and command model.
//! - [`message`] provides parsers, processors, middleware, and pipelines.
//! - [`serializer`], [`compression`], and [`encryption`] provide pluggable wire
//!   transformations.
//! - [`error`] exposes typed errors and localized error construction.
//! - [`config_types`] contains cross-cutting configuration such as heartbeat,
//!   TLS, and transport selection.
//! - [`features`] reports the capability set compiled into the current binary.
//!
//! Higher-level IM semantics should not be placed here. Keep this module
//! focused on reusable transport and protocol primitives.

#[cfg(not(target_arch = "wasm32"))]
pub mod cert;
pub mod compression;
pub mod config_types;
pub mod constants;
pub mod conversation;
pub mod device;
pub mod encryption;
pub mod error;
pub mod features;
pub mod kick;
pub mod link;
pub mod message;
pub mod message_observer;
pub mod platform;
pub mod protobuf_decoder;
pub mod protocol;
pub mod serializer;
pub mod shutdown;
pub mod utils;

// Commonly used types are re-exported here for ergonomic imports.

pub use compression::{CompressionAlgorithm, CompressionUtil, Compressor};
pub use config_types::{HeartbeatAppState, HeartbeatConfig, TlsConfig, TransportProtocol};
pub use constants::*;
pub use conversation::*;
pub use device::{
    DeviceConflictStrategy, DeviceConflictStrategyBuilder, DeviceInfo, DevicePlatform,
};
pub use encryption::{EncryptionAlgorithm, EncryptionUtil, Encryptor};
pub use error::{ClientError, ErrorBuilder, ErrorCode, FlareError, Result, ServerError};
pub use features::FeatureSet;
pub use kick::{DEVICE_CONFLICT_REASON, TENANT_UNAVAILABLE_REASON};
pub use link::{CloseKind, ConnectAckInfo, ServerClose, ServerRejection};
pub use message::{
    ArcMessageMiddleware, ArcMessageProcessor, DelegateProcessor, FunctionProcessor, LogLevel,
    LoggingMiddleware, MessageContext, MessageEvent, MessageHandler, MessageMiddleware,
    MessageParser, MessagePipeline, MessageProcessor, MetricsMiddleware, ValidationMiddleware,
};
pub use message_observer::{ArcMessageObserver, MessageObserver};
pub use platform::{
    AES256_KEY_LEN, MonotonicInstant, clear_runtime_encryption_key, default_local_ws_url,
    format_now_rfc3339, has_runtime_encryption_key, interval, monotonic_now,
    parse_encryption_key_hex, parse_encryption_key_utf8, register_aes256_encryption,
    resolve_encryption_key_bytes, runtime_instance_id, set_runtime_encryption_key, sleep, timeout,
    wall_clock_ms, web_device_info,
};
pub use protocol::{
    Command, CustomCommand, Frame, NotificationCommand, PayloadCommand, Reliability,
    SerializationFormat, SystemCommand,
};
pub use serializer::{SerializationUtil, Serializer};
pub use shutdown::{
    SERVER_SHUTDOWN_REASON, is_server_shutdown_reason, server_shutdown_reconnect_delay,
};
pub use utils::*;
