//! Helpers for connecting the WASM demo to `flare_chat_server` via `FlareClientBuilder`.

use async_trait::async_trait;
use flare_core::client::builder::flare::{FlareClientBuilder, MessageListener};
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::HeartbeatConfig;
use flare_core::common::error::Result;
use flare_core::common::platform::{register_aes256_encryption, web_device_info, AES256_KEY_LEN};
use flare_core::common::protocol::Frame;
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::flare::core::commands::payload_command::Type as PayloadType;
use flare_core::common::{MessageParser, SerializationFormat};
use std::sync::{Arc, OnceLock};

static MESSAGE_PARSER: OnceLock<Arc<tokio::sync::Mutex<MessageParser>>> = OnceLock::new();

pub fn message_parser_slot() -> &'static Arc<tokio::sync::Mutex<MessageParser>> {
    MESSAGE_PARSER.get_or_init(|| {
        Arc::new(tokio::sync::Mutex::new(
            flare_core::common::message::parser::PRE_NEGOTIATION_PARSER.clone(),
        ))
    })
}

/// Demo encryption key — must match `flare_chat_server` / `flare_chat_client` default.
pub const DEMO_ENCRYPTION_KEY: &[u8; AES256_KEY_LEN] = b"01234567890123456789012345678901";

/// Register AES-256-GCM encryptor (runtime key > demo default).
pub fn register_flare_chat_encryption() -> Result<()> {
    register_aes256_encryption(Some(DEMO_ENCRYPTION_KEY))
}

/// WASM chat listener — logs lifecycle + chat payloads through `MessageListener`.
pub struct WasmChatListener {
    log: Arc<dyn Fn(&str) + Send + Sync>,
}

impl WasmChatListener {
    pub fn new(log: Arc<dyn Fn(&str) + Send + Sync>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl MessageListener for WasmChatListener {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        let Some(ref cmd) = frame.command else {
            return Ok(None);
        };
        let Some(Type::Payload(payload_cmd)) = &cmd.r#type else {
            return Ok(None);
        };
        if payload_cmd.r#type == PayloadType::Message as i32 {
            let text = String::from_utf8_lossy(&payload_cmd.payload);
            (self.log)(&format!("[message] {text}"));
        }
        Ok(None)
    }

    async fn on_disconnect(
        &self,
        reason: &flare_core::client::link::DisconnectReason,
    ) -> Result<()> {
        (self.log)(&format!("[event] disconnected: {reason}"));
        Ok(())
    }
}

/// Build a `FlareClientBuilder` preset for `flare_chat_server` (WebSocket + Flare 协商).
pub fn flare_chat_flare_builder(
    server_url: String,
    user_id: String,
    listener: Arc<dyn MessageListener>,
) -> FlareClientBuilder {
    let device_info = web_device_info(&user_id);
    let heartbeat = HeartbeatConfig::default()
        .with_interval(std::time::Duration::from_secs(30))
        .with_timeout(std::time::Duration::from_secs(90));

    FlareClientBuilder::new(server_url)
        .with_user_id(user_id)
        .with_listener(listener)
        .with_device_info(device_info)
        .with_format(SerializationFormat::Protobuf)
        .with_compression(CompressionAlgorithm::Gzip)
        .with_heartbeat(heartbeat)
        .with_connect_timeout(std::time::Duration::from_secs(10))
}
