//! CONNECT 协商与 NEGOTIATION_READY 发送

use super::ClientCore;
use crate::common::error::{FlareError, Result};
use crate::common::protocol::flare::core::commands::command::Type;
use crate::common::protocol::flare::core::commands::system_command::Type as SystemCommandType;
use crate::common::protocol::{Frame, Reliability, connect, frame_with_system_command};
use crate::common::{CompressionAlgorithm, EncryptionAlgorithm, SerializationFormat};
use crate::transport::connection::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

impl ClientCore {
    /// 发送 CONNECT 消息进行协商
    pub async fn send_connect_message(
        &self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
    ) -> Result<()> {
        let metadata = self.build_connect_metadata();

        let connect_cmd = connect(crate::common::protocol::SerializationFormat::Json, metadata);
        let connect_frame = frame_with_system_command(connect_cmd, Reliability::AtLeastOnce);

        let data = self.serialize_connect_frame(&connect_frame).await?;
        let mut conn = connection.lock().await;
        conn.send(&data).await?;

        self.log_connect_sent();
        Ok(())
    }

    fn build_connect_metadata(&self) -> HashMap<String, Vec<u8>> {
        let mut metadata = HashMap::new();
        let (format, compression, should_send_format) = self.determine_connect_format();

        tracing::debug!(
            "[ClientCore] 发送 CONNECT 消息: 请求序列化方式={:?}, 请求压缩方式={:?}, 强制模式={}, 发送format={}",
            format,
            compression,
            self.config.is_force_format(),
            should_send_format
        );

        if should_send_format {
            let format_str = match format {
                crate::common::protocol::SerializationFormat::Protobuf => "protobuf",
                crate::common::protocol::SerializationFormat::Json => "json",
            };
            metadata.insert("format".to_string(), format_str.as_bytes().to_vec());
        }

        if compression != crate::common::compression::CompressionAlgorithm::None {
            metadata.insert("compression".to_string(), compression.as_str().into_bytes());
        }

        // 声明本端能解的压缩：没指定压缩时，服务端只会从这里面选它的默认压缩。
        let accepted = Self::accepted_compressions();
        if !accepted.is_empty() {
            metadata.insert(
                crate::common::constants::metadata_keys::ACCEPT_COMPRESSION.to_string(),
                accepted.join(",").into_bytes(),
            );
        }

        if self.config.is_force_format() {
            metadata.insert("force_format".to_string(), b"true".to_vec());
        }

        // 链路字段：协议版本让服务端在认证之前就能拒绝不兼容的客户端；连接代数与清单 serial
        // 由上层（链路监督者）每次建连给出，服务端记进连接元数据与日志。
        use crate::common::constants::metadata_keys;
        metadata.insert(
            metadata_keys::PROTOCOL_VERSION.to_string(),
            crate::common::constants::PROTOCOL_VERSION
                .as_bytes()
                .to_vec(),
        );
        metadata.insert(
            metadata_keys::CONNECTION_EPOCH.to_string(),
            self.config.connection_epoch.to_string().into_bytes(),
        );
        metadata.insert(
            metadata_keys::ACCESS_SERIAL.to_string(),
            self.config.access_serial.to_string().into_bytes(),
        );

        Self::add_device_metadata(&mut metadata, &self.config);

        if let Some(ref user_id) = self.config.user_id {
            metadata.insert("user_id".to_string(), user_id.as_bytes().to_vec());
        }

        if let Some(ref token) = self.config.token {
            metadata.insert("token".to_string(), token.as_bytes().to_vec());
            tracing::debug!("[ClientCore] 已添加 token 到 CONNECT 消息元数据");
        }

        for (key, value) in &self.config.metadata {
            metadata.insert(key.clone(), value.as_bytes().to_vec());
        }

        metadata
    }

    /// 本端注册了解压器的内置压缩（`gzip` 需要 `compression-gzip` 特性）。
    fn accepted_compressions() -> Vec<String> {
        [
            crate::common::compression::CompressionAlgorithm::Gzip,
            crate::common::compression::CompressionAlgorithm::Zstd,
        ]
        .iter()
        .map(|algorithm| algorithm.as_str())
        .filter(|name| crate::common::compression::CompressionUtil::is_registered(name))
        .collect()
    }

    fn determine_connect_format(
        &self,
    ) -> (
        crate::common::protocol::SerializationFormat,
        crate::common::compression::CompressionAlgorithm,
        bool,
    ) {
        if self.config.is_force_format() {
            (
                self.config.get_serialization_format(),
                self.config.get_compression(),
                true,
            )
        } else if self.config.serialization_format
            != crate::common::protocol::SerializationFormat::Json
        {
            (
                self.config.serialization_format,
                self.config.compression.clone(),
                true,
            )
        } else {
            (
                self.config.serialization_format,
                self.config.compression.clone(),
                false,
            )
        }
    }

    fn add_device_metadata(
        metadata: &mut HashMap<String, Vec<u8>>,
        config: &crate::client::config::ClientConfig,
    ) {
        let Some(ref device_info) = config.device_info else {
            return;
        };

        metadata.insert(
            "device_id".to_string(),
            device_info.device_id.as_bytes().to_vec(),
        );
        metadata.insert(
            "platform".to_string(),
            device_info.platform.as_str().as_bytes().to_vec(),
        );

        if let Some(ref model) = device_info.model {
            metadata.insert("model".to_string(), model.as_bytes().to_vec());
        }
        if let Some(ref app_version) = device_info.app_version {
            metadata.insert("app_version".to_string(), app_version.as_bytes().to_vec());
        }
        if let Some(ref system_version) = device_info.system_version {
            metadata.insert(
                "system_version".to_string(),
                system_version.as_bytes().to_vec(),
            );
        }

        for (key, value) in &device_info.metadata {
            metadata.insert(key.clone(), value.as_bytes().to_vec());
        }
    }

    async fn serialize_connect_frame(&self, frame: &Frame) -> Result<Vec<u8>> {
        if self.config.is_force_format() {
            let parser = self.parser.lock().await;
            parser.serialize(frame)
        } else {
            use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
            PRE_NEGOTIATION_PARSER.serialize(frame)
        }
    }

    fn log_connect_sent(&self) {
        let (format, compression, _) = self.determine_connect_format();

        if self.config.is_force_format() {
            tracing::debug!(
                "[ClientCore] CONNECT 消息已发送（强制模式: format={:?}, compression={:?}）",
                format,
                compression
            );
        } else {
            tracing::debug!(
                "[ClientCore] CONNECT 消息已发送（协商模式: 首选 format={:?}, compression={:?}）",
                format,
                compression
            );
        }
    }

    /// 处理 CONNECT_ACK 消息，返回协商结果
    pub fn handle_connect_ack(
        &self,
        frame: &Frame,
    ) -> Result<(
        SerializationFormat,
        CompressionAlgorithm,
        EncryptionAlgorithm,
    )> {
        let cmd = frame
            .command
            .as_ref()
            .and_then(|c| c.r#type.as_ref())
            .and_then(|t| {
                if let Type::System(sys_cmd) = t {
                    Some(sys_cmd)
                } else {
                    None
                }
            })
            .ok_or_else(|| FlareError::protocol_error("Not a CONNECT_ACK message".to_string()))?;

        let cmd_type = SystemCommandType::try_from(cmd.r#type)
            .map_err(|_| FlareError::protocol_error("Invalid system command type".to_string()))?;

        if cmd_type != SystemCommandType::ConnectAck {
            return Err(FlareError::protocol_error(
                "Not a CONNECT_ACK message".to_string(),
            ));
        }

        let format = SerializationFormat::try_from(cmd.format).unwrap_or(SerializationFormat::Json);
        let compression =
            CompressionAlgorithm::from_str(&cmd.compression).unwrap_or(CompressionAlgorithm::None);
        let encryption =
            EncryptionAlgorithm::from_str(&cmd.encryption).unwrap_or(EncryptionAlgorithm::None);

        tracing::debug!(
            "[ClientCore] 收到 CONNECT_ACK，协商结果: format={:?}, compression={:?}, encryption={:?}",
            format,
            compression,
            encryption
        );

        Self::check_conflict_connections(cmd);

        Ok((format, compression, encryption))
    }

    fn check_conflict_connections(cmd: &crate::common::protocol::SystemCommand) {
        if let Some(conflicts_bytes) = cmd.metadata.get("conflict_connections")
            && let Ok(conflicts_json) = String::from_utf8(conflicts_bytes.clone())
            && let Ok(conflict_connections) = serde_json::from_str::<Vec<String>>(&conflicts_json)
            && !conflict_connections.is_empty()
        {
            tracing::warn!(
                "[ClientCore] 检测到设备冲突，以下连接被踢掉: {:?}",
                conflict_connections
            );
        }
    }

    /// 发送 NEGOTIATION_READY 命令
    pub(crate) async fn send_negotiation_ready(&self) -> Result<()> {
        use crate::common::protocol::flare::core::commands::SystemCommand;
        use crate::common::protocol::flare::core::commands::system_command::Type as SysType;

        let negotiation_ready_cmd = SystemCommand {
            r#type: SysType::NegotiationReady as i32,
            format: 0,
            compression: String::new(),
            encryption: String::new(),
            message: String::new(),
            metadata: std::collections::HashMap::new(),
            data: vec![],
        };

        let frame = frame_with_system_command(negotiation_ready_cmd, Reliability::AtLeastOnce);

        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        let data = PRE_NEGOTIATION_PARSER.serialize(&frame)?;

        let client_conn_opt = {
            if let Ok(conn_guard) = self.client_connection.lock() {
                conn_guard.clone()
            } else {
                return Err(FlareError::connection_failed(
                    "Client connection not available".to_string(),
                ));
            }
        };

        if let Some(client_conn) = client_conn_opt {
            let mut conn = client_conn.lock().await;
            conn.send(&data).await?;
            tracing::debug!("[ClientCore] 已发送 NEGOTIATION_READY 命令");
            Ok(())
        } else {
            Err(FlareError::connection_failed(
                "Client connection not set".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compression::CompressionAlgorithm;
    use crate::common::encryption::EncryptionAlgorithm;
    use crate::common::protocol::builder::connect_ack;
    use crate::common::protocol::{Reliability, SerializationFormat, frame_with_system_command};
    use std::collections::HashMap;

    fn sample_config(user_id: &str, token: &str) -> crate::client::config::ClientConfig {
        crate::client::config::ClientConfig {
            user_id: Some(user_id.to_string()),
            token: Some(token.to_string()),
            serialization_format: SerializationFormat::Protobuf,
            ..Default::default()
        }
    }

    #[test]
    fn handle_connect_ack_parses_negotiation_result() {
        let core = ClientCore::new(&sample_config("u1", "tok"));
        let cmd = connect_ack(
            SerializationFormat::Protobuf,
            Some("gzip"),
            Some("none"),
            HashMap::new(),
        );
        let frame = frame_with_system_command(cmd, Reliability::AtLeastOnce);
        let (format, compression, encryption) = core.handle_connect_ack(&frame).expect("parse");
        assert_eq!(format, SerializationFormat::Protobuf);
        assert_eq!(compression, CompressionAlgorithm::Gzip);
        assert_eq!(encryption, EncryptionAlgorithm::None);
    }

    #[test]
    fn handle_connect_ack_rejects_non_ack_command() {
        use crate::common::protocol::ping;
        let core = ClientCore::new(&sample_config("u1", "tok"));
        let frame = frame_with_system_command(ping(), Reliability::AtLeastOnce);
        assert!(core.handle_connect_ack(&frame).is_err());
    }

    /// 客户端在 CONNECT 里声明能解的压缩；没指定压缩时不发 `compression`，由服务端在声明范围内
    /// 选它的默认压缩。
    #[cfg(feature = "compression-gzip")]
    #[test]
    fn build_connect_metadata_declares_the_compressions_it_can_decode() {
        let core = ClientCore::new(&sample_config("alice", "secret-token"));
        let metadata = core.build_connect_metadata();
        assert!(!metadata.contains_key("compression"));
        let accepted = String::from_utf8(
            metadata[crate::common::constants::metadata_keys::ACCEPT_COMPRESSION].clone(),
        )
        .unwrap();
        assert!(accepted.split(',').any(|name| name == "gzip"), "{accepted}");
    }

    /// CONNECT 带上协议版本、连接代数与接入清单 serial（都是十进制文本）。
    #[test]
    fn build_connect_metadata_carries_the_link_fields() {
        use crate::common::constants::metadata_keys;
        let config = sample_config("alice", "secret-token")
            .with_connection_epoch(7)
            .with_access_serial(42);
        let metadata = ClientCore::new(&config).build_connect_metadata();
        let text = |key: &str| String::from_utf8(metadata[key].clone()).unwrap();
        assert_eq!(
            text(metadata_keys::PROTOCOL_VERSION),
            crate::common::constants::PROTOCOL_VERSION
        );
        assert_eq!(text(metadata_keys::CONNECTION_EPOCH), "7");
        assert_eq!(text(metadata_keys::ACCESS_SERIAL), "42");
    }

    #[test]
    fn build_connect_metadata_includes_auth_fields() {
        let core = ClientCore::new(&sample_config("alice", "secret-token"));
        let metadata = core.build_connect_metadata();
        assert_eq!(
            String::from_utf8(metadata["user_id"].clone()).unwrap(),
            "alice"
        );
        assert_eq!(
            String::from_utf8(metadata["token"].clone()).unwrap(),
            "secret-token"
        );
        assert_eq!(
            String::from_utf8(metadata["format"].clone()).unwrap(),
            "protobuf"
        );
    }
}
