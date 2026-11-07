//! 连接协商模块
//!
//! 处理连接建立时的序列化格式、压缩算法和设备信息协商

use crate::common::compression::CompressionAlgorithm;
use crate::common::device::{DeviceInfo, DevicePlatform};
use crate::common::encryption::EncryptionAlgorithm;
use crate::common::error::Result;
use crate::common::protocol::flare::core::commands::command::Type as CommandType;
use crate::common::protocol::flare::core::commands::system_command::SerializationFormat;
use crate::common::protocol::flare::core::commands::system_command::Type as SystemType;
use crate::common::protocol::{Frame, SystemCommand};
use std::collections::HashMap;

const METADATA_COMPRESSION: &str = "compression";
const METADATA_ACCEPT_COMPRESSION: &str =
    crate::common::constants::metadata_keys::ACCEPT_COMPRESSION;
const METADATA_FORMAT: &str = "format";
const METADATA_ENCRYPTION: &str = "encryption";
const METADATA_DEVICE_ID: &str = "device_id";
const METADATA_PLATFORM: &str = "platform";
const METADATA_MODEL: &str = "model";
const METADATA_APP_VERSION: &str = "app_version";
const METADATA_SYSTEM_VERSION: &str = "system_version";
const METADATA_USER_ID: &str = "user_id";
const METADATA_TENANT_ID: &str = crate::server::auth::TENANT_ID_METADATA_KEY;
const METADATA_FORCE_FORMAT: &str = "force_format";
const METADATA_PROTOCOL_VERSION: &str = crate::common::constants::metadata_keys::PROTOCOL_VERSION;
const METADATA_CONNECTION_EPOCH: &str = crate::common::constants::metadata_keys::CONNECTION_EPOCH;
const METADATA_ACCESS_SERIAL: &str = crate::common::constants::metadata_keys::ACCESS_SERIAL;

const DEVICE_RESERVED_METADATA_KEYS: &[&str] = &[
    METADATA_COMPRESSION,
    METADATA_ACCEPT_COMPRESSION,
    METADATA_FORMAT,
    METADATA_ENCRYPTION,
    METADATA_DEVICE_ID,
    METADATA_PLATFORM,
    METADATA_MODEL,
    METADATA_APP_VERSION,
    METADATA_SYSTEM_VERSION,
    METADATA_USER_ID,
    METADATA_TENANT_ID,
    METADATA_FORCE_FORMAT,
];

/// 连接协商结果
#[derive(Debug, Clone)]
pub struct NegotiationResult {
    /// 序列化格式（客户端请求的格式）
    pub serialization_format: SerializationFormat,
    /// 客户端是否显式请求了序列化格式
    pub serialization_format_specified: bool,
    /// 压缩算法（客户端请求的压缩方式）
    pub compression: CompressionAlgorithm,
    /// 客户端声明能解的压缩（metadata `accept_compression`，逗号分隔，不含 `none`）。服务端只在
    /// 客户端没有指定压缩时，从这里面取自己的默认压缩。
    pub accepted_compressions: Vec<CompressionAlgorithm>,
    /// 加密方式
    pub encryption: EncryptionAlgorithm,
    /// 是否强制指定格式（客户端强制模式，服务端必须使用客户端指定的格式）
    pub is_forced: bool,
    /// 设备信息（如果客户端提供）
    pub device_info: Option<DeviceInfo>,
    /// 用户 ID（如果客户端在 CONNECT 中提供）
    pub user_id: Option<String>,
    /// 租户 ID（如果客户端在 CONNECT 中提供）
    ///
    /// 这是客户端的自报值，只在未启用认证时作为连接租户；启用认证时一律以认证结果中的租户为准，
    /// 不会采信这里的值。
    pub tenant_id: Option<String>,
    /// 客户端说的协议版本（metadata `protocol_version`）；没带时为 `None`
    pub protocol_version: Option<String>,
    /// 客户端连接代数（metadata `connection_epoch`），只用于日志与诊断
    pub connection_epoch: Option<u64>,
    /// 客户端当前接入清单 serial（metadata `access_serial`），只用于日志与诊断
    pub access_serial: Option<u64>,
}

impl Default for NegotiationResult {
    fn default() -> Self {
        Self {
            serialization_format: SerializationFormat::Json,
            serialization_format_specified: false,
            compression: CompressionAlgorithm::None,
            accepted_compressions: Vec::new(),
            encryption: EncryptionAlgorithm::None,
            is_forced: false,
            device_info: None,
            user_id: None,
            tenant_id: None,
            protocol_version: None,
            connection_epoch: None,
            access_serial: None,
        }
    }
}

/// 解析 CONNECT 消息，提取客户端协商信息
///
/// # 参数
/// - `frame`: CONNECT 消息的 Frame
///
/// # 返回
/// 协商结果，包含序列化格式、压缩算法、设备信息等
pub fn parse_connect_message(frame: &Frame) -> Result<NegotiationResult> {
    let Some(sys_cmd) = connect_system_command(frame) else {
        return Ok(NegotiationResult::default());
    };

    if sys_cmd.r#type != SystemType::Connect as i32 {
        return Ok(NegotiationResult::default());
    }

    let metadata = &sys_cmd.metadata;
    let (serialization_format, serialization_format_specified) =
        parse_serialization_format(metadata);
    let result = NegotiationResult {
        serialization_format,
        serialization_format_specified,
        compression: parse_compression(metadata),
        accepted_compressions: parse_accepted_compressions(metadata),
        encryption: parse_encryption(metadata),
        is_forced: metadata_string(metadata, METADATA_FORCE_FORMAT).is_some_and(|v| v == "true"),
        device_info: parse_device_info(metadata),
        user_id: metadata_string(metadata, METADATA_USER_ID),
        tenant_id: metadata_string(metadata, METADATA_TENANT_ID),
        protocol_version: metadata_string(metadata, METADATA_PROTOCOL_VERSION)
            .map(|version| version.trim().to_string())
            .filter(|version| !version.is_empty()),
        connection_epoch: metadata_u64(metadata, METADATA_CONNECTION_EPOCH),
        access_serial: metadata_u64(metadata, METADATA_ACCESS_SERIAL),
    };

    Ok(result)
}

fn connect_system_command(frame: &Frame) -> Option<&SystemCommand> {
    match frame.command.as_ref()?.r#type.as_ref()? {
        CommandType::System(sys_cmd) => Some(sys_cmd),
        _ => None,
    }
}

fn parse_serialization_format(metadata: &HashMap<String, Vec<u8>>) -> (SerializationFormat, bool) {
    let Some(format) = metadata_string(metadata, METADATA_FORMAT) else {
        return (SerializationFormat::Json, false);
    };

    let format = match format.as_str() {
        value if value.eq_ignore_ascii_case("protobuf") || value.eq_ignore_ascii_case("proto") => {
            SerializationFormat::Protobuf
        }
        value if value.eq_ignore_ascii_case("json") => SerializationFormat::Json,
        _ => SerializationFormat::Json,
    };

    (format, true)
}

fn parse_compression(metadata: &HashMap<String, Vec<u8>>) -> CompressionAlgorithm {
    metadata_string(metadata, METADATA_COMPRESSION)
        .and_then(|value| CompressionAlgorithm::from_str(&value))
        .unwrap_or(CompressionAlgorithm::None)
}

fn parse_accepted_compressions(metadata: &HashMap<String, Vec<u8>>) -> Vec<CompressionAlgorithm> {
    let Some(value) = metadata_string(metadata, METADATA_ACCEPT_COMPRESSION) else {
        return Vec::new();
    };
    let mut accepted = Vec::new();
    for algorithm in value
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .filter_map(CompressionAlgorithm::from_str)
    {
        if algorithm != CompressionAlgorithm::None && !accepted.contains(&algorithm) {
            accepted.push(algorithm);
        }
    }
    accepted
}

fn parse_encryption(metadata: &HashMap<String, Vec<u8>>) -> EncryptionAlgorithm {
    metadata_string(metadata, METADATA_ENCRYPTION)
        .and_then(|value| EncryptionAlgorithm::from_str(&value))
        .unwrap_or(EncryptionAlgorithm::None)
}

fn parse_device_info(metadata: &HashMap<String, Vec<u8>>) -> Option<DeviceInfo> {
    let device_id = metadata_string(metadata, METADATA_DEVICE_ID)?;
    let platform = metadata_string(metadata, METADATA_PLATFORM)
        .map(|value| DevicePlatform::from_str(&value))
        .unwrap_or_else(|| DevicePlatform::Other("unknown".to_string()));

    let mut device_info = DeviceInfo::new(device_id, platform);
    if let Some(model) = metadata_string(metadata, METADATA_MODEL) {
        device_info = device_info.with_model(model);
    }
    if let Some(app_version) = metadata_string(metadata, METADATA_APP_VERSION) {
        device_info = device_info.with_app_version(app_version);
    }
    if let Some(system_version) = metadata_string(metadata, METADATA_SYSTEM_VERSION) {
        device_info = device_info.with_system_version(system_version);
    }

    for (key, value) in metadata
        .iter()
        .filter(|(key, _)| !is_reserved_device_metadata_key(key))
    {
        if let Some(value) = bytes_to_string(value) {
            device_info = device_info.with_metadata(key.clone(), value);
        }
    }

    Some(device_info)
}

fn metadata_string(metadata: &HashMap<String, Vec<u8>>, key: &str) -> Option<String> {
    metadata.get(key).and_then(|value| bytes_to_string(value))
}

fn metadata_u64(metadata: &HashMap<String, Vec<u8>>, key: &str) -> Option<u64> {
    metadata_string(metadata, key).and_then(|value| value.trim().parse().ok())
}

fn bytes_to_string(value: &[u8]) -> Option<String> {
    String::from_utf8(value.to_vec()).ok()
}

fn is_reserved_device_metadata_key(key: &str) -> bool {
    DEVICE_RESERVED_METADATA_KEYS.contains(&key)
}

/// 创建 CONNECT_ACK 响应
///
/// # 参数
/// - `format`: 确认使用的序列化格式
/// - `compression`: 确认使用的压缩算法
/// - `additional_metadata`: 额外的元数据（如设备冲突信息等）
///
/// # 返回
/// CONNECT_ACK 命令
/// 创建 CONNECT_ACK 消息
///
/// # 参数
/// - `format`: 确认使用的序列化格式
/// - `compression`: 确认使用的压缩算法
/// - `encryption`: 确认使用的加密方式（目前为 "none"，为未来扩展预留）
/// - `additional_metadata`: 额外的元数据（如设备冲突信息等）
///
/// # 返回
/// CONNECT_ACK 命令
pub fn create_connect_ack(
    format: SerializationFormat,
    compression: CompressionAlgorithm,
    encryption: EncryptionAlgorithm,
    additional_metadata: Option<HashMap<String, Vec<u8>>>,
) -> SystemCommand {
    // 验证压缩算法是否已注册
    let mut compression_str = compression.as_str();
    if !crate::common::compression::CompressionUtil::is_registered(&compression_str) {
        tracing::warn!(
            "[Negotiation] 压缩算法 '{}' 未注册，将使用 'none'",
            compression_str
        );
        // 如果未注册，回退到 none
        compression_str = "none".to_string();
    }

    // 验证加密算法是否已注册
    let mut encryption_str = encryption.as_str();
    if encryption != EncryptionAlgorithm::None {
        if !crate::common::encryption::EncryptionUtil::is_registered(&encryption_str) {
            let registered = crate::common::encryption::EncryptionUtil::list_registered();
            tracing::warn!(
                "[Negotiation] 加密算法 '{}' 未注册，将使用 'none'。已注册的加密器: {:?}",
                encryption_str,
                registered
            );
            // 如果未注册，回退到 none
            encryption_str = "none".to_string();
        } else {
            tracing::debug!(
                "[Negotiation] 加密算法 '{}' 已注册，可以使用",
                encryption_str
            );
        }
    }

    let mut metadata = HashMap::new();

    // 添加额外的元数据
    if let Some(extra) = additional_metadata {
        for (key, value) in extra {
            metadata.insert(key, value);
        }
    }

    crate::common::protocol::connect_ack(
        format,
        Some(compression_str.as_str()),
        Some(encryption_str.as_str()),
        metadata,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::{Reliability, connect, frame_with_system_command, ping};

    #[test]
    fn parses_connect_metadata_into_negotiation_result() {
        let metadata = HashMap::from([
            (METADATA_COMPRESSION.to_string(), b"gzip".to_vec()),
            (METADATA_FORMAT.to_string(), b"protobuf".to_vec()),
            (METADATA_ENCRYPTION.to_string(), b"aes256gcm".to_vec()),
            (METADATA_DEVICE_ID.to_string(), b"device-1".to_vec()),
            (METADATA_PLATFORM.to_string(), b"ios".to_vec()),
            (METADATA_MODEL.to_string(), b"iPhone".to_vec()),
            (METADATA_APP_VERSION.to_string(), b"1.2.3".to_vec()),
            (METADATA_SYSTEM_VERSION.to_string(), b"18.0".to_vec()),
            (METADATA_USER_ID.to_string(), b"user-1".to_vec()),
            (METADATA_TENANT_ID.to_string(), b"tenant-1".to_vec()),
            (METADATA_FORCE_FORMAT.to_string(), b"true".to_vec()),
            ("trace_id".to_string(), b"trace-1".to_vec()),
        ]);
        let frame = frame_with_system_command(
            connect(SerializationFormat::Json, metadata),
            Reliability::AtLeastOnce,
        );

        let result = parse_connect_message(&frame).expect("parse connect");

        assert_eq!(result.serialization_format, SerializationFormat::Protobuf);
        assert!(result.serialization_format_specified);
        assert_eq!(result.compression, CompressionAlgorithm::Gzip);
        assert_eq!(result.encryption, EncryptionAlgorithm::Aes256Gcm);
        assert!(result.is_forced);
        assert_eq!(result.user_id.as_deref(), Some("user-1"));
        assert_eq!(result.tenant_id.as_deref(), Some("tenant-1"));

        let device = result.device_info.expect("device info");
        assert_eq!(device.device_id, "device-1");
        assert_eq!(device.platform, DevicePlatform::IOS);
        assert_eq!(device.model.as_deref(), Some("iPhone"));
        assert_eq!(device.app_version.as_deref(), Some("1.2.3"));
        assert_eq!(device.system_version.as_deref(), Some("18.0"));
        assert_eq!(
            device.metadata.get("trace_id").map(String::as_str),
            Some("trace-1")
        );
        assert!(
            !device.metadata.contains_key(METADATA_TENANT_ID),
            "租户是连接身份的一部分，不应混进设备元数据"
        );
    }

    /// `accept_compression` 是客户端能解的压缩列表：去重、去掉 `none`，不进设备元数据。
    #[test]
    fn parses_accepted_compressions_and_keeps_them_out_of_device_metadata() {
        let metadata = HashMap::from([
            (
                METADATA_ACCEPT_COMPRESSION.to_string(),
                b" gzip, zstd,none,,gzip".to_vec(),
            ),
            (METADATA_DEVICE_ID.to_string(), b"device-1".to_vec()),
        ]);
        let frame = frame_with_system_command(
            connect(SerializationFormat::Json, metadata),
            Reliability::AtLeastOnce,
        );

        let result = parse_connect_message(&frame).expect("parse connect");

        assert_eq!(result.compression, CompressionAlgorithm::None);
        assert_eq!(
            result.accepted_compressions,
            vec![CompressionAlgorithm::Gzip, CompressionAlgorithm::Zstd]
        );
        let device = result.device_info.expect("device info");
        assert!(!device.metadata.contains_key(METADATA_ACCEPT_COMPRESSION));
    }

    /// 协议版本、连接代数、清单 serial 读进协商结果；设备元数据里照样留一份，网关按需记录。
    #[test]
    fn parses_link_fields_from_connect_metadata() {
        let metadata = HashMap::from([
            (METADATA_PROTOCOL_VERSION.to_string(), b" 1.0.0 ".to_vec()),
            (METADATA_CONNECTION_EPOCH.to_string(), b"7".to_vec()),
            (METADATA_ACCESS_SERIAL.to_string(), b"42".to_vec()),
            (METADATA_DEVICE_ID.to_string(), b"device-1".to_vec()),
        ]);
        let frame = frame_with_system_command(
            connect(SerializationFormat::Json, metadata),
            Reliability::AtLeastOnce,
        );
        let result = parse_connect_message(&frame).expect("parse connect");
        assert_eq!(result.protocol_version.as_deref(), Some("1.0.0"));
        assert_eq!(result.connection_epoch, Some(7));
        assert_eq!(result.access_serial, Some(42));

        let bare = frame_with_system_command(
            connect(SerializationFormat::Json, HashMap::new()),
            Reliability::AtLeastOnce,
        );
        let result = parse_connect_message(&bare).expect("parse connect");
        assert_eq!(result.protocol_version, None);
        assert_eq!(result.connection_epoch, None);
    }

    #[test]
    fn returns_default_negotiation_for_non_connect_system_command() {
        let frame = frame_with_system_command(ping(), Reliability::BestEffort);

        let result = parse_connect_message(&frame).expect("parse non-connect");

        assert_eq!(result.serialization_format, SerializationFormat::Json);
        assert!(!result.serialization_format_specified);
        assert_eq!(result.compression, CompressionAlgorithm::None);
        assert_eq!(result.encryption, EncryptionAlgorithm::None);
        assert!(!result.is_forced);
        assert!(result.device_info.is_none());
        assert!(result.user_id.is_none());
        assert!(result.tenant_id.is_none());
    }

    #[test]
    fn parses_explicit_json_format_from_metadata() {
        let metadata = HashMap::from([(METADATA_FORMAT.to_string(), b"json".to_vec())]);
        let frame = frame_with_system_command(
            connect(SerializationFormat::Json, metadata),
            Reliability::AtLeastOnce,
        );

        let result = parse_connect_message(&frame).expect("parse connect");

        assert_eq!(result.serialization_format, SerializationFormat::Json);
        assert!(result.serialization_format_specified);
    }

    #[test]
    fn treats_missing_format_metadata_as_unspecified() {
        let frame = frame_with_system_command(
            connect(SerializationFormat::Json, HashMap::new()),
            Reliability::AtLeastOnce,
        );

        let result = parse_connect_message(&frame).expect("parse connect");

        assert_eq!(result.serialization_format, SerializationFormat::Json);
        assert!(!result.serialization_format_specified);
    }
}
