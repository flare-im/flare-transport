//! 快速构建命令和 Frame 消息的辅助模块
//! 提供便捷方法创建各种类型的命令，自动生成消息 ID 和时间戳

use super::flare::core::commands::{
    command::Type as CommandType,
    notification_command::Type as NotificationType,
    payload_command::Type as PayloadType,
    system_command::{SerializationFormat, Type as SystemType},
};
use super::flare::core::{
    Frame, Reliability,
    commands::{Command, CustomCommand, NotificationCommand, PayloadCommand, SystemCommand},
};
use crate::common::platform;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// 生成唯一的消息 ID（基于时间戳和递增计数器）
pub fn generate_message_id() -> String {
    let timestamp = platform::wall_clock_ms();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{:016x}", timestamp, counter)
}

/// 获取当前时间戳（毫秒）
pub fn current_timestamp() -> u64 {
    platform::wall_clock_ms()
}

// ============================================================================
// SystemCommand 构建辅助函数
// ============================================================================

/// 创建基础 SystemCommand（内部辅助函数）
fn create_base_system_command(r#type: SystemType, format: SerializationFormat) -> SystemCommand {
    SystemCommand {
        r#type: r#type as i32,
        format: format as i32,
        message: String::new(),
        metadata: HashMap::new(),
        data: Vec::new(),
        compression: String::new(),
        encryption: String::new(),
    }
}

/// 创建带消息的 SystemCommand（内部辅助函数）
fn create_system_command_with_message(
    r#type: SystemType,
    format: SerializationFormat,
    message: impl Into<String>,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> SystemCommand {
    SystemCommand {
        r#type: r#type as i32,
        format: format as i32,
        message: message.into(),
        metadata: metadata.unwrap_or_default(),
        data: Vec::new(),
        compression: String::new(),
        encryption: String::new(),
    }
}

/// 创建带数据的 SystemCommand（内部辅助函数）
fn create_system_command_with_data(
    r#type: SystemType,
    format: SerializationFormat,
    message: impl Into<String>,
    metadata: Option<HashMap<String, Vec<u8>>>,
    data: Option<Vec<u8>>,
) -> SystemCommand {
    SystemCommand {
        r#type: r#type as i32,
        format: format as i32,
        message: message.into(),
        metadata: metadata.unwrap_or_default(),
        data: data.unwrap_or_default(),
        compression: String::new(),
        encryption: String::new(),
    }
}

// ============================================================================
// Frame 构建器
// ============================================================================

/// Frame 构建器
pub struct FrameBuilder {
    command: Option<Command>,
    message_id: Option<String>,
    reliability: Reliability,
    timestamp: Option<u64>,
    metadata: HashMap<String, Vec<u8>>,
}

impl FrameBuilder {
    /// 创建新的 Frame 构建器
    pub fn new() -> Self {
        Self {
            command: None,
            message_id: None,
            reliability: Reliability::BestEffort,
            timestamp: None,
            metadata: HashMap::new(),
        }
    }

    /// 设置命令
    #[must_use]
    pub fn with_command(mut self, command: Command) -> Self {
        self.command = Some(command);
        self
    }

    /// 设置消息 ID（不设置则自动生成）
    #[must_use]
    pub fn with_message_id(mut self, message_id: String) -> Self {
        self.message_id = Some(message_id);
        self
    }

    /// 设置可靠性等级
    #[must_use]
    pub fn with_reliability(mut self, reliability: Reliability) -> Self {
        self.reliability = reliability;
        self
    }

    /// 设置时间戳（不设置则使用当前时间）
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// 添加元数据
    #[must_use]
    pub fn with_metadata(mut self, key: String, value: Vec<u8>) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// 添加字符串元数据
    #[must_use]
    pub fn with_metadata_str(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value.into_bytes());
        self
    }

    /// 构建 Frame
    pub fn build(self) -> Frame {
        Frame {
            command: self.command,
            message_id: self.message_id.unwrap_or_else(generate_message_id),
            reliability: self.reliability as i32,
            timestamp: self.timestamp.unwrap_or_else(current_timestamp),
            metadata: self.metadata,
        }
    }
}

impl Default for FrameBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// 系统命令构建方法
// ============================================================================

/// 创建 PING 命令
pub fn ping() -> SystemCommand {
    create_base_system_command(SystemType::Ping, SerializationFormat::Protobuf)
}

/// 创建 PONG 命令
pub fn pong() -> SystemCommand {
    create_base_system_command(SystemType::Pong, SerializationFormat::Protobuf)
}

/// 创建 CONNECT 命令
pub fn connect(format: SerializationFormat, metadata: HashMap<String, Vec<u8>>) -> SystemCommand {
    SystemCommand {
        r#type: SystemType::Connect as i32,
        format: format as i32,
        message: String::new(),
        metadata,
        data: Vec::new(),
        compression: String::new(),
        encryption: String::new(),
    }
}

/// 创建 CONNECT_ACK 命令
pub fn connect_ack(
    format: SerializationFormat,
    compression: Option<&str>,
    encryption: Option<&str>,
    metadata: HashMap<String, Vec<u8>>,
) -> SystemCommand {
    SystemCommand {
        r#type: SystemType::ConnectAck as i32,
        format: format as i32,
        message: String::new(),
        metadata,
        data: Vec::new(),
        compression: compression.unwrap_or("none").to_string(),
        encryption: encryption.unwrap_or("none").to_string(),
    }
}

/// 创建 CLOSE 命令
pub fn close(message: Option<String>, metadata: Option<HashMap<String, Vec<u8>>>) -> SystemCommand {
    create_system_command_with_message(
        SystemType::Close,
        SerializationFormat::Protobuf,
        message.unwrap_or_default(),
        metadata,
    )
}

/// 创建 ERROR 命令
pub fn error(message: String, metadata: Option<HashMap<String, Vec<u8>>>) -> SystemCommand {
    create_system_command_with_message(
        SystemType::Error,
        SerializationFormat::Protobuf,
        message,
        metadata,
    )
}

/// 创建 EVENT 命令
pub fn event(
    message: String,
    metadata: Option<HashMap<String, Vec<u8>>>,
    data: Option<Vec<u8>>,
) -> SystemCommand {
    create_system_command_with_data(
        SystemType::Event,
        SerializationFormat::Protobuf,
        message,
        metadata,
        data,
    )
}

/// 创建 KICKED 命令（被踢下线）
///
/// # 参数
/// - `reason`: 被踢的原因（必需）
/// - `metadata`: 可选的元数据（如设备信息、冲突连接ID等）
///
/// # 示例
/// ```rust
/// use flare_core::common::protocol::builder::kicked;
/// use flare_core::common::protocol::frame_with_system_command;
/// use flare_core::common::Reliability;
/// use std::collections::HashMap;
///
/// let mut metadata = HashMap::new();
/// metadata.insert("conflict_device".to_string(), "device-123".as_bytes().to_vec());
///
/// let kick_cmd = kicked("设备冲突：同一平台已有其他设备在线", Some(metadata));
/// let frame = frame_with_system_command(kick_cmd, Reliability::AtLeastOnce);
/// ```
pub fn kicked(
    reason: impl Into<String>,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> SystemCommand {
    create_system_command_with_message(
        SystemType::Kicked,
        SerializationFormat::Protobuf,
        reason,
        metadata,
    )
}

/// 创建 DEVICE_STATE 命令（客户端上报设备状态）。
///
/// `data` 是上层定义的状态载荷，传输层只负责送达，不解释内容。
pub fn device_state(data: Vec<u8>) -> SystemCommand {
    create_system_command_with_data(
        SystemType::DeviceState,
        SerializationFormat::Protobuf,
        String::new(),
        None,
        Some(data),
    )
}

/// 创建 DEVICE_STATE_ACK 命令（服务端确认设备状态）。
///
/// 服务端在处理完对应的 DEVICE_STATE 之后写出。同一条连接上帧按写出顺序到达，
/// 所以客户端收到它时，服务端在此之前写出的帧都已经到了。
pub fn device_state_ack(data: Vec<u8>) -> SystemCommand {
    create_system_command_with_data(
        SystemType::DeviceStateAck,
        SerializationFormat::Protobuf,
        String::new(),
        None,
        Some(data),
    )
}

// ============================================================================
// 载荷命令构建方法
// ============================================================================

/// 创建载荷命令（内部辅助函数）
fn create_payload_command(
    r#type: PayloadType,
    message_id: String,
    payload: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
    seq: Option<u64>,
) -> PayloadCommand {
    PayloadCommand {
        r#type: r#type as i32,
        message_id,
        payload,
        metadata: metadata.unwrap_or_default(),
        seq: seq.unwrap_or(0),
    }
}

/// 创建 MESSAGE 载荷命令（业务消息，需 ACK）
pub fn send_message(
    message_id: String,
    payload: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
    seq: Option<u64>,
) -> PayloadCommand {
    create_payload_command(PayloadType::Message, message_id, payload, metadata, seq)
}

/// 创建 EVENT 载荷命令（事件）
pub fn event_message(
    message_id: String,
    payload: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
    seq: Option<u64>,
) -> PayloadCommand {
    create_payload_command(PayloadType::Event, message_id, payload, metadata, seq)
}

/// 创建 ACK 载荷命令（确认）
pub fn ack_message(
    message_id: String,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> PayloadCommand {
    PayloadCommand {
        r#type: PayloadType::Ack as i32,
        message_id,
        payload: Vec::new(),
        metadata: metadata.unwrap_or_default(),
        seq: 0,
    }
}

/// 创建 DATA 载荷命令（无需 ACK）
pub fn data_message(
    message_id: String,
    payload: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
    seq: Option<u64>,
) -> PayloadCommand {
    create_payload_command(PayloadType::Data, message_id, payload, metadata, seq)
}

// ============================================================================
// 通知命令构建方法
// ============================================================================

/// 创建通知命令
pub fn notification(
    notification_type: NotificationType,
    title: String,
    content: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> NotificationCommand {
    NotificationCommand {
        r#type: notification_type as i32,
        title,
        content,
        metadata: metadata.unwrap_or_default(),
    }
}

// ============================================================================
// 自定义命令构建方法
// ============================================================================

/// 创建自定义命令
pub fn custom_command(
    name: String,
    data: Vec<u8>,
    metadata: Option<HashMap<String, Vec<u8>>>,
) -> CustomCommand {
    CustomCommand {
        name,
        data,
        metadata: metadata.unwrap_or_default(),
    }
}

// ============================================================================
// Frame 快速构建方法
// ============================================================================

/// 创建包含命令的 Frame（内部辅助函数）
fn create_frame_with_command(command_type: CommandType, reliability: Reliability) -> Frame {
    FrameBuilder::new()
        .with_command(Command {
            r#type: Some(command_type),
        })
        .with_reliability(reliability)
        .build()
}

/// 创建包含系统命令的 Frame
pub fn frame_with_system_command(system_command: SystemCommand, reliability: Reliability) -> Frame {
    create_frame_with_command(CommandType::System(system_command), reliability)
}

/// 创建包含载荷命令的 Frame（载荷类型：MESSAGE/EVENT/ACK/DATA）
///
/// Frame 的 message_id 使用 PayloadCommand.message_id；若为空则自动生成并回写。
pub fn frame_with_payload_command(
    mut payload_command: PayloadCommand,
    reliability: Reliability,
) -> Frame {
    if payload_command.message_id.is_empty() {
        payload_command.message_id = generate_message_id();
    }
    let message_id = payload_command.message_id.clone();
    FrameBuilder::new()
        .with_command(Command {
            r#type: Some(CommandType::Payload(payload_command)),
        })
        .with_message_id(message_id)
        .with_reliability(reliability)
        .build()
}

/// Alias for [`frame_with_payload_command`] (backward compatibility for examples).
pub fn frame_with_message_command(
    payload_command: PayloadCommand,
    reliability: Reliability,
) -> Frame {
    frame_with_payload_command(payload_command, reliability)
}

/// 创建包含通知命令的 Frame
pub fn frame_with_notification_command(
    notification_command: NotificationCommand,
    reliability: Reliability,
) -> Frame {
    create_frame_with_command(CommandType::Notification(notification_command), reliability)
}

/// 创建包含自定义命令的 Frame
pub fn frame_with_custom_command(custom_command: CustomCommand, reliability: Reliability) -> Frame {
    create_frame_with_command(CommandType::Custom(custom_command), reliability)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_pong() {
        let ping_cmd = ping();
        assert_eq!(ping_cmd.r#type, SystemType::Ping as i32);

        let pong_cmd = pong();
        assert_eq!(pong_cmd.r#type, SystemType::Pong as i32);
    }

    #[test]
    fn test_generate_message_id() {
        let id1 = generate_message_id();
        let id2 = generate_message_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_frame_builder() {
        let frame = FrameBuilder::new()
            .with_command(Command {
                r#type: Some(CommandType::System(ping())),
            })
            .with_reliability(Reliability::AtLeastOnce)
            .build();

        assert!(!frame.message_id.is_empty());
        assert!(frame.timestamp > 0);
    }
}
