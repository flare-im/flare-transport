//! 常量定义模块
//!
//! 定义项目中使用的各种常量，包括默认值、超时时间、限制等

/// 默认连接超时时间（秒）
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 300;

/// 默认心跳间隔（秒）
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// 默认消息大小限制（字节）
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10MB

/// 默认连接数限制
pub const DEFAULT_MAX_CONNECTIONS: usize = 10000;

/// 默认重连间隔（秒）
pub const DEFAULT_RECONNECT_INTERVAL_SECS: u64 = 5;

/// 最大重连次数
pub const DEFAULT_MAX_RECONNECT_ATTEMPTS: u32 = 5;

/// 默认缓冲区大小（字节）
pub const DEFAULT_BUFFER_SIZE: usize = 8192;

/// 协议版本
pub const PROTOCOL_VERSION: &str = "1.0.0";

/// 支持的协议版本列表
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["1.0.0"];

/// 默认压缩算法
pub const DEFAULT_COMPRESSION: &str = "none";

/// 默认序列化格式
pub const DEFAULT_SERIALIZATION_FORMAT: &str = "json";

/// 元数据键名常量
pub mod metadata_keys {
    /// 会话 ID
    pub const SESSION_ID: &str = "session_id";
    /// 用户 ID
    pub const USER_ID: &str = "user_id";
    /// 压缩算法
    pub const COMPRESSION: &str = "compression";
    /// 客户端能解的压缩（CONNECT 元数据，逗号分隔）
    pub const ACCEPT_COMPRESSION: &str = "accept_compression";
    /// 序列化格式
    pub const FORMAT: &str = "format";
    /// 优先级
    pub const PRIORITY: &str = "priority";
    /// 是否加密
    pub const ENCRYPTED: &str = "encrypted";
    /// 客户端版本
    pub const CLIENT_VERSION: &str = "client_version";
    /// 服务器版本
    pub const SERVER_VERSION: &str = "server_version";

    /// CONNECT：客户端说的协议版本（[`super::PROTOCOL_VERSION`]）。服务端不支持时以
    /// `PROTOCOL_VERSION_MISMATCH` 拒绝。
    pub const PROTOCOL_VERSION: &str = "protocol_version";
    /// CONNECT：客户端连接代数（十进制）。每次建连单调递增，上层据此丢弃旧连接迟到的帧与回调。
    pub const CONNECTION_EPOCH: &str = "connection_epoch";
    /// CONNECT：客户端当前接入清单的 serial；CONNECT_ACK：服务端已知的最新 serial（都是十进制）。
    pub const ACCESS_SERIAL: &str = "access_serial";

    /// CONNECT_ACK：服务端当前时间（Unix 毫秒，十进制），客户端据此修正时钟偏差。
    pub const SERVER_TIME_MS: &str = "server_time_ms";
    /// CONNECT_ACK：服务端建议的心跳间隔（秒，十进制）。
    pub const HEARTBEAT_INTERVAL_HINT_SEC: &str = "heartbeat_interval_hint_sec";
    /// CONNECT_ACK：处理这条连接的网关实例 ID（诊断用）。
    pub const GATEWAY_ID: &str = "gateway_id";
    /// CONNECT_ACK：这条连接所在的接入点 ID（诊断用）。
    pub const AP_ID: &str = "ap_id";

    /// ERROR / CLOSE / KICKED：类别（错误码字符串，如 `OVERLOADED`、`SERVICE_UNAVAILABLE`）。
    pub const CODE: &str = "code";
    /// CLOSE / KICKED：类型化原因（如 `DRAIN`、`DEVICE_CONFLICT`）。
    pub const REASON: &str = "reason";
    /// CLOSE：建议的重连窗口下界（毫秒，十进制）。
    pub const RETRY_AFTER_MIN_MS: &str = "retry_after_min_ms";
    /// CLOSE：建议的重连窗口上界（毫秒，十进制）。
    pub const RETRY_AFTER_MAX_MS: &str = "retry_after_max_ms";
    /// CLOSE：重连时优先的接入点 ID，逗号分隔（可选）。
    pub const PREFER_AP: &str = "prefer_ap";
    /// ERROR(`OVERLOADED`)：建议等多久再连同一入口（毫秒，十进制）。
    pub const RETRY_AFTER_MS: &str = "retry_after_ms";
    /// ERROR(`UPGRADE_REQUIRED`)：服务端要求的最低构建号（十进制）。
    pub const MIN_BUILD: &str = "min_build";
    /// ERROR(`UPGRADE_REQUIRED`)：给用户看的升级说明或下载入口（可选）。
    pub const RELEASE_HINT: &str = "release_hint";
}

/// 可靠性等级常量
pub mod reliability {
    use crate::common::protocol::Reliability;

    /// 默认可靠性等级
    pub const DEFAULT: Reliability = Reliability::BestEffort;

    /// 关键消息推荐使用的可靠性等级
    pub const CRITICAL: Reliability = Reliability::ExactlyOnce;
}

/// 错误消息常量
pub mod error_messages {
    /// 连接失败
    pub const CONNECTION_FAILED: &str = "Connection failed";
    /// 连接超时
    pub const CONNECTION_TIMEOUT: &str = "Connection timeout";
    /// 认证失败
    pub const AUTHENTICATION_FAILED: &str = "Authentication failed";
    /// 协议错误
    pub const PROTOCOL_ERROR: &str = "Protocol error";
    /// 消息格式错误
    pub const MESSAGE_FORMAT_ERROR: &str = "Message format error";
    /// 服务不可用
    pub const SERVICE_UNAVAILABLE: &str = "Service unavailable";
}
