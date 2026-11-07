//! 错误代码和错误类别定义

use serde::{Deserialize, Serialize};
use std::fmt;

/// 错误代码枚举 - 用于国际化
///
/// 错误代码按类别分组，每个类别占用1000个代码范围：
/// - 1000-1999: 连接相关错误
/// - 2000-2999: 认证相关错误
/// - 3000-3999: 协议相关错误
/// - 4000-4999: 消息相关错误
/// - 5000-5999: 用户相关错误
/// - 6000-6999: 系统相关错误
/// - 7000-7999: 网络相关错误
/// - 8000-8999: 序列化相关错误
/// - 9000-9999: 通用错误
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(u32)]
pub enum ErrorCode {
    // ============================================================
    // 连接相关错误 (1000-1999)
    // ============================================================
    ConnectionFailed = 1000,
    ConnectionTimeout = 1001,
    ConnectionClosed = 1002,
    ConnectionRefused = 1003,
    ConnectionLimitExceeded = 1004,
    NotConnected = 1005,
    ConnectionReconnecting = 1006,

    // ============================================================
    // 认证相关错误 (2000-2999)
    // ============================================================
    AuthenticationFailed = 2000,
    AuthenticationExpired = 2001,
    AuthenticationInvalid = 2002,
    AuthenticationRequired = 2003,
    PermissionDenied = 2004,
    TokenInvalid = 2005,
    TokenExpired = 2006,

    // ============================================================
    // 协议相关错误 (3000-3999)
    // ============================================================
    ProtocolError = 3000,
    ProtocolVersionMismatch = 3001,
    ProtocolNotSupported = 3002,
    MessageFormatError = 3003,
    MessageTooLarge = 3004,
    InvalidCommand = 3005,
    /// 客户端构建号低于服务端要求的最低值：连接被拒、不可重试，需要升级后再连。
    /// 拒绝帧带 `min_build`、`release_hint`（见 [`crate::common::link::ServerRejection`]）。
    UpgradeRequired = 3006,

    // ============================================================
    // 消息相关错误 (4000-4999)
    // ============================================================
    MessageSendFailed = 4000,
    MessageDeliveryFailed = 4001,
    MessageNotFound = 4002,
    MessageExpired = 4003,
    MessageRateLimitExceeded = 4004,
    MessageDecodeFailed = 4005,

    // ============================================================
    // 用户相关错误 (5000-5999)
    // ============================================================
    UserNotFound = 5000,
    UserOffline = 5001,
    UserBlocked = 5002,
    UserQuotaExceeded = 5003,
    UserSessionLimitExceeded = 5004,

    // ============================================================
    // 系统相关错误 (6000-6999)
    // ============================================================
    InternalError = 6000,
    ServiceUnavailable = 6001,
    ResourceExhausted = 6002,
    ConfigurationError = 6003,
    DatabaseError = 6004,
    /// 服务端过载，在鉴权之前快速拒绝：同一入口等 `retry_after_ms` 后重试，不换入口、不计熔断。
    Overloaded = 6005,

    // ============================================================
    // 网络相关错误 (7000-7999)
    // ============================================================
    NetworkError = 7000,
    NetworkTimeout = 7001,
    NetworkUnreachable = 7002,
    NetworkConnectionLost = 7003,
    /// 请求可能已被服务端处理、但结果没能回来（读超时、连接中途被重置），且请求不幂等
    /// （不带 `Idempotency-Key` 的写操作）：客户端不能自动重发，由用户确认后再试。
    NetworkUncertain = 7004,

    // ============================================================
    // 序列化相关错误 (8000-8999)
    // ============================================================
    SerializationError = 8000,
    DeserializationError = 8001,
    EncodingError = 8002,

    // ============================================================
    // 通用错误 (9000-9999)
    // ============================================================
    GeneralError = 9000,
    InvalidParameter = 9001,
    OperationNotSupported = 9002,
    OperationFailed = 9003,
    OperationTimeout = 9004,
    UnknownError = 9999,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl ErrorCode {
    /// 获取错误代码的数字值
    #[inline]
    pub fn as_u32(&self) -> u32 {
        *self as u32
    }

    /// 从数字值创建错误代码
    pub fn from_u32(code: u32) -> Option<Self> {
        match code {
            1000 => Some(ErrorCode::ConnectionFailed),
            1001 => Some(ErrorCode::ConnectionTimeout),
            1002 => Some(ErrorCode::ConnectionClosed),
            1003 => Some(ErrorCode::ConnectionRefused),
            1004 => Some(ErrorCode::ConnectionLimitExceeded),
            1005 => Some(ErrorCode::NotConnected),
            1006 => Some(ErrorCode::ConnectionReconnecting),
            2000 => Some(ErrorCode::AuthenticationFailed),
            2001 => Some(ErrorCode::AuthenticationExpired),
            2002 => Some(ErrorCode::AuthenticationInvalid),
            2003 => Some(ErrorCode::AuthenticationRequired),
            2004 => Some(ErrorCode::PermissionDenied),
            2005 => Some(ErrorCode::TokenInvalid),
            2006 => Some(ErrorCode::TokenExpired),
            3000 => Some(ErrorCode::ProtocolError),
            3001 => Some(ErrorCode::ProtocolVersionMismatch),
            3002 => Some(ErrorCode::ProtocolNotSupported),
            3003 => Some(ErrorCode::MessageFormatError),
            3004 => Some(ErrorCode::MessageTooLarge),
            3005 => Some(ErrorCode::InvalidCommand),
            3006 => Some(ErrorCode::UpgradeRequired),
            4000 => Some(ErrorCode::MessageSendFailed),
            4001 => Some(ErrorCode::MessageDeliveryFailed),
            4002 => Some(ErrorCode::MessageNotFound),
            4003 => Some(ErrorCode::MessageExpired),
            4004 => Some(ErrorCode::MessageRateLimitExceeded),
            4005 => Some(ErrorCode::MessageDecodeFailed),
            5000 => Some(ErrorCode::UserNotFound),
            5001 => Some(ErrorCode::UserOffline),
            5002 => Some(ErrorCode::UserBlocked),
            5003 => Some(ErrorCode::UserQuotaExceeded),
            5004 => Some(ErrorCode::UserSessionLimitExceeded),
            6000 => Some(ErrorCode::InternalError),
            6001 => Some(ErrorCode::ServiceUnavailable),
            6002 => Some(ErrorCode::ResourceExhausted),
            6003 => Some(ErrorCode::ConfigurationError),
            6004 => Some(ErrorCode::DatabaseError),
            6005 => Some(ErrorCode::Overloaded),
            7000 => Some(ErrorCode::NetworkError),
            7001 => Some(ErrorCode::NetworkTimeout),
            7002 => Some(ErrorCode::NetworkUnreachable),
            7003 => Some(ErrorCode::NetworkConnectionLost),
            7004 => Some(ErrorCode::NetworkUncertain),
            8000 => Some(ErrorCode::SerializationError),
            8001 => Some(ErrorCode::DeserializationError),
            8002 => Some(ErrorCode::EncodingError),
            9000 => Some(ErrorCode::GeneralError),
            9001 => Some(ErrorCode::InvalidParameter),
            9002 => Some(ErrorCode::OperationNotSupported),
            9003 => Some(ErrorCode::OperationFailed),
            9004 => Some(ErrorCode::OperationTimeout),
            9999 => Some(ErrorCode::UnknownError),
            _ => None,
        }
    }

    /// 获取错误代码的英文标识符
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::ConnectionFailed => "CONNECTION_FAILED",
            ErrorCode::ConnectionTimeout => "CONNECTION_TIMEOUT",
            ErrorCode::ConnectionClosed => "CONNECTION_CLOSED",
            ErrorCode::ConnectionRefused => "CONNECTION_REFUSED",
            ErrorCode::ConnectionLimitExceeded => "CONNECTION_LIMIT_EXCEEDED",
            ErrorCode::NotConnected => "NOT_CONNECTED",
            ErrorCode::ConnectionReconnecting => "CONNECTION_RECONNECTING",
            ErrorCode::AuthenticationFailed => "AUTHENTICATION_FAILED",
            ErrorCode::AuthenticationExpired => "AUTHENTICATION_EXPIRED",
            ErrorCode::AuthenticationInvalid => "AUTHENTICATION_INVALID",
            ErrorCode::AuthenticationRequired => "AUTHENTICATION_REQUIRED",
            ErrorCode::PermissionDenied => "PERMISSION_DENIED",
            ErrorCode::TokenInvalid => "TOKEN_INVALID",
            ErrorCode::TokenExpired => "TOKEN_EXPIRED",
            ErrorCode::ProtocolError => "PROTOCOL_ERROR",
            ErrorCode::ProtocolVersionMismatch => "PROTOCOL_VERSION_MISMATCH",
            ErrorCode::ProtocolNotSupported => "PROTOCOL_NOT_SUPPORTED",
            ErrorCode::MessageFormatError => "MESSAGE_FORMAT_ERROR",
            ErrorCode::MessageTooLarge => "MESSAGE_TOO_LARGE",
            ErrorCode::InvalidCommand => "INVALID_COMMAND",
            ErrorCode::UpgradeRequired => "UPGRADE_REQUIRED",
            ErrorCode::MessageSendFailed => "MESSAGE_SEND_FAILED",
            ErrorCode::MessageDeliveryFailed => "MESSAGE_DELIVERY_FAILED",
            ErrorCode::MessageNotFound => "MESSAGE_NOT_FOUND",
            ErrorCode::MessageExpired => "MESSAGE_EXPIRED",
            ErrorCode::MessageRateLimitExceeded => "MESSAGE_RATE_LIMIT_EXCEEDED",
            ErrorCode::MessageDecodeFailed => "MESSAGE_DECODE_FAILED",
            ErrorCode::UserNotFound => "USER_NOT_FOUND",
            ErrorCode::UserOffline => "USER_OFFLINE",
            ErrorCode::UserBlocked => "USER_BLOCKED",
            ErrorCode::UserQuotaExceeded => "USER_QUOTA_EXCEEDED",
            ErrorCode::UserSessionLimitExceeded => "USER_SESSION_LIMIT_EXCEEDED",
            ErrorCode::InternalError => "INTERNAL_ERROR",
            ErrorCode::ServiceUnavailable => "SERVICE_UNAVAILABLE",
            ErrorCode::ResourceExhausted => "RESOURCE_EXHAUSTED",
            ErrorCode::ConfigurationError => "CONFIGURATION_ERROR",
            ErrorCode::DatabaseError => "DATABASE_ERROR",
            ErrorCode::Overloaded => "OVERLOADED",
            ErrorCode::NetworkError => "NETWORK_ERROR",
            ErrorCode::NetworkTimeout => "NETWORK_TIMEOUT",
            ErrorCode::NetworkUnreachable => "NETWORK_UNREACHABLE",
            ErrorCode::NetworkConnectionLost => "NETWORK_CONNECTION_LOST",
            ErrorCode::NetworkUncertain => "NETWORK_UNCERTAIN",
            ErrorCode::SerializationError => "SERIALIZATION_ERROR",
            ErrorCode::DeserializationError => "DESERIALIZATION_ERROR",
            ErrorCode::EncodingError => "ENCODING_ERROR",
            ErrorCode::GeneralError => "GENERAL_ERROR",
            ErrorCode::InvalidParameter => "INVALID_PARAMETER",
            ErrorCode::OperationNotSupported => "OPERATION_NOT_SUPPORTED",
            ErrorCode::OperationFailed => "OPERATION_FAILED",
            ErrorCode::OperationTimeout => "OPERATION_TIMEOUT",
            ErrorCode::UnknownError => "UNKNOWN_ERROR",
        }
    }

    /// 获取错误代码的类别（用于错误分类）
    pub fn category(&self) -> ErrorCategory {
        let code = self.as_u32();
        match code {
            1000..=1999 => ErrorCategory::Connection,
            2000..=2999 => ErrorCategory::Authentication,
            3000..=3999 => ErrorCategory::Protocol,
            4000..=4999 => ErrorCategory::Message,
            5000..=5999 => ErrorCategory::User,
            6000..=6999 => ErrorCategory::System,
            7000..=7999 => ErrorCategory::Network,
            8000..=8999 => ErrorCategory::Serialization,
            _ => ErrorCategory::General,
        }
    }

    /// 判断是否为可重试的错误
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorCode::ConnectionTimeout
                | ErrorCode::ConnectionClosed
                | ErrorCode::NetworkTimeout
                | ErrorCode::NetworkConnectionLost
                | ErrorCode::ServiceUnavailable
                | ErrorCode::ResourceExhausted
                | ErrorCode::Overloaded
        )
    }

    /// 建连被服务端以这个错误码拒绝后，是否只能由用户动作恢复（重新登录、升级、联系管理员），
    /// 自动重连只会被再拒一次。
    ///
    /// 鉴权类（令牌问题）不在其中：令牌可以续期后重连，由上层决定。
    pub fn is_terminal_connect_rejection(&self) -> bool {
        matches!(
            self,
            ErrorCode::UpgradeRequired
                | ErrorCode::ProtocolVersionMismatch
                | ErrorCode::PermissionDenied
        )
    }
}

/// 错误类别
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCategory {
    Connection,
    Authentication,
    Protocol,
    Message,
    User,
    System,
    Network,
    Serialization,
    General,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCategory::Connection => write!(f, "CONNECTION"),
            ErrorCategory::Authentication => write!(f, "AUTHENTICATION"),
            ErrorCategory::Protocol => write!(f, "PROTOCOL"),
            ErrorCategory::Message => write!(f, "MESSAGE"),
            ErrorCategory::User => write!(f, "USER"),
            ErrorCategory::System => write!(f, "SYSTEM"),
            ErrorCategory::Network => write!(f, "NETWORK"),
            ErrorCategory::Serialization => write!(f, "SERIALIZATION"),
            ErrorCategory::General => write!(f, "GENERAL"),
        }
    }
}
