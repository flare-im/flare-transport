//! 服务端配置模块

use crate::common::compression::CompressionAlgorithm;
use crate::common::config_types::{HeartbeatConfig, TlsConfig, TransportProtocol};
use crate::common::device::DeviceConflictStrategy;
use crate::common::encryption::EncryptionAlgorithm;
use crate::common::protocol::SerializationFormat;
use std::time::Duration;

/// 服务端配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerConfig {
    /// 监听地址（单个协议时使用，多协议时用作默认地址）
    pub bind_address: String,
    /// 传输协议（单个）
    pub transport: TransportProtocol,
    /// 传输协议列表（用于同时监听多个协议）
    pub transports: Option<Vec<TransportProtocol>>,
    /// 每个协议的独立地址配置（协议 -> 地址映射）
    /// 如果设置了此映射，每个协议将使用对应的地址
    pub protocol_addresses: Option<std::collections::HashMap<TransportProtocol, String>>,
    /// 序列化格式（默认格式）
    pub default_serialization_format: SerializationFormat,
    /// 默认压缩算法。客户端在 CONNECT 里没有指定压缩时，只有它出现在客户端声明能解的压缩
    /// （metadata `accept_compression`）里才采用，否则不压缩；客户端指定了压缩就用客户端的。
    pub default_compression: CompressionAlgorithm,
    /// 加密算法（默认）
    pub default_encryption: EncryptionAlgorithm,
    /// 最大连接数
    pub max_connections: usize,
    /// 握手超时时间
    pub handshake_timeout: Duration,
    /// 最大并发握手数
    pub max_handshake_concurrency: usize,
    /// 单次连接写入超时时间
    pub write_timeout: Duration,
    /// fanout 发送最大并发度
    pub fanout_concurrency: usize,
    /// 连接超时时间
    pub connection_timeout: Duration,
    /// 默认心跳配置（可以为每个连接配置不同的心跳）
    pub default_heartbeat: HeartbeatConfig,
    /// TLS 配置（用于 HTTPS/WSS/QUIC）
    pub tls: TlsConfig,
    /// 消息大小限制（字节）
    pub max_message_size: usize,
    /// 设备冲突策略（用于多端设备管理）
    pub device_conflict_strategy: DeviceConflictStrategy,
    /// 是否启用认证（如果启用，连接必须通过 token 验证才能收发消息）
    pub auth_enabled: bool,
    /// 认证超时时间（连接建立后，如果在此时间内未完成认证，连接将被关闭）
    pub auth_timeout: Duration,
    /// 停机时下发 CLOSE(`SERVER_SHUTDOWN`) 后等客户端自行断开的宽限，期满还在的连接由服务端关闭。
    /// 默认 [`SERVER_SHUTDOWN_CLOSE_GRACE`](crate::common::shutdown::SERVER_SHUTDOWN_CLOSE_GRACE)（1 s）；
    /// 设为 0 时不等，下发后立即关闭。
    #[serde(default = "default_shutdown_close_grace")]
    pub shutdown_close_grace: Duration,
    /// CONNECT_ACK 里报给客户端的网关实例 ID（`gateway_id`，诊断用）；不设则不报。
    #[serde(default)]
    pub gateway_id: Option<String>,
    /// CONNECT_ACK 里报给客户端的接入点 ID（`ap_id`，诊断用）；不设则不报。
    #[serde(default)]
    pub ap_id: Option<String>,
    /// 握手并发或连接数超限时，`OVERLOADED` 拒绝里建议客户端等待的基数；实际下发的值再加 0–50%
    /// 随机抖动（见 [`crate::common::link::overload_retry_after`]）。默认 2 s。
    #[serde(default = "default_overload_retry_after")]
    pub overload_retry_after: Duration,
}

fn default_shutdown_close_grace() -> Duration {
    crate::common::shutdown::SERVER_SHUTDOWN_CLOSE_GRACE
}

fn default_overload_retry_after() -> Duration {
    Duration::from_secs(2)
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_string(),
            transport: TransportProtocol::WebSocket,
            transports: None,
            protocol_addresses: None,
            default_serialization_format: SerializationFormat::Protobuf,
            default_compression: CompressionAlgorithm::None,
            default_encryption: EncryptionAlgorithm::None,
            max_connections: 10000,
            handshake_timeout: Duration::from_secs(10),
            max_handshake_concurrency: 1024,
            write_timeout: Duration::from_secs(10),
            fanout_concurrency: 256,
            connection_timeout: Duration::from_secs(300),
            default_heartbeat: HeartbeatConfig::default(),
            tls: TlsConfig::none(),
            max_message_size: 10 * 1024 * 1024, // 10MB
            device_conflict_strategy: DeviceConflictStrategy::default(),
            auth_enabled: false,                   // 默认不启用认证
            auth_timeout: Duration::from_secs(30), // 默认认证超时 30 秒
            shutdown_close_grace: default_shutdown_close_grace(),
            gateway_id: None,
            ap_id: None,
            overload_retry_after: default_overload_retry_after(),
        }
    }
}

impl ServerConfig {
    /// 创建新的服务端配置
    pub fn new(bind_address: String) -> Self {
        Self {
            bind_address,
            ..Default::default()
        }
    }

    /// 使用 WebSocket 协议
    pub fn websocket(mut self) -> Self {
        self.transport = TransportProtocol::WebSocket;
        self
    }

    /// 使用 QUIC 协议
    pub fn quic(mut self) -> Self {
        self.transport = TransportProtocol::QUIC;
        self
    }

    /// 使用 TCP 协议
    pub fn tcp(mut self) -> Self {
        self.transport = TransportProtocol::TCP;
        self
    }

    /// 设置默认序列化格式
    pub fn with_format(mut self, format: SerializationFormat) -> Self {
        self.default_serialization_format = format;
        self
    }

    /// 设置默认压缩算法
    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.default_compression = compression;
        self
    }

    /// 设置默认加密算法
    pub fn with_encryption(mut self, encryption: EncryptionAlgorithm) -> Self {
        self.default_encryption = encryption;
        self
    }

    /// 设置最大连接数
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// 设置握手超时时间
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// 设置最大并发握手数
    pub fn with_max_handshake_concurrency(mut self, max: usize) -> Self {
        self.max_handshake_concurrency = max.max(1);
        self
    }

    /// 设置单次连接写入超时时间
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// 设置 fanout 发送最大并发度
    pub fn with_fanout_concurrency(mut self, max: usize) -> Self {
        self.fanout_concurrency = max.max(1);
        self
    }

    /// 启用多协议监听
    pub fn with_protocols(mut self, protocols: Vec<TransportProtocol>) -> Self {
        self.transports = Some(protocols);
        self
    }

    /// 为特定协议设置监听地址
    pub fn with_protocol_address(mut self, protocol: TransportProtocol, address: String) -> Self {
        if self.protocol_addresses.is_none() {
            self.protocol_addresses = Some(std::collections::HashMap::new());
        }
        if let Some(ref mut addresses) = self.protocol_addresses {
            addresses.insert(protocol, address);
        }
        self
    }

    /// 批量设置协议地址映射
    pub fn with_protocol_addresses(
        mut self,
        addresses: std::collections::HashMap<TransportProtocol, String>,
    ) -> Self {
        self.protocol_addresses = Some(addresses);
        self
    }

    /// 获取指定协议的地址
    pub fn get_protocol_address(&self, protocol: &TransportProtocol) -> String {
        let endpoint = if let Some(ref addresses) = self.protocol_addresses
            && let Some(addr) = addresses.get(protocol)
        {
            addr.as_str()
        } else {
            self.bind_address.as_str()
        };

        TransportProtocol::normalize_server_bind_address(endpoint)
    }

    /// 设置默认心跳配置
    pub fn with_heartbeat(mut self, heartbeat: HeartbeatConfig) -> Self {
        self.default_heartbeat = heartbeat;
        self
    }

    /// 设置 TLS 配置
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// 设置连接超时
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// 获取要使用的协议列表
    pub fn get_protocols(&self) -> Vec<TransportProtocol> {
        if let Some(ref protocols) = self.transports {
            protocols.clone()
        } else {
            vec![self.transport]
        }
    }

    /// 设置设备冲突策略
    pub fn with_device_conflict_strategy(mut self, strategy: DeviceConflictStrategy) -> Self {
        self.device_conflict_strategy = strategy;
        self
    }

    /// 启用认证
    ///
    /// 启用后，所有连接必须通过 token 验证才能收发消息
    pub fn enable_auth(mut self) -> Self {
        self.auth_enabled = true;
        self
    }

    /// 禁用认证（默认）
    pub fn disable_auth(mut self) -> Self {
        self.auth_enabled = false;
        self
    }

    /// 设置认证超时时间
    ///
    /// 连接建立后，如果在此时间内未完成认证，连接将被关闭
    /// 默认值为 30 秒
    pub fn with_auth_timeout(mut self, timeout: Duration) -> Self {
        self.auth_timeout = timeout;
        self
    }

    /// 设置停机宽限（见 [`Self::shutdown_close_grace`]）
    pub fn with_shutdown_close_grace(mut self, grace: Duration) -> Self {
        self.shutdown_close_grace = grace;
        self
    }

    /// 设置 CONNECT_ACK 里报给客户端的网关实例 ID（见 [`Self::gateway_id`]）
    pub fn with_gateway_id(mut self, gateway_id: impl Into<String>) -> Self {
        self.gateway_id = Some(gateway_id.into());
        self
    }

    /// 设置 CONNECT_ACK 里报给客户端的接入点 ID（见 [`Self::ap_id`]）
    pub fn with_ap_id(mut self, ap_id: impl Into<String>) -> Self {
        self.ap_id = Some(ap_id.into());
        self
    }

    /// 设置超限拒绝里建议客户端等待的基数（见 [`Self::overload_retry_after`]）
    pub fn with_overload_retry_after(mut self, base: Duration) -> Self {
        self.overload_retry_after = base;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_connection_admission_limits_are_enabled() {
        let config = ServerConfig::default();

        assert_eq!(config.handshake_timeout, Duration::from_secs(10));
        assert_eq!(config.max_handshake_concurrency, 1024);
        assert_eq!(config.write_timeout, Duration::from_secs(10));
        assert_eq!(config.fanout_concurrency, 256);
    }

    #[test]
    fn connection_admission_limits_are_configurable() {
        let config = ServerConfig::default()
            .with_handshake_timeout(Duration::from_secs(3))
            .with_max_handshake_concurrency(32)
            .with_write_timeout(Duration::from_secs(2))
            .with_fanout_concurrency(8);

        assert_eq!(config.handshake_timeout, Duration::from_secs(3));
        assert_eq!(config.max_handshake_concurrency, 32);
        assert_eq!(config.write_timeout, Duration::from_secs(2));
        assert_eq!(config.fanout_concurrency, 8);
    }

    /// 停机宽限默认 1 s、可配置；旧版本序列化出的配置没有这个字段时取默认值。
    #[test]
    fn shutdown_close_grace_defaults_to_one_second_and_is_configurable() {
        let config = ServerConfig::default();
        assert_eq!(config.shutdown_close_grace, Duration::from_secs(1));
        assert_eq!(
            config
                .with_shutdown_close_grace(Duration::from_millis(250))
                .shutdown_close_grace,
            Duration::from_millis(250)
        );

        let mut value = serde_json::to_value(ServerConfig::default()).expect("serialize");
        value
            .as_object_mut()
            .expect("config object")
            .remove("shutdown_close_grace");
        let config: ServerConfig = serde_json::from_value(value).expect("deserialize");
        assert_eq!(config.shutdown_close_grace, Duration::from_secs(1));
    }

    #[test]
    fn derives_bind_addresses_without_client_schemes() {
        let config = ServerConfig::new("ws://0.0.0.0:8080".to_string());

        assert_eq!(
            config.get_protocol_address(&TransportProtocol::WebSocket),
            "0.0.0.0:8080"
        );
        assert_eq!(
            config.get_protocol_address(&TransportProtocol::QUIC),
            "0.0.0.0:8080"
        );
        assert_eq!(
            config.get_protocol_address(&TransportProtocol::TCP),
            "0.0.0.0:8080"
        );
    }

    #[test]
    fn normalizes_explicit_protocol_bind_address() {
        let config = ServerConfig::default()
            .with_protocol_address(TransportProtocol::TCP, "tcp://127.0.0.1:19090".to_string());

        assert_eq!(
            config.get_protocol_address(&TransportProtocol::TCP),
            "127.0.0.1:19090"
        );
    }
}
