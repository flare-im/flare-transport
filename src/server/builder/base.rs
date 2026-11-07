//! 服务端构建器基类
//!
//! 提供所有构建器共享的配置方法，体现"公共逻辑统一处理"的设计原则。
//!
//! ## 设计原则
//!
//! - **统一配置接口**：所有构建器（简单模式、观察者模式、Flare 模式）共享相同的配置方法
//! - **减少代码重复**：通过组合模式，让各个构建器复用配置逻辑
//! - **类型安全**：使用 Rust 类型系统确保配置的正确性

use crate::common::compression::CompressionAlgorithm;
use crate::common::config_types::{HeartbeatConfig, TlsConfig, TransportProtocol};
use crate::common::device::DeviceConflictStrategy;
use crate::common::encryption::EncryptionAlgorithm;
use crate::common::protocol::SerializationFormat;
use crate::server::config::ServerConfig;
use std::sync::Arc;
use std::time::Duration;

/// 服务端构建器基类
///
/// 提供所有构建器共享的配置方法
/// 使用组合模式，让各个构建器可以复用这些配置逻辑
pub struct BaseServerBuilderConfig {
    pub config: ServerConfig,
    pub authenticator: Option<Arc<dyn crate::server::auth::Authenticator>>,
}

impl BaseServerBuilderConfig {
    /// 创建新的构建器配置
    pub fn new(bind_address: impl Into<String>) -> Self {
        Self {
            config: ServerConfig::new(bind_address.into()),
            authenticator: None,
        }
    }

    /// 设置认证器（如果启用认证，必须提供）
    #[must_use]
    pub fn with_authenticator(
        mut self,
        authenticator: Arc<dyn crate::server::auth::Authenticator>,
    ) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    /// 启用认证
    #[must_use]
    pub fn enable_auth(mut self) -> Self {
        self.config = self.config.enable_auth();
        self
    }

    /// 设置认证超时时间
    #[must_use]
    pub fn with_auth_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_auth_timeout(timeout);
        self
    }

    /// 设置停机宽限：下发 CLOSE(`SERVER_SHUTDOWN`) 后等客户端自行断开多久（默认 1 s）
    #[must_use]
    pub fn with_shutdown_close_grace(mut self, grace: Duration) -> Self {
        self.config = self.config.with_shutdown_close_grace(grace);
        self
    }

    /// 设置 CONNECT_ACK 里报给客户端的网关实例 ID
    #[must_use]
    pub fn with_gateway_id(mut self, gateway_id: impl Into<String>) -> Self {
        self.config = self.config.with_gateway_id(gateway_id);
        self
    }

    /// 设置 CONNECT_ACK 里报给客户端的接入点 ID
    #[must_use]
    pub fn with_ap_id(mut self, ap_id: impl Into<String>) -> Self {
        self.config = self.config.with_ap_id(ap_id);
        self
    }

    /// 设置超限拒绝（`OVERLOADED`）里建议客户端等待的基数
    #[must_use]
    pub fn with_overload_retry_after(mut self, base: Duration) -> Self {
        self.config = self.config.with_overload_retry_after(base);
        self
    }

    /// 设置传输协议
    #[must_use]
    pub fn with_protocol(mut self, protocol: TransportProtocol) -> Self {
        self.config.transport = protocol;
        self
    }

    /// 启用多协议监听
    #[must_use]
    pub fn with_protocols(mut self, protocols: Vec<TransportProtocol>) -> Self {
        self.config = self.config.with_protocols(protocols);
        self
    }

    /// 为特定协议设置监听地址
    #[must_use]
    pub fn with_protocol_address(mut self, protocol: TransportProtocol, address: String) -> Self {
        self.config = self.config.with_protocol_address(protocol, address);
        self
    }

    /// 设置最大连接数
    #[must_use]
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.config = self.config.with_max_connections(max);
        self
    }

    /// 设置握手超时时间
    #[must_use]
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_handshake_timeout(timeout);
        self
    }

    /// 设置最大并发握手数
    #[must_use]
    pub fn with_max_handshake_concurrency(mut self, max: usize) -> Self {
        self.config = self.config.with_max_handshake_concurrency(max);
        self
    }

    /// 设置单次连接写入超时时间
    #[must_use]
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_write_timeout(timeout);
        self
    }

    /// 设置 fanout 发送最大并发度
    #[must_use]
    pub fn with_fanout_concurrency(mut self, max: usize) -> Self {
        self.config = self.config.with_fanout_concurrency(max);
        self
    }

    /// 设置连接超时
    #[must_use]
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_connection_timeout(timeout);
        self
    }

    /// 设置心跳配置
    #[must_use]
    pub fn with_heartbeat(mut self, heartbeat: HeartbeatConfig) -> Self {
        self.config = self.config.with_heartbeat(heartbeat);
        self
    }

    /// 设置 TLS 配置
    #[must_use]
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.config = self.config.with_tls(tls);
        self
    }

    /// 设置默认序列化格式（用于协商，默认 Protobuf）
    #[must_use]
    pub fn with_default_format(mut self, format: SerializationFormat) -> Self {
        self.config = self.config.with_format(format);
        self
    }

    /// 设置默认压缩算法（用于协商，默认 None）。客户端没指定压缩时，只有客户端声明能解它
    /// （CONNECT metadata `accept_compression`）才采用，见 [`ServerConfig::default_compression`]。
    #[must_use]
    pub fn with_default_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.config = self.config.with_compression(compression);
        self
    }

    /// 设置默认加密算法（用于协商，默认 None）
    #[must_use]
    pub fn with_default_encryption(mut self, encryption: EncryptionAlgorithm) -> Self {
        self.config = self.config.with_encryption(encryption);
        self
    }

    /// 设置设备冲突策略
    #[must_use]
    pub fn with_device_conflict_strategy(mut self, strategy: DeviceConflictStrategy) -> Self {
        self.config = self.config.with_device_conflict_strategy(strategy);
        self
    }
}
