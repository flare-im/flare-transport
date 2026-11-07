//! 客户端构建器基类
//!
//! 提供所有构建器共享的配置方法，体现"公共逻辑统一处理"的设计原则。
//!
//! ## 设计原则
//!
//! - **统一配置接口**：所有构建器（简单模式、观察者模式、Flare 模式）共享相同的配置方法
//! - **减少代码重复**：通过组合模式，让各个构建器复用配置逻辑
//! - **类型安全**：使用 Rust 类型系统确保配置的正确性

use crate::client::config::ClientConfig;
use crate::common::compression::CompressionAlgorithm;
use crate::common::config_types::{HeartbeatConfig, TlsConfig, TransportProtocol};
use crate::common::device::DeviceInfo;
use crate::common::protocol::SerializationFormat;
use std::time::Duration;

/// 客户端构建器基类配置
///
/// 提供所有构建器共享的配置方法
/// 使用组合模式，让各个构建器可以复用这些配置逻辑
pub struct BaseClientBuilderConfig {
    pub config: ClientConfig,
}

impl BaseClientBuilderConfig {
    /// 创建新的构建器配置
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            config: ClientConfig::new(server_url.into()),
        }
    }

    /// 设置传输协议
    #[must_use]
    pub fn with_protocol(mut self, protocol: TransportProtocol) -> Self {
        self.config.transport = protocol;
        self
    }

    /// 启用多协议竞速
    #[must_use]
    pub fn with_protocol_race(mut self, protocols: Vec<TransportProtocol>) -> Self {
        self.config = self.config.with_protocol_race(protocols);
        self
    }

    /// 为特定协议设置服务器地址
    #[must_use]
    pub fn with_protocol_url(mut self, protocol: TransportProtocol, url: String) -> Self {
        self.config = self.config.with_protocol_url(protocol, url);
        self
    }

    /// 设置用户 ID
    #[must_use]
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.config = self.config.with_user_id(user_id);
        self
    }

    /// 设置 Token（用于认证）
    #[must_use]
    pub fn with_token(mut self, token: String) -> Self {
        self.config = self.config.with_token(token);
        self
    }

    /// 设置序列化格式（用于协商，默认 JSON）
    #[must_use]
    pub fn with_format(mut self, format: SerializationFormat) -> Self {
        self.config = self.config.with_format(format);
        self
    }

    /// 设置压缩算法（用于协商，默认 None）
    #[must_use]
    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.config = self.config.with_compression(compression);
        self
    }

    /// 强制指定序列化格式（不进行协商）
    #[must_use]
    pub fn force_format(mut self, format: SerializationFormat) -> Self {
        self.config = self.config.force_format(format);
        self
    }

    /// 强制指定压缩算法（不进行协商）
    #[must_use]
    pub fn force_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.config = self.config.force_compression(compression);
        self
    }

    /// 设置设备信息
    #[must_use]
    pub fn with_device_info(mut self, device_info: DeviceInfo) -> Self {
        self.config = self.config.with_device_info(device_info);
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

    /// 设置单个协议一次握手的时限（传输建立 + CONNECT_ACK，见 [`ClientConfig::connect_timeout`]）
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.config = self.config.with_connect_timeout(timeout);
        self
    }

    /// 设置交错竞速的间隔（见 [`ClientConfig::race_stagger`]）
    #[must_use]
    pub fn with_race_stagger(mut self, stagger: Duration) -> Self {
        self.config = self.config.with_race_stagger(stagger);
        self
    }

    /// 设置这次建连的连接代数（CONNECT metadata `connection_epoch`）
    #[must_use]
    pub fn with_connection_epoch(mut self, epoch: u64) -> Self {
        self.config = self.config.with_connection_epoch(epoch);
        self
    }

    /// 设置上层当前接入清单的 serial（CONNECT metadata `access_serial`）
    #[must_use]
    pub fn with_access_serial(mut self, serial: u64) -> Self {
        self.config = self.config.with_access_serial(serial);
        self
    }

    /// 设置接入点的备用 IP（DNS 失败或结果可疑时才用，见 [`ClientConfig::ip_hints`]）
    #[must_use]
    pub fn with_ip_hints(mut self, ip_hints: Vec<std::net::IpAddr>) -> Self {
        self.config = self.config.with_ip_hints(ip_hints);
        self
    }

    /// 设置 DoH 备用解析（见 [`ClientConfig::doh`]）
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn with_doh(mut self, doh: std::sync::Arc<crate::client::dial::DohCache>) -> Self {
        self.config = self.config.with_doh(doh);
        self
    }

    /// 启用消息路由
    #[must_use]
    pub fn enable_router(mut self) -> Self {
        self.config = self.config.enable_router();
        self
    }
}
