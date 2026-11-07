//! 观察者模式客户端构建器
//!
//! 提供基本功能实现，使用 `ConnectionObserver` trait 处理连接事件和消息。
//!
//! ## 特点
//! - **实现 `ConnectionObserver` trait**：自定义消息和事件处理
//! - **事件处理**：支持自定义事件处理器
//! - **消息路由**：支持消息路由功能
//! - **灵活扩展**：可以添加自定义的观察器和处理器
//!
//! ## 适用场景
//! - 需要自定义消息处理逻辑但不需要完整功能集
//! - 需要事件驱动的架构
//! - 需要消息路由功能
//!
//! ## 架构说明
//!
//! 观察者模式基于 `HybridClient`，使用 `ConnectionObserver` trait 处理所有连接事件。
//! 观察者可以处理消息、连接建立、断开、错误等事件，提供灵活的事件驱动架构。

use crate::client::HybridClient;
use crate::client::builder::{BaseClientBuilderConfig, ClientWrapper};
use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::transport::events::ConnectionObserver;
use std::sync::Arc;

/// 观察者模式客户端构建器
///
/// 提供基本功能实现，使用 `ConnectionObserver` trait 处理连接事件和消息。
///
/// ## 设计原则
///
/// - **公共逻辑统一处理**：基于 `HybridClient`，共享所有核心能力
/// - **事件驱动**：使用 `ConnectionObserver` trait 处理所有连接事件
/// - **灵活扩展**：支持自定义观察器和事件处理器
///
/// ## 使用方式
///
/// 用户只需要实现 `ConnectionObserver` trait，处理消息、连接建立、断开、错误等事件。
pub struct ObserverClientBuilder {
    base: BaseClientBuilderConfig,
    observer: Option<Arc<dyn ConnectionObserver>>,
    event_handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
}

impl ObserverClientBuilder {
    /// 创建新的观察者模式构建器
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            base: BaseClientBuilderConfig::new(server_url),
            observer: None,
            event_handler: None,
        }
    }

    /// 设置事件处理器（可选，用于自定义业务逻辑）
    pub fn with_event_handler(
        mut self,
        event_handler: Arc<dyn crate::client::events::handler::ClientEventHandler>,
    ) -> Self {
        self.event_handler = Some(event_handler);
        self
    }

    /// 设置观察者（必须）
    pub fn with_observer(mut self, observer: Arc<dyn ConnectionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// 设置传输协议
    pub fn with_protocol(
        mut self,
        protocol: crate::common::config_types::TransportProtocol,
    ) -> Self {
        self.base = self.base.with_protocol(protocol);
        self
    }

    /// 启用多协议竞速
    ///
    /// 第一个建好网络连接的协议胜出，不等其余协议；列表顺序是发起顺序
    /// （见 [`crate::client::config::ClientConfig::with_protocol_race`]）
    pub fn with_protocol_race(
        mut self,
        protocols: Vec<crate::common::config_types::TransportProtocol>,
    ) -> Self {
        self.base = self.base.with_protocol_race(protocols);
        self
    }

    /// 为特定协议设置服务器地址
    pub fn with_protocol_url(
        mut self,
        protocol: crate::common::config_types::TransportProtocol,
        url: String,
    ) -> Self {
        self.base = self.base.with_protocol_url(protocol, url);
        self
    }

    /// 设置用户 ID
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.base = self.base.with_user_id(user_id);
        self
    }

    /// 设置序列化格式
    pub fn with_format(mut self, format: crate::common::protocol::SerializationFormat) -> Self {
        self.base = self.base.with_format(format);
        self
    }

    /// 设置压缩算法
    pub fn with_compression(
        mut self,
        compression: crate::common::compression::CompressionAlgorithm,
    ) -> Self {
        self.base = self.base.with_compression(compression);
        self
    }

    /// 设置心跳配置
    pub fn with_heartbeat(
        mut self,
        heartbeat: crate::common::config_types::HeartbeatConfig,
    ) -> Self {
        self.base = self.base.with_heartbeat(heartbeat);
        self
    }

    /// 设置 TLS 配置
    pub fn with_tls(mut self, tls: crate::common::config_types::TlsConfig) -> Self {
        self.base = self.base.with_tls(tls);
        self
    }

    /// 设置连接超时
    pub fn with_connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_connect_timeout(timeout);
        self
    }

    /// 设置交错竞速的间隔（上一个协议这么久还没完成握手就发起下一个，默认 300 ms）
    pub fn with_race_stagger(mut self, stagger: std::time::Duration) -> Self {
        self.base = self.base.with_race_stagger(stagger);
        self
    }

    /// 启用消息路由
    ///
    /// 启用后，可以通过 ClientCore 的 router 方法注册消息处理器
    pub fn enable_router(mut self) -> Self {
        self.base = self.base.enable_router();
        self
    }

    /// 设置设备信息（用于协商和设备管理）
    pub fn with_device_info(mut self, device_info: crate::common::device::DeviceInfo) -> Self {
        self.base = self.base.with_device_info(device_info);
        self
    }

    /// 设置 Token（用于认证，如果服务端启用认证，必须提供）
    pub fn with_token(mut self, token: String) -> Self {
        self.base = self.base.with_token(token);
        self
    }

    /// 交错竞速建连后返回客户端（返回时已经收到 CONNECT_ACK）。
    ///
    /// 观察者在收到 CONNECT_ACK 时挂到胜出的连接上，落败连接的事件到不了它。失败时错误里是服务端
    /// 拒绝的类别，或各协议的失败汇总（类型化的结果见 [`HybridClient::connect_with_race`]）。
    pub async fn build_with_race(self) -> Result<ObserverClient> {
        let observer = self.observer.ok_or_else(|| {
            crate::common::error::FlareError::general_error("Observer is required")
        })?;

        let mut client = HybridClient::new(self.base.config)?;
        if let Some(event_handler) = self.event_handler {
            client.core_mut().set_event_handler(Some(event_handler));
        }
        client
            .core()
            .add_observer(Arc::clone(&observer) as Arc<dyn ConnectionObserver>);
        crate::client::Client::connect(&mut client).await?;

        Ok(ObserverClient {
            wrapper: ClientWrapper::new(client),
            observer: None,
        })
    }

    /// 构建客户端
    pub fn build(self) -> Result<ObserverClient> {
        let observer = self.observer.ok_or_else(|| {
            crate::common::error::FlareError::general_error("Observer is required")
        })?;

        let mut client = HybridClient::new(self.base.config)?;

        // 设置事件处理器（如果提供）
        if let Some(event_handler) = self.event_handler {
            client.core_mut().set_event_handler(Some(event_handler));
        }

        let wrapper = ClientWrapper::new(client);

        Ok(ObserverClient {
            wrapper,
            observer: Some(observer),
        })
    }
}

/// 观察者模式客户端实例
pub struct ObserverClient {
    wrapper: ClientWrapper,
    observer: Option<Arc<dyn ConnectionObserver>>,
}

impl ObserverClient {
    /// 连接到服务器
    pub async fn connect(&mut self) -> Result<()> {
        // 先添加观察者（如果还未添加）
        if let Some(observer) = &self.observer {
            self.wrapper
                .add_observer(Arc::clone(observer) as Arc<dyn ConnectionObserver>)
                .await;
        }

        self.wrapper.connect().await
    }

    /// 断开连接
    pub async fn disconnect(&mut self) -> Result<()> {
        self.wrapper.disconnect().await
    }

    /// 发送消息
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        self.wrapper.send_frame(frame).await
    }

    /// 发送并等待响应（按 message_id 匹配）
    pub async fn send_frame_and_wait(
        &mut self,
        frame: &Frame,
        timeout: std::time::Duration,
    ) -> Result<Frame> {
        self.wrapper.send_frame_and_wait(frame, timeout).await
    }

    /// 检查连接状态
    pub fn is_connected(&self) -> bool {
        crate::client::runtime::run_client_async(self.wrapper.is_connected_async())
    }

    /// 获取连接 ID
    pub fn connection_id(&self) -> Option<String> {
        crate::client::runtime::run_client_async(self.wrapper.connection_id_async())
    }

    /// 获取活动协议
    pub fn active_protocol(&self) -> crate::common::config_types::TransportProtocol {
        self.wrapper.active_protocol()
    }
}
