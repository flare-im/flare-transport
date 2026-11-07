//! 简单模式客户端构建器
//!
//! 提供最小实现，使用闭包定义消息处理逻辑，适合快速原型开发和学习。
//!
//! ## 特点
//! - **使用闭包处理消息和事件**：无需实现 trait，直接使用闭包
//! - **最小依赖**：只提供基本的消息处理功能
//! - **零配置**：使用默认配置即可运行
//! - **轻量级**：不包含中间件、管道等高级功能
//! - **快速上手**：几行代码即可启动客户端
//!
//! ## 适用场景
//! - 快速原型开发
//! - 学习和测试
//! - 小型应用
//! - 需要完全控制消息处理流程的场景
//!
//! ## 架构说明
//!
//! 简单模式基于 `HybridClient`，共享所有核心能力（多协议支持、序列化协商、心跳检测等），
//! 但消息处理逻辑通过闭包定义，不依赖高级抽象。

#[cfg(not(target_arch = "wasm32"))]
use crate::client::HybridClient;
#[cfg(target_arch = "wasm32")]
use crate::client::WebSocketClient;
use crate::client::builder::{BaseClientBuilderConfig, ClientWrapper};
use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use std::sync::Arc;

/// 客户端消息处理函数类型
pub type ClientMessageHandler = Box<dyn Fn(&Frame) -> Result<()> + Send + Sync>;

/// 客户端事件处理函数类型
pub type ClientEventHandler = Box<dyn Fn(&ConnectionEvent) + Send + Sync>;

/// 简化的客户端观察者
struct SimpleClientObserver {
    message_handler: Option<ClientMessageHandler>,
    event_handler: Option<ClientEventHandler>,
}

impl ConnectionObserver for SimpleClientObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        if let Some(ref handler) = self.event_handler {
            handler(event);
        }

        if let ConnectionEvent::Message(data) = event
            && let Some(ref handler) = self.message_handler
            && let Ok(frame) = crate::common::MessageParser::new(
                crate::common::protocol::SerializationFormat::Protobuf,
                crate::common::compression::CompressionAlgorithm::None,
                crate::common::encryption::EncryptionAlgorithm::None,
            )
            .parse(data)
            && let Err(e) = handler(&frame)
        {
            tracing::error!("消息处理错误: {:?}", e);
        }
    }
}

/// 简化的客户端实例
pub struct SimpleClient {
    wrapper: ClientWrapper,
    observer: Arc<SimpleClientObserver>,
}

impl SimpleClient {
    /// 连接到服务器
    pub async fn connect(&mut self) -> Result<()> {
        self.wrapper
            .add_observer(self.observer.clone() as Arc<dyn ConnectionObserver>)
            .await;
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
    pub async fn is_connected(&self) -> bool {
        self.wrapper.is_connected_async().await
    }

    /// 获取连接 ID
    pub async fn connection_id(&self) -> Option<String> {
        self.wrapper.connection_id_async().await
    }

    /// 获取活动协议
    pub fn active_protocol(&self) -> crate::common::config_types::TransportProtocol {
        self.wrapper.active_protocol()
    }

    /// 获取当前消息解析器快照（协商完成后与 `flare_chat_server` 等 Flare 服务端一致）
    pub async fn parser_snapshot(&self) -> crate::common::MessageParser {
        self.wrapper.parser_snapshot().await
    }

    /// 等待 CONNECT_ACK 协商完成
    pub async fn wait_for_negotiation(&self, timeout: std::time::Duration) -> Result<()> {
        self.wrapper.wait_for_negotiation(timeout).await
    }
}

/// 简单模式客户端构建器
///
/// 提供最小实现，使用闭包定义消息处理逻辑，适合快速原型开发和学习。
///
/// ## 设计原则
///
/// - **公共逻辑统一处理**：基于 `HybridClient`，共享所有核心能力（多协议、序列化协商、心跳等）
/// - **最小抽象**：消息处理通过闭包定义，不依赖 trait 实现
/// - **零配置**：使用默认配置即可运行
///
/// ## 使用方式
///
/// 使用闭包定义消息和事件处理逻辑，无需实现 trait。
pub struct ClientBuilder {
    base: BaseClientBuilderConfig,
    message_handler: Option<ClientMessageHandler>,
    event_handler: Option<ClientEventHandler>,
}

impl ClientBuilder {
    /// 创建新的客户端构建器
    ///
    /// # 参数
    /// - `server_url`: 服务器地址，例如 "ws://127.0.0.1:8080" 或 "quic://127.0.0.1:8080"
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            base: BaseClientBuilderConfig::new(server_url),
            message_handler: None,
            event_handler: None,
        }
    }

    /// 设置消息处理函数
    ///
    /// # 参数
    /// - `handler`: 消息处理函数，接收 Frame
    pub fn on_message<F>(mut self, handler: F) -> Self
    where
        F: Fn(&Frame) -> Result<()> + Send + Sync + 'static,
    {
        self.message_handler = Some(Box::new(handler));
        self
    }

    /// 设置事件处理函数
    ///
    /// # 参数
    /// - `handler`: 事件处理函数，接收 ConnectionEvent
    pub fn on_event<F>(mut self, handler: F) -> Self
    where
        F: Fn(&ConnectionEvent) + Send + Sync + 'static,
    {
        self.event_handler = Some(Box::new(handler));
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

    /// 设置设备信息（CONNECT 协商与设备管理）
    pub fn with_device_info(mut self, device_info: crate::common::device::DeviceInfo) -> Self {
        self.base = self.base.with_device_info(device_info);
        self
    }

    /// 设置 Token（用于认证，如果服务端启用认证，必须提供）
    pub fn with_token(mut self, token: String) -> Self {
        self.base = self.base.with_token(token);
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

    /// 构建客户端
    ///
    /// # 返回
    /// 返回配置好的 SimpleClient 实例
    pub fn build(self) -> Result<SimpleClient> {
        let observer = Arc::new(SimpleClientObserver {
            message_handler: self.message_handler,
            event_handler: self.event_handler,
        });

        let client = {
            #[cfg(not(target_arch = "wasm32"))]
            {
                HybridClient::new(self.base.config)?
            }
            #[cfg(target_arch = "wasm32")]
            {
                WebSocketClient::new(self.base.config)
            }
        };
        let wrapper = ClientWrapper::new(client);

        Ok(SimpleClient { wrapper, observer })
    }
}
