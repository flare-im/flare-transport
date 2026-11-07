//! Flare 模式服务端构建器
//!
//! 提供完整功能实现，包含所有 `common` 和 `server` 模块的能力，推荐用于生产环境。
//!
//! ## 特点
//! - **实现 `ServerEventHandler` trait（必需）**：提供细化的命令处理方法
//! - **自动消息路由**：`ServerMessageWrapper` 自动将消息路由到对应的处理方法
//! - **自动 ACK 处理**：如果 handler 返回 `None`，框架自动发送 ACK
//! - **错误处理**：处理失败时自动发送错误 ACK，确保客户端能收到响应
//! - **设备管理**：完整的设备冲突策略和多端管理
//! - **认证机制**：JWT Token 认证
//! - **心跳检测**：自动心跳和超时管理
//! - **多协议支持**：WebSocket + QUIC 双协议
//! - **序列化协商**：自动协商最佳序列化格式（JSON/Protobuf）和压缩算法（None/Gzip/Zstd）
//!
//! ## 适用场景
//! - 生产环境
//! - 需要完整功能的企业应用
//! - 需要高性能和可扩展性的场景
//! - 需要统一消息处理流程的场景
//!
//! ## 架构说明
//!
//! Flare 模式基于 `HybridServer`，使用 `ServerMessageWrapper` 作为消息处理器。
//! `ServerMessageWrapper` 集成了所有核心功能：
//! - 自动消息路由到 `ServerEventHandler` 的对应方法
//! - 自动 ACK 处理和错误响应
//! - 设备管理和连接生命周期管理
//! - 所有模式共享的底层能力（多协议、序列化协商、心跳等）

use crate::common::error::Result;
use crate::common::message::{ArcMessageMiddleware, ArcMessageProcessor};
use crate::common::protocol::Frame;
use crate::server::HybridServer;
use crate::server::builder::{BaseServerBuilderConfig, ServerWrapper};
use crate::server::connection::ConnectionManager;
use crate::server::events::handler::ServerEventHandler;
use crate::server::handle::ServerHandle;
use std::sync::Arc;
use tracing::{error, info};

/// Flare 模式服务端构建器
///
/// 提供完整功能实现，是最强大的服务端构建模式。
///
/// ## 设计原则
///
/// - **公共逻辑统一处理**：基于 `HybridServer`，共享所有核心能力
/// - **自动消息路由**：`ServerMessageWrapper` 自动将消息路由到 `ServerEventHandler` 的对应方法
/// - **自动 ACK 处理**：如果 handler 返回 `None`，框架自动发送 ACK
/// - **错误处理**：处理失败时自动发送错误 ACK，确保客户端能收到响应
///
/// ## 使用方式
///
/// 用户只需要实现 `ServerEventHandler` trait，框架会自动处理：
/// - 消息路由到对应的方法（handle_message、handle_ack、handle_notification_command 等）
/// - ACK 和错误响应
/// - 连接生命周期管理
/// - 设备管理和认证
pub struct FlareServerBuilder {
    base: BaseServerBuilderConfig,
    event_handler: Arc<dyn ServerEventHandler>,
    connection_manager: Option<Arc<ConnectionManager>>,
    device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    middlewares: Vec<ArcMessageMiddleware>,
    processors: Vec<ArcMessageProcessor>,
}

impl FlareServerBuilder {
    /// 创建新的 Flare 服务端构建器
    ///
    /// # 参数
    /// - `bind_address`: 绑定地址
    /// - `event_handler`: 事件处理器（必须），用户只需要实现 `ServerEventHandler` 的 `handle_message` 方法即可
    ///
    /// # 示例
    /// ```rust,ignore
    /// use flare_core::server::events::handler::ServerEventHandler;
    /// use flare_core::common::protocol::PayloadCommand;
    /// use flare_core::common::error::Result;
    /// use flare_core::common::protocol::Frame;
    /// use async_trait::async_trait;
    /// use std::sync::Arc;
    ///
    /// struct MyHandler;
    ///
    /// #[async_trait]
    /// impl ServerEventHandler for MyHandler {
    /// async fn handle_message(
    /// &self,
    /// command: &PayloadCommand,
    /// connection_id: &str,
    /// ) -> Result<Option<Frame>> {
    /// // 处理消息
    /// Ok(None)
    /// }
    /// }
    ///
    /// let server = FlareServerBuilder::new("0.0.0.0:8080", Arc::new(MyHandler))
    /// .build()?;
    /// ```
    pub fn new(
        bind_address: impl Into<String>,
        event_handler: Arc<dyn ServerEventHandler>,
    ) -> Self {
        Self {
            base: BaseServerBuilderConfig::new(bind_address),
            event_handler,
            connection_manager: None,
            device_manager: None,
            middlewares: Vec::new(),
            processors: Vec::new(),
        }
    }

    /// 设置连接管理器（可选）
    pub fn with_connection_manager(mut self, manager: Arc<ConnectionManager>) -> Self {
        self.connection_manager = Some(manager);
        self
    }

    /// 设置设备管理器（可选）
    pub fn with_device_manager(
        mut self,
        device_manager: Arc<crate::server::device::DeviceManager>,
    ) -> Self {
        self.device_manager = Some(device_manager);
        self
    }

    /// 设置认证器（可选）
    pub fn with_authenticator(
        mut self,
        authenticator: Arc<dyn crate::server::auth::Authenticator>,
    ) -> Self {
        self.base = self.base.with_authenticator(authenticator);
        self
    }

    /// 启用认证
    pub fn enable_auth(mut self) -> Self {
        self.base = self.base.enable_auth();
        self
    }

    /// 设置认证超时时间
    pub fn with_auth_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_auth_timeout(timeout);
        self
    }

    /// 设置停机宽限：下发 CLOSE(`SERVER_SHUTDOWN`) 后等客户端自行断开多久（默认 1 s）
    pub fn with_shutdown_close_grace(mut self, grace: std::time::Duration) -> Self {
        self.base = self.base.with_shutdown_close_grace(grace);
        self
    }

    /// 设置 CONNECT_ACK 里报给客户端的网关实例 ID（诊断用）
    pub fn with_gateway_id(mut self, gateway_id: impl Into<String>) -> Self {
        self.base = self.base.with_gateway_id(gateway_id);
        self
    }

    /// 设置 CONNECT_ACK 里报给客户端的接入点 ID（诊断用）
    pub fn with_ap_id(mut self, ap_id: impl Into<String>) -> Self {
        self.base = self.base.with_ap_id(ap_id);
        self
    }

    /// 设置握手并发或连接数超限时 `OVERLOADED` 拒绝里建议客户端等待的基数（再加 0–50% 抖动）
    pub fn with_overload_retry_after(mut self, base: std::time::Duration) -> Self {
        self.base = self.base.with_overload_retry_after(base);
        self
    }

    // ============================================================
    // 配置方法（委托给 BaseServerBuilderConfig）
    // ============================================================

    /// 设置传输协议
    pub fn with_protocol(
        mut self,
        protocol: crate::common::config_types::TransportProtocol,
    ) -> Self {
        self.base = self.base.with_protocol(protocol);
        self
    }

    /// 启用多协议监听
    pub fn with_protocols(
        mut self,
        protocols: Vec<crate::common::config_types::TransportProtocol>,
    ) -> Self {
        self.base = self.base.with_protocols(protocols);
        self
    }

    /// 为特定协议设置监听地址
    pub fn with_protocol_address(
        mut self,
        protocol: crate::common::config_types::TransportProtocol,
        address: String,
    ) -> Self {
        self.base = self.base.with_protocol_address(protocol, address);
        self
    }

    /// 设置默认序列化格式
    pub fn with_default_format(
        mut self,
        format: crate::common::protocol::SerializationFormat,
    ) -> Self {
        self.base = self.base.with_default_format(format);
        self
    }

    /// 设置默认压缩算法
    pub fn with_default_compression(
        mut self,
        compression: crate::common::compression::CompressionAlgorithm,
    ) -> Self {
        self.base = self.base.with_default_compression(compression);
        self
    }

    /// 设置默认加密算法
    pub fn with_default_encryption(
        mut self,
        encryption: crate::common::encryption::EncryptionAlgorithm,
    ) -> Self {
        self.base = self.base.with_default_encryption(encryption);
        self
    }

    /// 设置最大连接数
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.base = self.base.with_max_connections(max);
        self
    }

    /// 设置握手超时时间
    pub fn with_handshake_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_handshake_timeout(timeout);
        self
    }

    /// 设置最大并发握手数
    pub fn with_max_handshake_concurrency(mut self, max: usize) -> Self {
        self.base = self.base.with_max_handshake_concurrency(max);
        self
    }

    /// 设置单次连接写入超时时间
    pub fn with_write_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_write_timeout(timeout);
        self
    }

    /// 设置 fanout 发送最大并发度
    pub fn with_fanout_concurrency(mut self, max: usize) -> Self {
        self.base = self.base.with_fanout_concurrency(max);
        self
    }

    /// 设置连接超时
    pub fn with_connection_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_connection_timeout(timeout);
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

    /// 设置设备冲突策略
    pub fn with_device_conflict_strategy(
        mut self,
        strategy: crate::common::device::DeviceConflictStrategy,
    ) -> Self {
        self.base = self.base.with_device_conflict_strategy(strategy);
        self
    }

    /// 添加中间件（用于消息处理管道）
    ///
    /// 中间件会在消息处理前后执行，可以用于日志、监控、验证等。
    /// 中间件按添加顺序执行，优先级高的中间件会先执行。
    ///
    /// # 参数
    /// - `middleware`: 中间件实例
    ///
    /// # 示例
    /// ```rust,ignore
    /// use flare_core::common::message::{LoggingMiddleware, LogLevel};
    ///
    /// FlareServerBuilder::new("0.0.0.0:8080", handler)
    /// .with_middleware(Arc::new(LoggingMiddleware::new("ServerLogging")
    /// .with_level(LogLevel::Info)))
    /// .build()?;
    /// ```
    pub fn with_middleware(mut self, middleware: ArcMessageMiddleware) -> Self {
        self.middlewares.push(middleware);
        self
    }

    /// 添加处理器（用于消息处理管道）
    ///
    /// 处理器用于处理具体的业务逻辑。
    /// 如果处理器返回响应，后续处理器不会执行。
    ///
    /// # 参数
    /// - `processor`: 处理器实例
    pub fn with_processor(mut self, processor: ArcMessageProcessor) -> Self {
        self.processors.push(processor);
        self
    }

    /// 构建服务端
    ///
    /// # 错误处理
    /// - 如果配置无效（如启用了认证但未提供认证器），返回配置错误
    /// - 如果服务器初始化失败，返回相应的错误
    ///
    /// # 返回
    /// - `Ok(FlareServer)` - 成功构建的服务端实例
    /// - `Err(FlareError)` - 构建失败的错误信息
    ///
    /// Flare 模式使用 `ServerMessageWrapper` 作为消息处理器，它：
    /// - 集成了 `ServerEventHandler` 的所有功能，自动路由消息到对应的处理方法
    /// - 自动处理 ACK、错误响应等基础功能
    /// - 支持设备管理、连接管理等高级特性
    pub fn build(self) -> Result<FlareServer> {
        // 验证配置（使用公共验证逻辑）
        crate::server::builder::common::validate_auth_config(
            &self.base.config,
            &self.base.authenticator,
        )?;

        // 创建消息解析器（使用公共创建逻辑）
        // let parser = crate::server::builder::common::create_message_parser(&self.base.config);

        info!(
            "[FlareServerBuilder] 开始构建服务端: bind_address={}, protocols={:?}, format={:?}, compression={:?}",
            self.base.config.bind_address,
            self.base.config.get_protocols(),
            self.base.config.default_serialization_format,
            self.base.config.default_compression
        );

        let middleware_count = self.middlewares.len();
        let processor_count = self.processors.len();

        let server = HybridServer::with_connection_manager_and_pipeline(
            self.base.config,
            self.connection_manager,
            self.device_manager,
            Some(self.event_handler.clone()),
            self.base.authenticator,
            self.middlewares,
            self.processors,
        )
        .map_err(|e| {
            error!("[FlareServerBuilder] 构建服务端失败: {}", e);
            e
        })?;

        if middleware_count > 0 || processor_count > 0 {
            info!(
                "[FlareServerBuilder] 已配置 {} 个中间件和 {} 个处理器",
                middleware_count, processor_count
            );
        }

        info!("[FlareServerBuilder] 服务端构建成功");
        Ok(FlareServer {
            wrapper: ServerWrapper::new(server),
            event_handler: self.event_handler,
        })
    }
}

/// Flare 服务端
pub struct FlareServer {
    wrapper: ServerWrapper,
    #[allow(dead_code)] // 保留用于未来扩展（如动态更新事件处理器）
    event_handler: Arc<dyn ServerEventHandler>,
}

impl FlareServer {
    /// 启动服务端
    pub async fn start(&self) -> Result<()> {
        self.wrapper.start().await
    }

    /// 停止服务端
    pub async fn stop(&self) -> Result<()> {
        self.wrapper.stop().await
    }

    /// 检查服务端是否运行
    pub fn is_running(&self) -> bool {
        self.wrapper.is_running()
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        self.wrapper.connection_count()
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        self.wrapper.user_count()
    }

    /// 向指定连接发送消息
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        self.wrapper.send_to(connection_id, frame).await
    }

    /// 向（租户，用户）的所有连接发送消息；其他租户里同名用户的连接收不到
    pub async fn send_to_user(
        &self,
        owner: &crate::server::device::DeviceOwner,
        frame: &Frame,
    ) -> Result<()> {
        self.wrapper.send_to_user(owner, frame).await
    }

    /// 广播消息到所有连接
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        self.wrapper.broadcast(frame).await
    }

    /// 广播消息到所有连接，排除指定连接
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        self.wrapper
            .broadcast_except(frame, exclude_connection_id)
            .await
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        self.wrapper.disconnect(connection_id).await
    }

    /// 获取协议列表
    pub fn protocols(&self) -> Vec<crate::common::config_types::TransportProtocol> {
        self.wrapper.protocols()
    }

    /// 获取 ServerHandle 组件（用于创建 DefaultServerHandle）
    ///
    /// # 返回
    /// 返回 ConnectionManagerTrait
    pub fn get_server_handle_components(
        &self,
    ) -> Option<Arc<dyn crate::server::connection::ConnectionManagerTrait>> {
        self.wrapper.get_server_handle_components()
    }

    /// 获取 ServerHandle（用于消息发送和连接管理）
    pub fn get_server_handle(&self) -> Option<Arc<dyn ServerHandle>> {
        self.wrapper.get_server_handle()
    }
}
