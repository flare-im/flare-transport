//! Flare 模式客户端构建器
//!
//! 提供完整功能实现，包含所有 `common` 和 `client` 模块的能力，推荐用于生产环境。
//!
//! ## 特点
//! - **实现 `MessageListener` trait（必需）**：提供统一的消息处理接口
//! - **消息管道**：自动处理序列化、压缩、加密
//! - **中间件支持**：日志、性能监控、验证等
//! - **处理器链**：可组合多个处理器
//! - **序列化协商**：自动协商最佳序列化格式（JSON/Protobuf）和压缩算法（None/Gzip/Zstd）
//! - **心跳管理**：自动心跳和超时管理
//! - **自动重连**：支持断线重连
//! - **多协议支持**：WebSocket + QUIC 双协议竞速
//! - **统一事件处理**：使用 Observer 模式统一处理所有事件（连接事件、消息事件等）
//!
//! ## 适用场景
//! - 生产环境
//! - 需要完整功能的企业应用
//! - 需要高性能和可扩展性的场景
//! - 需要统一消息处理流程的场景
//!
//! ## 架构说明
//!
//! Flare 模式基于 `HybridClient`，使用 `MessagePipeline` 提供统一的消息处理流程。
//! 消息管道支持中间件、自动序列化/压缩、加密等功能，同时保持与底层实现的统一。

#[cfg(not(target_arch = "wasm32"))]
use crate::client::HybridClient;
#[cfg(target_arch = "wasm32")]
use crate::client::WebSocketClient;
use crate::client::builder::{BaseClientBuilderConfig, ClientWrapper};
use crate::common::MessageParser;
use crate::common::config_types::{HeartbeatAppState, HeartbeatConfig};
use crate::common::error::Result;
use crate::common::message::{
    ArcMessageMiddleware, ArcMessageProcessor, MessageContext, MessagePipeline, MessageProcessor,
};
use crate::common::protocol::Frame;
use crate::common::protocol::flare::core::commands::command::Type as CommandType;
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// 消息监听器
///
/// 用户只需要实现这个简单的接口就能处理消息
#[async_trait]
pub trait MessageListener: Send + Sync {
    /// 处理收到的消息
    ///
    /// # 参数
    /// - `frame`: 收到的消息 Frame
    ///
    /// # 返回
    /// - `Ok(Some(Frame))`: 需要发送的响应
    /// - `Ok(None)`: 不需要响应
    /// - `Err`: 处理失败
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        let _ = frame;
        Ok(None)
    }

    /// 连接建立时调用
    async fn on_connect(&self) -> Result<()> {
        Ok(())
    }

    /// 连接断开时调用
    async fn on_disconnect(&self, reason: Option<&str>) -> Result<()> {
        let _ = reason;
        Ok(())
    }

    /// 连接错误时调用
    async fn on_error(&self, error: &str) -> Result<()> {
        let _ = error;
        Ok(())
    }
}

/// Flare 模式客户端构建器
///
/// 提供完整功能实现的客户端构建器
///
/// 用户需要实现 `MessageListener` trait，框架会自动处理消息序列化、压缩、加密和心跳管理
/// - 压缩/解压
/// - 加密/解密
/// - 心跳管理
/// - 自动重连
pub struct FlareClientBuilder {
    base: BaseClientBuilderConfig,
    listener: Option<Arc<dyn MessageListener>>,
    middlewares: Vec<ArcMessageMiddleware>,
    processors: Vec<ArcMessageProcessor>,
    observers: Vec<Arc<dyn ConnectionObserver>>,
}

impl FlareClientBuilder {
    /// 创建新的 Flare 客户端构建器
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            base: BaseClientBuilderConfig::new(server_url),
            listener: None,
            middlewares: Vec::new(),
            processors: Vec::new(),
            observers: Vec::new(),
        }
    }

    /// 设置消息监听器（必须）
    pub fn with_listener(mut self, listener: Arc<dyn MessageListener>) -> Self {
        self.listener = Some(listener);
        self
    }

    /// 添加中间件
    pub fn with_middleware(mut self, middleware: ArcMessageMiddleware) -> Self {
        self.middlewares.push(middleware);
        self
    }

    /// 添加处理器
    pub fn with_processor(mut self, processor: ArcMessageProcessor) -> Self {
        self.processors.push(processor);
        self
    }

    /// 添加连接观察者（可选）
    ///
    /// 观察者会收到连接事件（Connected、Disconnected、Error、Message）
    ///
    /// 添加连接观察者
    ///
    /// 观察者会收到连接事件（Connected、Disconnected、Error、Message）
    ///
    /// ## 示例
    ///
    /// ```rust,no_run
    /// use flare_core::client::builder::flare::FlareClientBuilder;
    /// use flare_core::transport::events::{ConnectionObserver, ConnectionEvent};
    /// use std::sync::Arc;
    ///
    /// struct MyObserver;
    ///
    /// impl ConnectionObserver for MyObserver {
    /// fn on_event(&self, event: &ConnectionEvent) {
    /// match event {
    /// ConnectionEvent::Connected => {
    /// println!("已连接");
    /// }
    /// ConnectionEvent::Disconnected(reason) => {
    /// println!("连接断开: {}", reason);
    /// }
    /// ConnectionEvent::Message(data) => {
    /// // 处理原始消息数据
    /// println!("收到消息: {} bytes", data.len());
    /// }
    /// ConnectionEvent::Error(err) => {
    /// println!("连接错误: {:?}", err);
    /// }
    /// }
    /// }
    /// }
    ///
    /// let builder = FlareClientBuilder::new("ws://127.0.0.1:8080")
    /// .with_observer(Arc::new(MyObserver));
    /// ```
    pub fn with_observer(mut self, observer: Arc<dyn ConnectionObserver>) -> Self {
        self.observers.push(observer);
        self
    }

    // ============================================================
    // 配置方法（委托给 BaseClientBuilderConfig）
    // ============================================================

    /// 设置传输协议
    pub fn with_protocol(
        mut self,
        protocol: crate::common::config_types::TransportProtocol,
    ) -> Self {
        self.base = self.base.with_protocol(protocol);
        self
    }

    /// 启用多协议竞速
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

    /// 强制指定序列化格式
    pub fn force_format(mut self, format: crate::common::protocol::SerializationFormat) -> Self {
        self.base = self.base.force_format(format);
        self
    }

    /// 强制指定压缩算法
    pub fn force_compression(
        mut self,
        compression: crate::common::compression::CompressionAlgorithm,
    ) -> Self {
        self.base = self.base.force_compression(compression);
        self
    }

    /// 设置设备信息
    pub fn with_device_info(mut self, device_info: crate::common::device::DeviceInfo) -> Self {
        self.base = self.base.with_device_info(device_info);
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

    /// 设置协议竞速超时
    pub fn with_race_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_race_timeout(timeout);
        self
    }

    /// 设置重连间隔
    pub fn with_reconnect_interval(mut self, interval: std::time::Duration) -> Self {
        self.base = self.base.with_reconnect_interval(interval);
        self
    }

    /// 设置最大重连次数
    pub fn with_max_reconnect_attempts(mut self, attempts: Option<u32>) -> Self {
        self.base = self.base.with_max_reconnect_attempts(attempts);
        self
    }

    /// 设置断开后发送时是否由客户端自己重连（默认 `true`）。上层自带重连循环时置为 `false`，
    /// 见 [`crate::client::config::ClientConfig::reconnect_on_send`]。
    pub fn with_reconnect_on_send(mut self, enabled: bool) -> Self {
        self.base = self.base.with_reconnect_on_send(enabled);
        self
    }

    /// 设置 Token（用于认证）
    pub fn with_token(mut self, token: String) -> Self {
        self.base = self.base.with_token(token);
        self
    }

    /// 启用消息路由
    pub fn enable_router(mut self) -> Self {
        self.base = self.base.enable_router();
        self
    }

    /// 构建客户端（使用协议竞速）
    pub async fn build_with_race(self) -> Result<FlareClient> {
        let listener = self.listener.ok_or_else(|| {
            crate::common::error::FlareError::protocol_error(
                "MessageListener is required".to_string(),
            )
        })?;

        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;

        // WASM: 先建连再初始化 pipeline，避免在 LocalSet 驱动尚未打开 WS 前卡在 pipeline.await。
        #[cfg(target_arch = "wasm32")]
        let client = WebSocketClient::connect_with_config(self.base.config.clone()).await?;

        // `MessagePipeline` is internally synchronized and cloneable. Keeping an
        // additional mutex around the whole pipeline serialized every inbound
        // frame across the complete async listener call. A slow data/sync handler
        // could therefore block a later send ACK indefinitely, leaving a message
        // stuck in the pending state even though the ACK had reached the socket.
        let pipeline = Arc::new(MessagePipeline::new(PRE_NEGOTIATION_PARSER.clone()));

        for middleware in self.middlewares {
            pipeline.add_middleware(middleware).await;
        }

        let listener_processor = Arc::new(ListenerProcessor {
            listener: listener.clone(),
        });
        pipeline.add_processor(listener_processor).await;

        for processor in self.processors {
            pipeline.add_processor(processor).await;
        }

        #[cfg(not(target_arch = "wasm32"))]
        let client = HybridClient::connect_with_race(self.base.config.clone()).await?;

        let wrapper = ClientWrapper::new(client);

        // 创建观察者（集成消息管道）
        let observer = Arc::new(FlareObserver {
            pipeline: pipeline.clone(),
            listener: listener.clone(),
        });

        let observer_clone = observer.clone();

        // 添加观察者（FlareObserver 和用户提供的 observers）
        // 注意：统一使用 Observer 模式，不再使用 event_handler
        // 所有事件处理都通过 Observer 完成，更符合 IM SDK 的最佳实践
        wrapper.add_observer(observer_clone).await;
        for observer in self.observers {
            wrapper.add_observer(observer).await;
        }

        wrapper
            .wait_for_negotiation(std::time::Duration::from_secs(10))
            .await?;
        let parser = wrapper.parser_snapshot().await;
        pipeline.update_parser(parser).await;

        Ok(FlareClient {
            wrapper,
            pipeline,
            listener,
        })
    }
}

/// Flare 客户端
#[derive(Clone)]
pub struct FlareClient {
    wrapper: ClientWrapper,
    #[allow(dead_code)] // 保留用于未来扩展（如动态更新管道配置）
    pipeline: Arc<MessagePipeline>,
    #[allow(dead_code)] // 保留用于未来扩展（如动态更新监听器）
    listener: Arc<dyn MessageListener>,
}

impl FlareClient {
    /// 发送消息 Frame
    pub async fn send_frame(&self, frame: &Frame) -> Result<()> {
        self.wrapper.send_frame(frame).await
    }

    /// 发送并等待响应（按 message_id 匹配）
    pub async fn send_frame_and_wait(
        &self,
        frame: &Frame,
        timeout: std::time::Duration,
    ) -> Result<Frame> {
        self.wrapper.send_frame_and_wait(frame, timeout).await
    }

    /// 检查是否已连接（native 同步；WASM 请用 [`Self::is_connected_async`]）
    #[cfg(not(target_arch = "wasm32"))]
    pub fn is_connected(&self) -> bool {
        crate::client::runtime::run_client_async(self.is_connected_async())
    }

    /// 检查是否已连接（async，全平台）
    pub async fn is_connected_async(&self) -> bool {
        self.wrapper.is_connected_async().await
    }

    /// 断开连接
    pub async fn disconnect(self) -> Result<()> {
        self.wrapper.disconnect().await
    }

    /// 获取连接 ID（native 同步；WASM 请用 [`Self::connection_id_async`]）
    #[cfg(not(target_arch = "wasm32"))]
    pub fn connection_id(&self) -> Option<String> {
        crate::client::runtime::run_client_async(self.connection_id_async())
    }

    /// 获取连接 ID（async，全平台）
    pub async fn connection_id_async(&self) -> Option<String> {
        self.wrapper.connection_id_async().await
    }

    /// 获取协商后的消息解析器快照
    pub async fn parser_snapshot(&self) -> MessageParser {
        self.wrapper.parser_snapshot().await
    }

    /// 获取活动协议
    pub fn active_protocol(&self) -> crate::common::config_types::TransportProtocol {
        self.wrapper.active_protocol()
    }

    /// 运行期替换心跳策略。
    pub async fn update_heartbeat_config(&self, config: HeartbeatConfig) {
        self.wrapper.update_heartbeat_config(config).await;
    }

    /// 更新应用前后台状态。移动端进入后台时可拉长心跳，回到前台时恢复较短心跳。
    pub async fn set_heartbeat_app_state(&self, state: HeartbeatAppState) {
        self.wrapper.set_heartbeat_app_state(state).await;
    }

    /// 对当前连接做一次即时验活（不改前后台状态）：立即 ping，验活窗口内无 PONG 即断开，
    /// 断开原因为心跳超时，上层按断线重连处理。
    ///
    /// 立即返回，不等客户端锁：发送正在等回执时也能马上生效。没有在跑的心跳时，唤醒留到心跳
    /// 启动后立即验活一次，无害。
    pub fn probe_heartbeat(&self) {
        self.wrapper.probe_heartbeat();
    }

    /// 更新 NAT 空闲超时探测结果。传入 `None` 表示清除探测值。
    pub async fn set_heartbeat_nat_timeout(&self, timeout: Option<std::time::Duration>) {
        self.wrapper.set_heartbeat_nat_timeout(timeout).await;
    }

    /// 当前实际心跳间隔。
    pub async fn heartbeat_effective_interval(&self) -> std::time::Duration {
        self.wrapper.heartbeat_effective_interval().await
    }

    /// 更新消息管道解析器（协商完成后调用）
    pub async fn update_parser(&self, parser: MessageParser) {
        self.pipeline.update_parser(parser).await;
    }

    /// 添加连接观察者
    ///
    /// 观察者会收到连接事件（Connected、Disconnected、Error、Message）
    pub async fn add_observer(&self, observer: Arc<dyn ConnectionObserver>) {
        self.wrapper.add_observer(observer).await;
    }
}

/// Flare 客户端观察者
///
/// 协议竞速时，它在竞速结束之后才挂上：未选中的连接在选中连接发 CONNECT 之前就已与共享状态脱钩并
/// 关闭（见 `HybridClient::connect_with_race`），这里收到的事件都来自选中的连接。
struct FlareObserver {
    pipeline: Arc<MessagePipeline>,
    listener: Arc<dyn MessageListener>,
}

impl ConnectionObserver for FlareObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                info!("[FlareClient] 已连接");
                // 在后台任务中调用 listener.on_connect
                let listener = self.listener.clone();
                crate::client::runtime::spawn_client_task(async move {
                    if let Err(e) = listener.on_connect().await {
                        error!("[FlareClient] on_connect 失败: {}", e);
                    }
                });
            }

            ConnectionEvent::Disconnected(reason) => {
                // 使用 Arc<str> 避免 String clone，减少内存分配
                let reason_arc: Arc<str> = Arc::from(reason.as_str());
                info!("[FlareClient] 连接断开: {}", reason_arc);
                let listener = self.listener.clone();
                crate::client::runtime::spawn_client_task(async move {
                    if let Err(e) = listener.on_disconnect(Some(&reason_arc)).await {
                        error!("[FlareClient] on_disconnect 失败: {}", e);
                    }
                });
            }

            ConnectionEvent::Error(err) => {
                // 错误是终止事件，一律交给监听器。此前协议竞速时按文本把 WebSocket 协议错误当作关闭
                // 未选中连接的正常错误吞掉，而未选中的连接根本到不了这里（见结构体说明）：被吞掉的是
                // 选中连接自己的终止性错误，上层既收不到断开也收不到错误，关掉了发送前重连的上层
                // （IM SDK）就不再重连。
                let err_str = format!("{:?}", err);

                // 连接丢失错误：可能是网络问题或服务器关闭连接
                let is_connection_lost = err_str.contains("connection lost")
                    || err_str.contains("connection closed")
                    || err_str.contains("Connection reset");

                if is_connection_lost {
                    // 连接丢失错误，记录为 warn 级别并通知 listener
                    // 注意：底层客户端（WebSocketClient/QUICClient）会自动尝试重连
                    warn!("[FlareClient] 连接丢失: {:?}", err);
                    info!("[FlareClient] 下一次发送时底层客户端会尝试重连（除非关掉了发送前重连）");
                    let listener = self.listener.clone();
                    // 使用 Arc<str> 避免 String clone，减少内存分配
                    let err_str_arc: Arc<str> = Arc::from(err_str.as_str());
                    crate::client::runtime::spawn_client_task(async move {
                        if let Err(e) = listener.on_error(&err_str_arc).await {
                            error!("[FlareClient] on_error 失败: {}", e);
                        }
                    });
                } else {
                    // 其他错误，正常记录并通知 listener
                    warn!("[FlareClient] 连接错误: {:?}", err);
                    let listener = self.listener.clone();
                    // 使用 Arc<str> 避免 String clone，减少内存分配
                    let err_str_arc: Arc<str> = Arc::from(err_str.as_str());
                    crate::client::runtime::spawn_client_task(async move {
                        if let Err(e) = listener.on_error(&err_str_arc).await {
                            error!("[FlareClient] on_error 失败: {}", e);
                        }
                    });
                }
            }

            ConnectionEvent::Message(data) => {
                // 先尝试用 PRE_NEGOTIATION_PARSER 解析，检查是否是 CONNECT_ACK 消息
                // CONNECT_ACK 消息必须使用 PRE_NEGOTIATION_PARSER（JSON、不压缩、不加密）
                use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
                use crate::common::protocol::flare::core::commands::system_command::Type as SysType;

                let pipeline = self.pipeline.clone();
                let data = data.clone();
                crate::client::runtime::spawn_client_task(async move {
                    // 1. 先尝试用 PRE_NEGOTIATION_PARSER 解析，检查是否是 CONNECT_ACK
                    if let Ok(frame) = PRE_NEGOTIATION_PARSER.parse(&data)
                        && let Some(cmd) = &frame.command
                        && let Some(CommandType::System(sys_cmd)) = &cmd.r#type
                        && sys_cmd.r#type == SysType::ConnectAck as i32
                    {
                        // 这是 CONNECT_ACK 消息，需要更新 MessagePipeline 的 parser
                        // 从 CONNECT_ACK 中提取协商结果
                        let format =
                            crate::common::protocol::SerializationFormat::try_from(sys_cmd.format)
                                .unwrap_or(crate::common::protocol::SerializationFormat::Json);
                        let compression =
                            crate::common::compression::CompressionAlgorithm::from_str(
                                &sys_cmd.compression,
                            )
                            .unwrap_or(crate::common::compression::CompressionAlgorithm::None);
                        let encryption = crate::common::encryption::EncryptionAlgorithm::from_str(
                            &sys_cmd.encryption,
                        )
                        .unwrap_or(crate::common::encryption::EncryptionAlgorithm::None);

                        // 更新 MessagePipeline 的 parser
                        {
                            let compression_clone = compression.clone();
                            let encryption_clone = encryption.clone();
                            let new_parser =
                                crate::common::MessageParser::new(format, compression, encryption);
                            pipeline.update_parser(new_parser).await;
                            debug!(
                                "[FlareObserver] 已更新 MessagePipeline 的 parser: format={:?}, compression={:?}, encryption={:?}",
                                format, compression_clone, encryption_clone
                            );
                        }

                        // 使用 PRE_NEGOTIATION_PARSER 解析的 frame 继续处理
                        match pipeline.process_frame(&frame, None).await {
                            Ok(Some(_response_data)) => {
                                debug!(
                                    "[FlareClient] 消息管道返回响应，但客户端无法自动发送，需要用户手动处理"
                                );
                            }
                            Ok(None) => {
                                debug!("[FlareClient] CONNECT_ACK 处理完成，无需响应");
                            }
                            Err(e) => {
                                error!("[FlareClient] CONNECT_ACK 处理失败: {}", e);
                            }
                        }
                        return;
                    }

                    // 2. 如果不是 CONNECT_ACK，使用 MessagePipeline 的 parser 解析
                    match pipeline.process_raw(&data, None).await {
                        Ok(Some(_response_data)) => {
                            debug!(
                                "[FlareClient] 消息管道返回响应，但客户端无法自动发送，需要用户手动处理"
                            );
                            // 注意：这里无法直接发送响应，因为需要访问 client
                            // 用户应该在 listener.on_message 中返回响应 Frame
                        }
                        Ok(None) => {
                            debug!("[FlareClient] 消息处理完成，无需响应");
                        }
                        Err(e) => {
                            error!("[FlareClient] 消息管道处理失败: {}", e);
                        }
                    }
                });
            }
        }
    }
}

/// 监听器处理器
struct ListenerProcessor {
    listener: Arc<dyn MessageListener>,
}

#[async_trait]
impl MessageProcessor for ListenerProcessor {
    async fn process(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        self.listener.on_message(&ctx.frame).await
    }

    fn name(&self) -> &str {
        "ListenerProcessor"
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::common::protocol::{FrameBuilder, Reliability};
    use tokio::sync::{Mutex, oneshot};

    struct SelectivelyBlockingListener {
        first_entered: Mutex<Option<oneshot::Sender<()>>>,
        first_release: Mutex<Option<oneshot::Receiver<()>>>,
        second_seen: Mutex<Option<oneshot::Sender<()>>>,
    }

    #[async_trait]
    impl MessageListener for SelectivelyBlockingListener {
        async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
            match frame.message_id.as_str() {
                "slow-data-response" => {
                    if let Some(tx) = self.first_entered.lock().await.take() {
                        let _ = tx.send(());
                    }
                    if let Some(rx) = self.first_release.lock().await.take() {
                        let _ = rx.await;
                    }
                }
                "send-ack" => {
                    if let Some(tx) = self.second_seen.lock().await.take() {
                        let _ = tx.send(());
                    }
                }
                _ => {}
            }
            Ok(None)
        }
    }

    #[tokio::test]
    async fn slow_inbound_frame_does_not_block_later_send_ack() {
        let parser = MessageParser::json();
        let pipeline = Arc::new(MessagePipeline::new(parser.clone()));
        let (first_entered_tx, first_entered_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = oneshot::channel();
        let (second_seen_tx, second_seen_rx) = oneshot::channel();
        let listener = Arc::new(SelectivelyBlockingListener {
            first_entered: Mutex::new(Some(first_entered_tx)),
            first_release: Mutex::new(Some(first_release_rx)),
            second_seen: Mutex::new(Some(second_seen_tx)),
        });
        pipeline
            .add_processor(Arc::new(ListenerProcessor {
                listener: listener.clone(),
            }))
            .await;
        let observer = FlareObserver { pipeline, listener };

        let slow = FrameBuilder::new()
            .with_message_id("slow-data-response".to_string())
            .with_reliability(Reliability::BestEffort)
            .build();
        let ack = FrameBuilder::new()
            .with_message_id("send-ack".to_string())
            .with_reliability(Reliability::BestEffort)
            .build();

        observer.on_event(&ConnectionEvent::Message(
            parser.serialize(&slow).expect("serialize slow frame"),
        ));
        first_entered_rx
            .await
            .expect("slow frame should enter listener");

        observer.on_event(&ConnectionEvent::Message(
            parser.serialize(&ack).expect("serialize ACK frame"),
        ));
        tokio::time::timeout(std::time::Duration::from_millis(250), second_seen_rx)
            .await
            .expect("ACK must not wait behind an unrelated slow frame")
            .expect("ACK listener should remain alive");

        let _ = first_release_tx.send(());
    }

    /// 记下 `on_error` 收到的错误文本。
    #[derive(Default)]
    struct ErrorRecorder {
        errors: std::sync::Mutex<Vec<String>>,
        seen: tokio::sync::Notify,
    }

    #[async_trait]
    impl MessageListener for ErrorRecorder {
        async fn on_message(&self, _frame: &Frame) -> Result<Option<Frame>> {
            Ok(None)
        }

        async fn on_error(&self, error: &str) -> Result<()> {
            self.errors.lock().unwrap().push(error.to_string());
            self.seen.notify_one();
            Ok(())
        }
    }

    fn observer_with(listener: Arc<ErrorRecorder>) -> FlareObserver {
        FlareObserver {
            pipeline: Arc::new(MessagePipeline::new(MessageParser::json())),
            listener,
        }
    }

    /// WebSocket 协议错误是当前连接的终止性错误，必须交给监听器：吞掉它，上层既收不到断开也收不到
    /// 错误，关掉发送前重连的上层就再也不会重连。协议竞速时也一样：观察者挂上时竞速已经结束，
    /// 错误只可能来自选中的连接。
    #[tokio::test]
    async fn websocket_protocol_errors_reach_the_listener() {
        let protocol_error = crate::common::error::FlareError::connection_failed(
            "WebSocket protocol error: Encountered invalid opcode: 3".to_string(),
        );

        let listener = Arc::new(ErrorRecorder::default());
        let observer = observer_with(listener.clone());
        observer.on_event(&ConnectionEvent::Error(protocol_error));
        tokio::time::timeout(std::time::Duration::from_secs(1), listener.seen.notified())
            .await
            .expect("the protocol error must reach the listener");
        let errors = listener.errors.lock().unwrap().clone();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("WebSocket protocol error"), "{errors:?}");
    }
}
