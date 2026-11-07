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
//! - **多协议支持**：WebSocket + QUIC 交错竞速（见 [`crate::client::race`]），建连结果与失败分类
//!   类型化交给上层
//! - **不自己重连**：连接断开后以类型化原因（[`DisconnectReason`]）通知上层，由上层新建客户端重连
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
use crate::client::link::{
    ConnectAckInfo, ConnectFailure, ConnectReport, DisconnectReason, DisconnectReasonCell,
};
use crate::client::transports::client_core::LinkBinding;
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

    /// 连接结束时调用（恰好一次语义见 [`DisconnectReason`]）：对端关闭、读写出错、心跳判死、
    /// 被踢、服务端计划性关闭、我方主动断开都走这里。
    ///
    /// 建连成功不回调：[`FlareClientBuilder::build_with_race`] 返回即已收到 CONNECT_ACK，
    /// 结果见 [`FlareClient::connect_report`]。
    async fn on_disconnect(&self, reason: &DisconnectReason) -> Result<()> {
        let _ = reason;
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
///
/// 每个客户端只建一次连接；断开后由上层新建客户端重连。
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

    /// 设置单个协议一次握手的时限：传输建立（含 TLS）加 CONNECT → CONNECT_ACK，默认 8 s
    pub fn with_connect_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.base = self.base.with_connect_timeout(timeout);
        self
    }

    /// 设置交错竞速的间隔：上一个协议这么久还没完成握手就发起下一个，默认 300 ms
    pub fn with_race_stagger(mut self, stagger: std::time::Duration) -> Self {
        self.base = self.base.with_race_stagger(stagger);
        self
    }

    /// 设置这次建连的连接代数（CONNECT metadata `connection_epoch`），由上层每次建连给出
    pub fn with_connection_epoch(mut self, epoch: u64) -> Self {
        self.base = self.base.with_connection_epoch(epoch);
        self
    }

    /// 设置上层当前接入清单的 serial（CONNECT metadata `access_serial`），还没有清单时为 0
    pub fn with_access_serial(mut self, serial: u64) -> Self {
        self.base = self.base.with_access_serial(serial);
        self
    }

    /// 设置接入点的备用 IP：主机名 DNS 失败或结果可疑时才直连，TLS SNI、证书校验和钉扎仍按主机名
    pub fn with_ip_hints(mut self, ip_hints: Vec<std::net::IpAddr>) -> Self {
        self.base = self.base.with_ip_hints(ip_hints);
        self
    }

    /// 设置 DoH 备用解析：DNS 失败、结果可疑或 TLS 失败时后台查一次，答复留给下一次拨号
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_doh(mut self, doh: std::sync::Arc<crate::client::dial::DohCache>) -> Self {
        self.base = self.base.with_doh(doh);
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

    /// 交错竞速建连（单协议是只有一个协议的竞速），返回时已经收到 CONNECT_ACK。
    ///
    /// 协议优先序、交错间隔、单次握手时限分别来自 [`Self::with_protocol_race`]（没设置时只有
    /// [`Self::with_protocol`] 一个）、[`Self::with_race_stagger`]、[`Self::with_connect_timeout`]。
    /// 成功时 [`FlareClient::connect_report`] 给出胜出协议、握手耗时、CONNECT_ACK 信息与失败的协议；
    /// 失败时 [`ConnectFailure`] 给出每个协议的失败类别（服务端拒绝时立即结束）。
    ///
    /// 监听器与观察者在收到 CONNECT_ACK 时挂到胜出的连接上：落败连接的事件到不了它们，这帧
    /// CONNECT_ACK 与紧随其后的帧都能收到。
    pub async fn build_with_race(self) -> std::result::Result<FlareClient, ConnectFailure> {
        let Some(listener) = self.listener else {
            return Err(ConnectFailure {
                attempts: Vec::new(),
                skipped: Vec::new(),
                elapsed: std::time::Duration::ZERO,
            });
        };

        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;

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

        let reason_slot = Arc::new(std::sync::OnceLock::new());
        let observer = Arc::new(FlareObserver {
            pipeline: pipeline.clone(),
            listener: listener.clone(),
            reason_slot: Arc::clone(&reason_slot),
        });
        let mut observers: Vec<Arc<dyn ConnectionObserver>> =
            vec![observer as Arc<dyn ConnectionObserver>];
        observers.extend(self.observers);
        let binding = LinkBinding {
            observers,
            reason_slot: Some(reason_slot),
        };

        #[cfg(not(target_arch = "wasm32"))]
        let (client, report) = {
            let client = HybridClient::connect_bound(self.base.config.clone(), binding).await?;
            let report = client
                .connect_report()
                .cloned()
                .expect("a connected hybrid client carries its connect report");
            (client, report)
        };
        #[cfg(target_arch = "wasm32")]
        let (client, report) = connect_websocket_race(&self.base.config, binding).await?;

        let wrapper = ClientWrapper::new(client);
        let parser = wrapper.parser_snapshot().await;
        pipeline.update_parser(parser).await;

        Ok(FlareClient {
            wrapper,
            pipeline,
            listener,
            report: Arc::new(report),
        })
    }
}

/// 浏览器里只有 WebSocket：优先序里的其它协议直接记为本端不支持，其余与 native 的交错竞速相同。
#[cfg(target_arch = "wasm32")]
async fn connect_websocket_race(
    config: &crate::client::config::ClientConfig,
    binding: LinkBinding,
) -> std::result::Result<(WebSocketClient, ConnectReport), ConnectFailure> {
    use crate::client::link::AttemptFailure;
    use crate::client::transports::ClientCore;
    use crate::client::transports::attempt::TransportAttempt;
    use crate::common::config_types::TransportProtocol;

    let base = ClientCore::new(config);
    let (attempt, report) =
        crate::client::race::staggered_race(&config.race_params(), |protocol| match protocol {
            TransportProtocol::WebSocket => {
                let protocol_config = crate::client::config::ClientConfig {
                    server_url: config.get_protocol_url(&protocol),
                    transport: protocol,
                    transports: None,
                    ..config.clone()
                };
                let core = ClientCore::for_attempt(&protocol_config, &base);
                Ok(TransportAttempt::new(
                    WebSocketClient::with_core(protocol_config, core.clone()),
                    core,
                    binding.clone(),
                ))
            }
            other => Err(AttemptFailure::Unsupported {
                detail: format!("{other:?} is not available in the browser"),
            }),
        })
        .await?;
    let (client, _core) = attempt.into_parts();
    Ok((client, report))
}

/// Flare 客户端：一条已协商完成的连接。
#[derive(Clone)]
pub struct FlareClient {
    wrapper: ClientWrapper,
    #[allow(dead_code)] // 保留用于未来扩展（如动态更新管道配置）
    pipeline: Arc<MessagePipeline>,
    #[allow(dead_code)] // 保留用于未来扩展（如动态更新监听器）
    listener: Arc<dyn MessageListener>,
    /// 建连结果
    report: Arc<ConnectReport>,
}

impl FlareClient {
    /// 建连结果：胜出协议、握手耗时、CONNECT_ACK 信息、胜出前失败与被取消的协议。
    pub fn connect_report(&self) -> &ConnectReport {
        &self.report
    }

    /// CONNECT_ACK 带回的接入信息（服务端时间、清单 serial、心跳建议、网关与接入点）。
    pub fn connect_ack_info(&self) -> &ConnectAckInfo {
        &self.report.ack
    }

    /// 这条连接结束的原因（还没结束时为 `None`）。与 [`MessageListener::on_disconnect`] 收到的相同。
    pub async fn last_disconnect_reason(&self) -> Option<DisconnectReason> {
        self.wrapper
            .with_core(|core| core.last_disconnect_reason())
            .await
    }

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

    /// 最近一次心跳往返时长：上层据此更新入口 RTT 与连接质量。还没有被应答过的 ping 时为 `None`。
    pub async fn heartbeat_round_trip(&self) -> Option<std::time::Duration> {
        self.wrapper.heartbeat_round_trip().await
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
/// 它只在胜出的连接收到 CONNECT_ACK 时挂上（见 [`LinkBinding`]），这里收到的事件都来自那条连接。
struct FlareObserver {
    pipeline: Arc<MessagePipeline>,
    listener: Arc<dyn MessageListener>,
    /// 挂上时绑定到那条连接的断开原因槽：断开、错误事件到来时从这里读类型化原因
    reason_slot: Arc<std::sync::OnceLock<DisconnectReasonCell>>,
}

impl FlareObserver {
    /// 这次断开、错误事件的类型化原因：连接在通知观察者之前把它记进槽里；万一读不到（还没绑定），
    /// 按事件文本归类。
    fn disconnect_reason(&self, event: &ConnectionEvent) -> DisconnectReason {
        let recorded = self
            .reason_slot
            .get()
            .and_then(|cell| cell.lock().ok().and_then(|reason| reason.clone()));
        recorded.unwrap_or_else(|| match event {
            ConnectionEvent::Error(error) => DisconnectReason::TransportError {
                detail: error.to_string(),
            },
            ConnectionEvent::Disconnected(detail) => DisconnectReason::from_detail(detail),
            _ => DisconnectReason::PeerClosed {
                detail: String::new(),
            },
        })
    }

    fn report_disconnect(&self, reason: DisconnectReason) {
        let listener = self.listener.clone();
        crate::client::runtime::spawn_client_task(async move {
            if let Err(e) = listener.on_disconnect(&reason).await {
                error!("[FlareClient] on_disconnect 失败: {}", e);
            }
        });
    }
}

impl ConnectionObserver for FlareObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                // 建连成功由 build_with_race 的返回表达，这里不再回调
                debug!("[FlareClient] 已连接（CONNECT_ACK 已收到）");
            }

            ConnectionEvent::Disconnected(_) | ConnectionEvent::Error(_) => {
                // 错误也是终止事件：连接已不可用，之后不会再补一个断开。原因在通知之前已经记在连接上，
                // 同步读出来，免得被之后的事件覆盖。
                let reason = self.disconnect_reason(event);
                match event {
                    ConnectionEvent::Error(err) => {
                        warn!("[FlareClient] 连接错误: {:?} ({reason})", err)
                    }
                    _ => info!("[FlareClient] 连接断开: {reason}"),
                }
                self.report_disconnect(reason);
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
        let observer = FlareObserver {
            pipeline,
            listener,
            reason_slot: Arc::new(std::sync::OnceLock::new()),
        };

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

    /// 记下 `on_disconnect` 收到的断开原因。
    #[derive(Default)]
    struct ErrorRecorder {
        reasons: std::sync::Mutex<Vec<DisconnectReason>>,
        seen: tokio::sync::Notify,
    }

    #[async_trait]
    impl MessageListener for ErrorRecorder {
        async fn on_message(&self, _frame: &Frame) -> Result<Option<Frame>> {
            Ok(None)
        }

        async fn on_disconnect(&self, reason: &DisconnectReason) -> Result<()> {
            self.reasons.lock().unwrap().push(reason.clone());
            self.seen.notify_one();
            Ok(())
        }
    }

    fn observer_with(listener: Arc<ErrorRecorder>) -> FlareObserver {
        FlareObserver {
            pipeline: Arc::new(MessagePipeline::new(MessageParser::json())),
            listener,
            reason_slot: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// WebSocket 协议错误是当前连接的终止性错误，必须作为断开交给监听器：吞掉它，上层收不到断开，
    /// 就再也不会重连。
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
        let reasons = listener.reasons.lock().unwrap().clone();
        assert_eq!(reasons.len(), 1, "{reasons:?}");
        assert!(
            matches!(&reasons[0], DisconnectReason::TransportError { detail } if detail.contains("WebSocket protocol error")),
            "{reasons:?}"
        );
    }

    /// 绑定到连接后，监听器收到的是连接记下的类型化原因（这里是带窗口的计划性关闭）。
    #[tokio::test]
    async fn the_listener_gets_the_reason_recorded_on_the_connection() {
        use crate::common::link::{CloseKind, ServerClose};
        let listener = Arc::new(ErrorRecorder::default());
        let observer = observer_with(listener.clone());
        let close = ServerClose::new(CloseKind::Rebalance).with_retry_window(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(3),
        );
        let cell: DisconnectReasonCell = Arc::new(std::sync::Mutex::new(Some(
            DisconnectReason::ServerClose(close.clone()),
        )));
        observer.reason_slot.set(cell).expect("bind once");
        observer.on_event(&ConnectionEvent::Disconnected(
            "REBALANCE: server closed the connection".to_string(),
        ));
        tokio::time::timeout(std::time::Duration::from_secs(1), listener.seen.notified())
            .await
            .expect("the close must reach the listener");
        assert_eq!(
            *listener.reasons.lock().unwrap(),
            vec![DisconnectReason::ServerClose(close)]
        );
    }
}
