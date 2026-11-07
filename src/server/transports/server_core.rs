//! 服务器核心功能
//!
//! 提供统一的连接管理和心跳检测功能，简化服务器实现

use crate::common::MessageParser;
use crate::common::compression::CompressionAlgorithm;
use crate::common::encryption::EncryptionAlgorithm;
use crate::common::error::Result;
use crate::common::message::pipeline::{
    ArcMessageMiddleware, ArcMessageProcessor, MessagePipeline,
};
use crate::common::protocol::{Frame, Reliability, SerializationFormat, frame_with_system_command};
use crate::server::auth::{Authenticator, TENANT_ID_METADATA_KEY};
use crate::server::config::ServerConfig;
use crate::server::connection::{
    ConnectionManager, ConnectionManagerTrait, device_handler, negotiation,
};
use crate::server::device::{DeviceManager, DeviceOwner};
use crate::server::events::factory::ServerMessageObserverFactory;
use crate::server::events::handler::ServerEventHandler;
use crate::server::handle::ServerHandle;
use crate::server::heartbeat::HeartbeatDetector;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// 服务器核心功能
///
/// 统一管理连接和心跳检测，简化服务器实现
pub struct ServerCore {
    /// 连接管理器
    pub connection_manager: Arc<ConnectionManager>,
    /// 消息解析器
    pub parser: MessageParser,
    /// 心跳检测器（可选，使用 Mutex 包装以支持内部可变性）
    heartbeat_detector: Arc<tokio::sync::Mutex<Option<HeartbeatDetector>>>,
    /// 设备管理器（可选，用于设备冲突管理）
    device_manager: Option<Arc<DeviceManager>>,
    /// 事件处理器（可选，用于细化的命令处理）
    event_handler: Option<Arc<dyn ServerEventHandler>>,
    /// 认证器（可选，如果提供则启用认证）
    authenticator: Option<Arc<dyn Authenticator>>,
    /// 是否启用认证（从配置读取）
    auth_enabled: bool,
    /// 认证超时时间（从配置读取）
    auth_timeout: Duration,
    /// 停机宽限（从配置读取）
    shutdown_close_grace: Duration,
    /// CONNECT_ACK 里报给客户端的固定接入信息（网关 ID、接入点 ID、心跳建议），从配置读取
    connect_ack_static: ConnectAckStatic,
    /// 默认序列化格式（用于协商）
    default_serialization_format: SerializationFormat,
    /// 默认压缩算法（用于协商）
    default_compression: CompressionAlgorithm,
    /// 默认加密算法（用于协商）
    default_encryption: EncryptionAlgorithm,
    /// 观察者工厂（用于创建连接观察者）
    observer_factory: Arc<dyn ServerMessageObserverFactory>,
    /// 共享的中间件列表（可选，用于消息处理管道）
    /// 如果配置了中间件，每个连接在协商完成时会创建自己的 pipeline
    shared_middlewares: Vec<ArcMessageMiddleware>,
    /// 共享的处理器列表（可选，用于消息处理管道）
    /// 如果配置了处理器，每个连接在协商完成时会创建自己的 pipeline
    shared_processors: Vec<ArcMessageProcessor>,
    /// 按协商 profile 复用共享 pipeline，减少大量长连接的重复分配。
    pipeline_cache: Arc<tokio::sync::Mutex<HashMap<PipelineCacheKey, Arc<MessagePipeline>>>>,
}

/// CONNECT_ACK 里不随连接变化的接入信息
#[derive(Debug, Clone, Default)]
struct ConnectAckStatic {
    gateway_id: Option<String>,
    ap_id: Option<String>,
    heartbeat_interval_hint_sec: Option<u32>,
}

impl ConnectAckStatic {
    fn from_config(config: &ServerConfig) -> Self {
        let heartbeat = &config.default_heartbeat;
        Self {
            gateway_id: config.gateway_id.clone(),
            ap_id: config.ap_id.clone(),
            heartbeat_interval_hint_sec: heartbeat
                .enabled
                .then(|| u32::try_from(heartbeat.interval.as_secs()).unwrap_or(u32::MAX))
                .filter(|secs| *secs > 0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PipelineCacheKey {
    format: SerializationFormat,
    compression: CompressionAlgorithm,
    encryption: EncryptionAlgorithm,
}

struct NegotiatedConnectionUpdate {
    format: SerializationFormat,
    compression: CompressionAlgorithm,
    encryption: EncryptionAlgorithm,
    user_id: Option<String>,
    metadata: Option<HashMap<String, String>>,
}

/// 认证后的连接身份
///
/// 启用认证时全部来自认证结果；未启用认证（或没有配置认证器）时沿用客户端在 CONNECT 里的自报值。
struct ConnectionIdentity {
    user_id: Option<String>,
    tenant_id: Option<String>,
    /// 写入连接元数据的认证元数据
    metadata: Option<HashMap<String, String>>,
    /// CONNECT_ACK 要报给这条连接的接入清单 serial
    access_serial: u64,
}

impl ConnectionIdentity {
    /// 未经认证器校验时的身份：客户端自报的用户与租户
    fn declared(negotiation: &negotiation::NegotiationResult) -> Self {
        Self {
            user_id: negotiation.user_id.clone(),
            tenant_id: negotiation.tenant_id.clone(),
            metadata: Some(HashMap::new()),
            access_serial: 0,
        }
    }

    /// 连接元数据：认证元数据加上租户，业务层读到的租户与设备分桶用的租户是同一个值
    fn connection_metadata(&self) -> Option<HashMap<String, String>> {
        let Some(tenant_id) = &self.tenant_id else {
            return self.metadata.clone();
        };
        let mut metadata = self.metadata.clone().unwrap_or_default();
        metadata.insert(TENANT_ID_METADATA_KEY.to_string(), tenant_id.clone());
        Some(metadata)
    }
}

impl ServerCore {
    /// 获取默认序列化格式
    pub fn default_serialization_format(&self) -> SerializationFormat {
        self.default_serialization_format
    }

    /// 获取默认压缩算法
    pub fn default_compression(&self) -> CompressionAlgorithm {
        self.default_compression.clone()
    }

    /// 设置默认序列化格式
    #[must_use]
    pub fn with_default_format(mut self, format: SerializationFormat) -> Self {
        self.default_serialization_format = format;
        self
    }

    /// 设置默认压缩算法
    #[must_use]
    pub fn with_default_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.default_compression = compression;
        self
    }

    /// 创建新的服务器核心
    pub fn new(config: &ServerConfig, connection_manager: Option<Arc<ConnectionManager>>) -> Self {
        Self::with_observer_factory(
            config,
            connection_manager,
            Arc::new(crate::server::events::DefaultServerMessageObserverFactory::new()),
        )
    }

    /// 使用指定的观察者工厂创建服务器核心
    ///
    /// # 参数
    /// - `config`: 服务器配置
    /// - `connection_manager`: 可选的连接管理器
    /// - `observer_factory`: 观察者工厂
    ///
    /// # 示例
    /// ```rust,ignore
    /// use flare_core::server::events::factory::ServerMessageObserverFactory;
    ///
    /// // 使用自定义工厂
    /// let factory = Arc::new(MyCustomObserverFactory::new());
    /// let core = ServerCore::with_observer_factory(&config, None, factory);
    /// ```
    pub fn with_observer_factory(
        config: &ServerConfig,
        connection_manager: Option<Arc<ConnectionManager>>,
        observer_factory: Arc<dyn ServerMessageObserverFactory>,
    ) -> Self {
        let connection_manager = connection_manager.unwrap_or_else(|| {
            Arc::new(ConnectionManager::with_limits(
                config.write_timeout,
                config.fanout_concurrency,
            ))
        });

        // 初始parser用于解析CONNECT消息，应该不使用加密（协商阶段）
        let parser = MessageParser::new(
            config.default_serialization_format,
            config.default_compression.clone(),
            EncryptionAlgorithm::None, // CONNECT消息不使用加密
        );

        Self {
            connection_manager,
            parser,
            heartbeat_detector: Arc::new(tokio::sync::Mutex::new(None)),
            device_manager: None,
            event_handler: None,
            authenticator: None,
            auth_enabled: config.auth_enabled,
            auth_timeout: config.auth_timeout,
            shutdown_close_grace: config.shutdown_close_grace,
            connect_ack_static: ConnectAckStatic::from_config(config),
            default_serialization_format: config.default_serialization_format,
            default_compression: config.default_compression.clone(),
            default_encryption: config.default_encryption.clone(),
            observer_factory,
            shared_middlewares: Vec::new(), // 默认不配置中间件，避免性能开销
            shared_processors: Vec::new(),  // 默认不配置处理器，避免性能开销
            pipeline_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// 设置设备管理器
    #[must_use]
    pub fn with_device_manager(mut self, device_manager: Option<Arc<DeviceManager>>) -> Self {
        self.device_manager = device_manager;
        self
    }

    /// 获取设备管理器
    pub fn device_manager(&self) -> Option<Arc<DeviceManager>> {
        self.device_manager.clone()
    }

    /// 设置事件处理器
    #[must_use]
    pub fn with_event_handler(
        mut self,
        event_handler: Option<Arc<dyn ServerEventHandler>>,
    ) -> Self {
        self.event_handler = event_handler;
        self
    }

    /// 设置观察者工厂
    ///
    /// # 参数
    /// - `factory`: 观察者工厂
    pub fn set_observer_factory(&mut self, factory: Arc<dyn ServerMessageObserverFactory>) {
        self.observer_factory = factory;
    }

    /// 获取观察者工厂
    pub fn observer_factory(&self) -> &Arc<dyn ServerMessageObserverFactory> {
        &self.observer_factory
    }

    /// 创建连接观察者
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `core_arc`: ServerCore 的 Arc 包装（由调用方提供，避免循环引用）
    ///
    /// # 返回
    /// 创建的观察者实例；若 `event_handler` 未设置则返回配置错误
    ///
    /// # Errors
    /// 如果 `event_handler` 未设置，返回 [`FlareError::configuration_error`]
    pub fn create_observer_with_core(
        &self,
        connection_id: String,
        core_arc: Arc<ServerCore>,
    ) -> Result<Arc<dyn crate::transport::events::ConnectionObserver>> {
        // 创建 ServerCore 的引用（避免循环引用）
        let core_ref = Arc::new(crate::server::events::factory::ServerCoreRef {
            device_manager: self.device_manager.clone(),
            event_handler: self.event_handler.clone(),
        });

        let event_handler = self.event_handler.clone().ok_or_else(|| {
            crate::common::error::FlareError::configuration_error(
                "ServerEventHandler is required for creating observer but was not configured",
            )
        })?;

        Ok(self.observer_factory.create_observer(
            Arc::clone(&self.connection_manager),
            self.parser.clone(),
            event_handler,
            connection_id,
            core_ref,
            core_arc,
        ))
    }

    /// 获取事件处理器
    pub fn event_handler(&self) -> Option<Arc<dyn ServerEventHandler>> {
        self.event_handler.clone()
    }

    /// 设置事件处理器（可变引用版本，用于已经创建的实例）
    pub fn set_event_handler(&mut self, event_handler: Option<Arc<dyn ServerEventHandler>>) {
        self.event_handler = event_handler;
    }

    /// 设置设备管理器（可变引用版本，用于已经创建的实例）
    pub fn set_device_manager(&mut self, device_manager: Option<Arc<DeviceManager>>) {
        self.device_manager = device_manager;
    }

    /// 设置认证器
    #[must_use]
    pub fn with_authenticator(mut self, authenticator: Option<Arc<dyn Authenticator>>) -> Self {
        self.authenticator = authenticator;
        self
    }

    /// 获取认证器
    pub fn authenticator(&self) -> Option<Arc<dyn Authenticator>> {
        self.authenticator.clone()
    }

    /// 设置认证器（可变引用版本，用于已经创建的实例）
    pub fn set_authenticator(&mut self, authenticator: Option<Arc<dyn Authenticator>>) {
        self.authenticator = authenticator;
    }

    /// 检查是否启用认证
    pub fn auth_enabled(&self) -> bool {
        self.auth_enabled && self.authenticator.is_some()
    }

    /// 获取认证超时时间
    pub fn auth_timeout(&self) -> Duration {
        self.auth_timeout
    }

    /// 停机时下发 CLOSE 后等客户端自行断开的宽限（`ServerConfig::shutdown_close_grace`）
    pub fn shutdown_close_grace(&self) -> Duration {
        self.shutdown_close_grace
    }

    /// 本次握手要放进 CONNECT_ACK 的接入信息：服务端当前时间、接入清单 serial（按连接所属应用，
    /// 由认证器给出）、心跳建议、网关与接入点 ID。
    fn connect_ack_info(&self, access_serial: u64) -> crate::common::link::ConnectAckInfo {
        crate::common::link::ConnectAckInfo {
            server_time_ms: Some(crate::common::platform::wall_clock_ms()),
            access_serial: Some(access_serial),
            heartbeat_interval_hint_sec: self.connect_ack_static.heartbeat_interval_hint_sec,
            gateway_id: self.connect_ack_static.gateway_id.clone(),
            ap_id: self.connect_ack_static.ap_id.clone(),
        }
    }

    /// 添加中间件（用于消息处理管道）
    ///
    /// 中间件会在消息处理前后执行，可以用于日志、监控、验证等。
    /// 中间件按添加顺序执行，优先级高的中间件会先执行。
    ///
    /// # 参数
    /// - `middleware`: 中间件实例
    pub async fn add_middleware(&mut self, middleware: ArcMessageMiddleware) {
        self.shared_middlewares.push(middleware);
        self.pipeline_cache.lock().await.clear();
    }

    /// 添加处理器（用于消息处理管道）
    ///
    /// 处理器用于处理具体的业务逻辑。
    /// 如果处理器返回响应，后续处理器不会执行。
    ///
    /// # 参数
    /// - `processor`: 处理器实例
    pub async fn add_processor(&mut self, processor: ArcMessageProcessor) {
        self.shared_processors.push(processor);
        self.pipeline_cache.lock().await.clear();
    }

    /// 启动心跳检测
    pub fn start_heartbeat(&self, config: &ServerConfig) {
        let manager_trait = Arc::clone(&self.connection_manager) as Arc<dyn ConnectionManagerTrait>;
        let timeout = config.connection_timeout;
        let check_interval =
            Duration::from_secs(timeout.as_secs() / 3).max(Duration::from_secs(10));

        let mut detector = HeartbeatDetector::new(manager_trait, timeout, check_interval);
        detector.start();

        // 使用 Mutex 设置 heartbeat_detector
        let detector_arc = Arc::clone(&self.heartbeat_detector);
        tokio::spawn(async move {
            let mut guard = detector_arc.lock().await;
            *guard = Some(detector);
        });
    }

    /// 停止心跳检测
    pub fn stop_heartbeat(&self) {
        let detector_arc = Arc::clone(&self.heartbeat_detector);
        tokio::spawn(async move {
            let mut guard = detector_arc.lock().await;
            if let Some(ref mut detector) = *guard {
                detector.stop();
            }
        });
    }

    /// 获取连接管理器 trait
    pub fn connection_manager_trait(&self) -> Arc<dyn ConnectionManagerTrait> {
        Arc::clone(&self.connection_manager) as Arc<dyn ConnectionManagerTrait>
    }

    /// 向指定连接发送消息
    ///
    /// 使用连接协商后的序列化格式和压缩算法
    pub async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait
            .send_frame_to(connection_id, frame, None)
            .await
    }

    /// 同帧多连接扇出（群下行主路径）：按协商编码分组、每组序列化一次。
    /// 返回（成功连接数, 失败连接数）。
    pub async fn send_to_connections(
        &self,
        connection_ids: &[String],
        frame: &Frame,
    ) -> (i32, i32) {
        let manager_trait = self.connection_manager_trait();
        manager_trait
            .send_frame_to_connections(connection_ids, frame)
            .await
    }

    /// 向（租户，用户）的所有连接发送消息
    ///
    /// 只投递到该租户下的连接；每个连接使用其协商后的序列化格式和压缩算法
    pub async fn send_to_user(&self, owner: &DeviceOwner, frame: &Frame) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.send_frame_to_user(owner, frame, None).await
    }

    /// 广播消息到所有连接
    ///
    /// 每个连接使用其协商后的序列化格式和压缩算法
    pub async fn broadcast(&self, frame: &Frame) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.broadcast_frame(frame, None).await
    }

    /// 广播消息到所有连接，排除指定连接
    ///
    /// 每个连接使用其协商后的序列化格式和压缩算法
    pub async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait
            .broadcast_frame_except(frame, exclude_connection_id, None)
            .await
    }

    /// 获取连接数量
    pub fn connection_count(&self) -> usize {
        self.connection_manager.connection_count()
    }

    /// 获取用户数量
    pub fn user_count(&self) -> usize {
        self.connection_manager.stats().total_users
    }

    /// 断开指定连接
    pub async fn disconnect(&self, connection_id: &str) -> Result<()> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.remove_connection(connection_id).await
    }

    /// 获取所有连接 ID（异步）
    pub async fn list_connections(&self) -> Vec<String> {
        let manager_trait = self.connection_manager_trait();
        manager_trait.list_connections().await
    }

    /// 处理 CONNECT 消息（协商）
    ///
    /// # 参数
    /// - `frame`: CONNECT 消息的 Frame
    /// - `connection_id`: 连接 ID
    ///
    /// # 返回
    /// 协商结果，包含：
    /// - `ack_frame`: 需要发送的 CONNECT_ACK Frame
    /// - `parser`: 基于协商结果创建的 MessageParser
    pub async fn handle_connect_message(
        &self,
        frame: &Frame,
        connection_id: &str,
    ) -> Result<(Frame, MessageParser)> {
        // 1. 解析协商信息
        let negotiation = negotiation::parse_connect_message(frame)?;

        // 协议版本不被支持：在认证之前拒绝，客户端据此进入终态（需要升级），不必再走一遍认证。
        // 没带版本的是这个字段出现之前的客户端，按当前版本处理。
        if let Some(version) = negotiation.protocol_version.as_deref()
            && !crate::common::constants::SUPPORTED_PROTOCOL_VERSIONS.contains(&version)
        {
            warn!(
                "[ServerCore] 协议版本不被支持，拒绝 CONNECT: connection_id={}, protocol_version={}, supported={:?}",
                connection_id,
                version,
                crate::common::constants::SUPPORTED_PROTOCOL_VERSIONS
            );
            return Err(crate::common::error::FlareError::localized(
                crate::common::error::ErrorCode::ProtocolVersionMismatch,
                crate::common::error::ErrorCode::ProtocolVersionMismatch.as_str(),
            ));
        }

        // 2. 确定最终使用的序列化格式、压缩算法和加密方式
        let (final_format, final_compression, final_encryption) =
            self.determine_negotiation_result(&negotiation);

        self.log_negotiation_details(
            connection_id,
            &negotiation,
            final_format,
            final_compression.clone(),
            final_encryption.clone(),
        );

        // 3. Token 验证（如果启用认证）- 先完成认证
        let identity = self
            .authenticate_connection(frame, connection_id, &negotiation)
            .await?;

        // 用户只取连接身份：启用认证时来自认证结果（认证通过却没给用户的已在上一步拒绝），
        // 未启用认证时就是客户端在 CONNECT 里的自报值。不再拿自报值补认证结果的缺。
        let user_id = identity.user_id.clone();
        // 设备按（租户，用户）分桶；租户只取认证身份，启用认证时不采信客户端自报的租户
        let device_owner = user_id
            .as_deref()
            .map(|user_id| DeviceOwner::new(identity.tenant_id.as_deref(), user_id));

        // 4. 更新连接信息（在认证后）
        self.update_connection_info(
            connection_id,
            &negotiation,
            NegotiatedConnectionUpdate {
                format: final_format,
                compression: final_compression.clone(),
                encryption: final_encryption.clone(),
                user_id: user_id.clone(),
                metadata: identity.connection_metadata(),
            },
        )
        .await;

        // 5. 标记连接为已验证
        self.mark_connection_authenticated(connection_id, &user_id)
            .await;

        // 6. 延迟处理设备冲突（在认证完成后，避免同时踢掉两个连接）
        // 使用小延迟确保第一个连接先完成认证，再处理第二个连接的冲突
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        let conflict_connections = self
            .handle_device_conflict(connection_id, &negotiation, device_owner.as_ref())
            .await;

        // 7. 创建 CONNECT_ACK
        let ack_frame = self.create_connect_ack(
            final_format,
            final_compression.clone(),
            final_encryption.clone(),
            &conflict_connections,
            identity.access_serial,
        );

        // 8. 创建基于协商结果的 MessageParser
        // 使用配置的默认加密算法（协商时不改变加密算法，保持配置值）
        let parser = MessageParser::new(final_format, final_compression, final_encryption);

        Ok((ack_frame, parser))
    }

    /// 完整处理 CONNECT 消息（协商、发送 ACK、调用 handler）
    ///
    /// 这是一个统一的处理方法，将协商、发送 ACK 和调用 handler 的逻辑集中在一起
    ///
    /// # 参数
    /// - `frame`: CONNECT 消息的 Frame
    /// - `connection_id`: 连接 ID
    /// - `connection`: 连接实例（用于发送 CONNECT_ACK）
    ///
    /// # 返回
    /// 处理成功返回 `Ok(())`，失败返回错误
    ///
    /// # Errors
    /// 如果 `event_handler` 未设置，返回 [`FlareError::configuration_error`]
    pub async fn handle_connect_complete(
        &self,
        frame: &Frame,
        connection_id: &str,
        connection: Arc<tokio::sync::Mutex<Box<dyn crate::transport::connection::Connection>>>,
    ) -> Result<()> {
        // 1. 处理协商（但不立即标记协商完成，等 CONNECT_ACK 发送完成后再标记）
        let (ack_frame, negotiation_parser) =
            match self.handle_connect_message(frame, connection_id).await {
                Ok(negotiated) => negotiated,
                Err(e) => {
                    if let Some(
                        code @ (crate::common::error::ErrorCode::AuthenticationFailed
                        | crate::common::error::ErrorCode::ServiceUnavailable
                        | crate::common::error::ErrorCode::ProtocolVersionMismatch),
                    ) = e.code()
                    {
                        Self::send_connect_rejection(&connection, connection_id, code, e.reason())
                            .await;
                    }
                    return Err(e);
                }
            };

        // 记录最终协商结果
        let final_format = negotiation_parser.default_format();
        let final_compression = negotiation_parser.default_compression();
        let final_encryption = negotiation_parser.default_encryption();
        debug!(
            "[ServerCore] 协商完成: connection_id={}, 最终序列化方式={:?}, 最终压缩方式={:?},最终加密方式={:?}",
            connection_id, final_format, final_compression, final_encryption
        );

        // 2. 序列化 CONNECT_ACK 并发送
        // CONNECT_ACK 使用 PRE_NEGOTIATION_PARSER（JSON、不压缩、不加密）
        // 这样客户端在收到 CONNECT_ACK 时可以使用相同的 parser 解析
        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        let ack_data = PRE_NEGOTIATION_PARSER.serialize(&ack_frame)?;
        {
            let mut conn = connection.lock().await;
            conn.send(&ack_data).await?;
        }
        debug!(
            "[ServerCore] CONNECT_ACK 已发送: connection_id={}",
            connection_id
        );

        // 3. CONNECT_ACK 发送完成后，才标记协商完成并更新连接信息
        // 这样可以确保客户端在收到 CONNECT_ACK 之前发送的消息不会被错误地按加密方式解析
        self.finalize_negotiation_after_ack(
            connection_id,
            final_format,
            final_compression,
            final_encryption,
        )
        .await;

        // 4. 通知连接建立（在协商完成后）
        // 使用 ServerEventHandler.on_connect
        let event_handler = self.event_handler.as_ref().ok_or_else(|| {
            crate::common::error::FlareError::configuration_error(
                "ServerEventHandler is required to complete connection but was not configured",
            )
        })?;
        if let Err(e) = event_handler.on_connect(connection_id).await {
            // 租户不可用这类拒绝是客户端侧的常态（停用后缓存翻转前的重连），不算服务端故障。
            if matches!(
                e.code(),
                Some(crate::common::error::ErrorCode::PermissionDenied)
            ) {
                warn!(
                    "[ServerCore] ServerEventHandler.on_connect 拒绝: connection_id={}, error={}",
                    connection_id, e
                );
            } else {
                error!(
                    "[ServerCore] ServerEventHandler.on_connect 失败: connection_id={}, error={}",
                    connection_id, e
                );
            }
            Self::send_post_ack_rejection(&connection, connection_id, &negotiation_parser, &e)
                .await;
            return Err(e);
        }

        Ok(())
    }

    /// CONNECT_ACK 已经发出、`on_connect` 却失败时（典型：认证时租户缓存还是 active，业务层登记
    /// 会话时租户已被停用），关连接前先下发一帧 KICKED 说明原因。没有这一帧，客户端刚看到连接成功
    /// 就被无原因断开，分不清是网络故障还是被拒，只能立即重连。
    ///
    /// 编码用本次协商出的 parser：客户端收到 CONNECT_ACK 后已切换到协商格式，KICKED 紧随其后。
    /// 原因是类型化错误码（如 `TENANT_UNAVAILABLE`）时原样下发，否则只下发错误码本身（如
    /// `SERVICE_UNAVAILABLE`），不把服务端内部错误文本带给客户端。与 CONNECT 前的拒绝一样直接写连接、
    /// 不走写队列，并且有超时。
    async fn send_post_ack_rejection(
        connection: &Arc<tokio::sync::Mutex<Box<dyn crate::transport::connection::Connection>>>,
        connection_id: &str,
        parser: &MessageParser,
        error: &crate::common::error::FlareError,
    ) {
        let code = error
            .code()
            .unwrap_or(crate::common::error::ErrorCode::ConnectionFailed)
            .as_str();
        let reason = if is_typed_reason(error.reason()) {
            error.reason()
        } else {
            code
        };
        let metadata = HashMap::from([
            ("reason".to_string(), reason.as_bytes().to_vec()),
            ("code".to_string(), code.as_bytes().to_vec()),
        ]);
        let frame = frame_with_system_command(
            crate::common::protocol::builder::kicked(reason.to_string(), Some(metadata)),
            Reliability::BestEffort,
        );
        let data = match parser.serialize(&frame) {
            Ok(data) => data,
            Err(e) => {
                warn!(
                    "[ServerCore] CONNECT_ACK 后的拒绝帧序列化失败: connection_id={}, error={}",
                    connection_id, e
                );
                return;
            }
        };
        let sent = tokio::time::timeout(CONNECT_REJECTION_SEND_TIMEOUT, async {
            connection.lock().await.send(&data).await
        })
        .await;
        if !matches!(sent, Ok(Ok(()))) {
            debug!(
                "[ServerCore] CONNECT_ACK 后的拒绝帧未送达: connection_id={}",
                connection_id
            );
        }
    }

    /// 拒绝 CONNECT 前先告诉客户端原因：一个 ERROR 帧，编码与 CONNECT_ACK 相同（协商前 JSON）。
    /// 调用方随后关闭连接。没有这一帧，客户端只能看到连接被关掉，分不清是令牌失效、租户被停用，
    /// 还是服务端暂时放不了行。
    ///
    /// metadata `code` 是拒绝的类别：`AUTHENTICATION_FAILED`（认证器给了结论），或
    /// `SERVICE_UNAVAILABLE`（[`AuthResult::retryable_failure`](crate::server::auth::AuthResult::retryable_failure)，
    /// 客户端应退避重连）。`message` 只在认证器给的是类型化错误码（如 `TENANT_UNAVAILABLE`）时原样
    /// 下发，其余一律用 `code`，免得把服务端内部错误文本带给客户端。直接写连接而不走写队列：
    /// 移除连接会立刻关闭写队列，排在队列里的帧会被丢掉。
    async fn send_connect_rejection(
        connection: &Arc<tokio::sync::Mutex<Box<dyn crate::transport::connection::Connection>>>,
        connection_id: &str,
        code: crate::common::error::ErrorCode,
        reason: &str,
    ) {
        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        let code = code.as_str();
        let message = if is_typed_reason(reason) {
            reason
        } else {
            code
        };
        let metadata = HashMap::from([("code".to_string(), code.as_bytes().to_vec())]);
        let frame = frame_with_system_command(
            crate::common::protocol::builder::error(message.to_string(), Some(metadata)),
            Reliability::BestEffort,
        );
        let data = match PRE_NEGOTIATION_PARSER.serialize(&frame) {
            Ok(data) => data,
            Err(e) => {
                warn!(
                    "[ServerCore] CONNECT 拒绝帧序列化失败: connection_id={}, error={}",
                    connection_id, e
                );
                return;
            }
        };
        let sent = tokio::time::timeout(CONNECT_REJECTION_SEND_TIMEOUT, async {
            connection.lock().await.send(&data).await
        })
        .await;
        if !matches!(sent, Ok(Ok(()))) {
            debug!(
                "[ServerCore] CONNECT 拒绝帧未送达: connection_id={}",
                connection_id
            );
        }
    }

    /// 在 CONNECT_ACK 发送完成后，最终完成协商（标记协商完成并更新连接信息）
    ///
    /// 这样可以确保客户端在收到 CONNECT_ACK 之前发送的消息不会被错误地按加密方式解析
    async fn finalize_negotiation_after_ack(
        &self,
        connection_id: &str,
        final_format: SerializationFormat,
        final_compression: CompressionAlgorithm,
        final_encryption: EncryptionAlgorithm,
    ) {
        let manager = Arc::clone(&self.connection_manager);

        // 获取连接信息（应该已经存在，因为 handle_connect_message 已经创建了）
        let info = match manager.get_connection(connection_id) {
            Some((_, conn_info)) => conn_info,
            None => {
                error!(
                    "[ServerCore] 连接不存在，无法完成协商: connection_id={}",
                    connection_id
                );
                return;
            }
        };

        // 创建 parser（用于协商和 pipeline）
        let compression_clone = final_compression.clone();
        let encryption_clone = final_encryption.clone();
        let parser = crate::common::MessageParser::new(
            final_format,
            compression_clone.clone(),
            encryption_clone.clone(),
        );

        let pipeline = self
            .pipeline_for_negotiation_profile(
                &parser,
                PipelineCacheKey {
                    format: final_format,
                    compression: final_compression.clone(),
                    encryption: final_encryption.clone(),
                },
            )
            .await;

        // 更新连接信息，标记协商完成并设置 parser/pipeline
        if let Err(e) = manager.update_connection_negotiation_with_pipeline(
            connection_id,
            info.device_info.clone(),
            final_format,
            final_compression,
            final_encryption,
            info.user_id.clone(),
            parser,
            pipeline,
        ) {
            error!("[ServerCore] 最终完成协商失败: {}", e);
            return;
        }

        debug!(
            "[ServerCore] 协商最终完成（CONNECT_ACK 已发送）: connection_id={}",
            connection_id
        );
    }

    // ============================================================================
    // 内部辅助方法
    // ============================================================================

    async fn pipeline_for_negotiation_profile(
        &self,
        parser: &MessageParser,
        key: PipelineCacheKey,
    ) -> Option<Arc<MessagePipeline>> {
        if self.shared_middlewares.is_empty() && self.shared_processors.is_empty() {
            return None;
        }

        {
            let cache = self.pipeline_cache.lock().await;
            if let Some(pipeline) = cache.get(&key) {
                return Some(Arc::clone(pipeline));
            }
        }

        let pipeline = MessagePipeline::new(parser.clone());
        for middleware in &self.shared_middlewares {
            pipeline.add_middleware(Arc::clone(middleware)).await;
        }
        for processor in &self.shared_processors {
            pipeline.add_processor(Arc::clone(processor)).await;
        }
        let pipeline = Arc::new(pipeline);

        let mut cache = self.pipeline_cache.lock().await;
        let cached = cache.entry(key).or_insert_with(|| Arc::clone(&pipeline));
        Some(Arc::clone(cached))
    }

    /// 确定协商结果（内部辅助方法）
    fn determine_negotiation_result(
        &self,
        negotiation: &negotiation::NegotiationResult,
    ) -> (
        SerializationFormat,
        CompressionAlgorithm,
        EncryptionAlgorithm,
    ) {
        // 协商规则：
        // - 如果客户端强制指定（force_format=true），使用客户端格式
        // - 如果客户端指定了格式但未强制，优先使用客户端格式（如果服务端支持）
        // - 如果客户端未指定格式，使用服务端默认格式（JSON）
        let final_format = if negotiation.is_forced || negotiation.serialization_format_specified {
            negotiation.serialization_format
        } else {
            self.default_serialization_format
        };

        // 压缩只用客户端解得开的：客户端指定了（或强制）就用它的；没指定时，服务端默认压缩须在
        // 客户端声明能解的压缩（`accept_compression`）之内，否则不压缩。以前没指定就用服务端默认，
        // 不带对应解压器的客户端（例如编译时没开 gzip）解不开此后的帧，包括停机 CLOSE。
        let final_compression =
            if negotiation.is_forced || negotiation.compression != CompressionAlgorithm::None {
                negotiation.compression.clone()
            } else if self.default_compression != CompressionAlgorithm::None
                && negotiation
                    .accepted_compressions
                    .contains(&self.default_compression)
            {
                self.default_compression.clone()
            } else {
                CompressionAlgorithm::None
            };

        // 加密方式
        let final_encryption = if negotiation.encryption != EncryptionAlgorithm::None {
            negotiation.encryption.clone()
        } else {
            self.default_encryption.clone()
        };

        (final_format, final_compression, final_encryption)
    }

    /// 记录协商详情（内部辅助方法）
    fn log_negotiation_details(
        &self,
        connection_id: &str,
        negotiation: &negotiation::NegotiationResult,
        final_format: SerializationFormat,
        final_compression: CompressionAlgorithm,
        final_encryption: EncryptionAlgorithm,
    ) {
        debug!(
            "[ServerCore] 收到 CONNECT 消息: connection_id={}",
            connection_id
        );
        debug!(
            "[ServerCore] 协商详情: 客户端请求={:?}, 客户端是否指定格式={}, 客户端压缩={:?}, 强制模式={}, 服务端默认={:?}, 服务端默认压缩={:?}, 最终格式={:?}, 最终压缩={:?},最终加密={:?} device={:?}, user_id={:?}",
            negotiation.serialization_format,
            negotiation.serialization_format_specified,
            negotiation.compression,
            negotiation.is_forced,
            self.default_serialization_format,
            self.default_compression,
            final_format,
            final_compression,
            final_encryption,
            negotiation.device_info.as_ref().map(|d| &d.platform),
            negotiation.user_id
        );
    }

    /// 处理设备冲突（内部辅助方法）
    ///
    /// `owner` 取自认证后的连接身份（与连接绑定的用户、写进连接元数据的租户一致），冲突只在同一归属内判定。
    /// 认证给出了用户就不再看客户端在 CONNECT 里自报的 user_id，否则持有效令牌的客户端报上别人的 user_id
    /// 就能踢掉别人的设备。
    async fn handle_device_conflict(
        &self,
        connection_id: &str,
        negotiation: &negotiation::NegotiationResult,
        owner: Option<&DeviceOwner>,
    ) -> Vec<String> {
        let mut conflict_connections = Vec::new();

        debug!(
            "[ServerCore] 设备冲突检测条件: device_manager={}, device_info={}, user_id={}",
            self.device_manager.is_some(),
            negotiation.device_info.is_some(),
            owner.is_some()
        );

        if let (Some(device_mgr), Some(device_info)) =
            (&self.device_manager, &negotiation.device_info)
        {
            if let Some(owner) = owner {
                let user_id = owner.user_id();
                info!(
                    "[ServerCore] 开始设备冲突检测: tenant_id={}, user_id={}, connection_id={}, platform={:?}",
                    owner.tenant_id(),
                    user_id,
                    connection_id,
                    device_info.platform
                );

                let manager_trait = self.connection_manager_trait();
                let platform = device_info.platform.clone();

                match device_handler::handle_device_conflict(
                    Some(Arc::clone(device_mgr)),
                    owner,
                    connection_id,
                    &platform,
                    device_info,
                    manager_trait,
                )
                .await
                {
                    Ok(conflict_result) => {
                        conflict_connections = conflict_result.conflict_connections;

                        // 防御性检查：确保冲突连接列表不包含新连接本身
                        conflict_connections.retain(|conn_id| {
                            if conn_id == connection_id {
                                error!(
                                    "[ServerCore] 错误：冲突连接列表包含新连接ID，已过滤: connection_id={}",
                                    connection_id
                                );
                                false
                            } else {
                                true
                            }
                        });

                        if !conflict_connections.is_empty() {
                            info!(
                                "[ServerCore] 检测到设备冲突: user_id={}, 新连接={}, 将踢掉 {} 个旧连接: {:?}",
                                user_id,
                                connection_id,
                                conflict_connections.len(),
                                conflict_connections
                            );
                        } else {
                            debug!(
                                "[ServerCore] 无设备冲突: user_id={}, platform={:?}, 新连接={}",
                                user_id, platform, connection_id
                            );
                        }
                    }
                    Err(e) => {
                        error!("[ServerCore] 设备冲突处理失败: {}", e);
                    }
                }
            } else {
                debug!("[ServerCore] 跳过设备冲突检测: user_id 为空");
            }
        } else {
            debug!(
                "[ServerCore] 跳过设备冲突检测: device_manager={}, device_info={}",
                self.device_manager.is_some(),
                negotiation.device_info.is_some()
            );
        }

        conflict_connections
    }

    /// 认证连接（内部辅助方法）
    async fn authenticate_connection(
        &self,
        frame: &Frame,
        connection_id: &str,
        negotiation: &negotiation::NegotiationResult,
    ) -> Result<ConnectionIdentity> {
        let auth_enabled = self.auth_enabled();

        if !auth_enabled {
            debug!("[ServerCore] 跳过 token 验证: 认证未启用");
            return Ok(ConnectionIdentity::declared(negotiation));
        }

        let Some(authenticator) = &self.authenticator else {
            return Ok(ConnectionIdentity::declared(negotiation));
        };

        // 从 CONNECT 消息的 metadata 中提取 token
        let token = Self::extract_token_from_frame(frame);

        let Some(token) = token else {
            error!("[ServerCore] 未提供 token: connection_id={}", connection_id);
            return Err(crate::common::error::FlareError::authentication_failed(
                "未提供 token".to_string(),
            ));
        };

        debug!(
            "[ServerCore] 开始验证 token: connection_id={}",
            connection_id
        );

        let metadata = Self::extract_system_command_metadata(frame);

        match authenticator
            .authenticate(
                &token,
                connection_id,
                negotiation.device_info.as_ref(),
                metadata,
            )
            .await
        {
            Ok(auth_result) => {
                if auth_result.authenticated
                    && auth_result
                        .user_id
                        .as_deref()
                        .is_none_or(|user_id| user_id.trim().is_empty())
                {
                    // 认证器说通过却没给出用户：这是认证器的实现问题。不能拿客户端在 CONNECT 里自报的
                    // user_id 顶上——那等于不认证，持任意有效令牌就能冒用别人的身份。
                    error!(
                        "[ServerCore] 认证器返回已通过但没有 user_id，拒绝连接: connection_id={}",
                        connection_id
                    );
                    Err(crate::common::error::FlareError::authentication_failed(
                        "认证结果缺少用户".to_string(),
                    ))
                } else if auth_result.authenticated {
                    debug!(
                        "[ServerCore] Token 验证成功: connection_id={}, user_id={:?}, tenant_id={:?}",
                        connection_id,
                        auth_result.user_id,
                        auth_result.tenant_id()
                    );
                    Ok(ConnectionIdentity {
                        tenant_id: auth_result.tenant_id().map(str::to_owned),
                        access_serial: auth_result.access_serial,
                        user_id: auth_result.user_id,
                        metadata: auth_result.user_metadata,
                    })
                } else if auth_result.retryable {
                    // 认证依赖暂时不可用（如租户状态读不到）：不是对令牌或租户的结论。按服务不可用拒绝，
                    // 客户端退避重连；报成鉴权失败会让客户端去换令牌，报成租户不可用会让它终止会话。
                    let error_msg = auth_result
                        .error_message
                        .unwrap_or_else(|| "认证暂不可用".to_string());
                    warn!(
                        "[ServerCore] 认证暂不可用，按可重试拒绝: connection_id={}, error={}",
                        connection_id, error_msg
                    );
                    Err(crate::common::error::FlareError::localized(
                        crate::common::error::ErrorCode::ServiceUnavailable,
                        error_msg,
                    ))
                } else {
                    let error_msg = auth_result
                        .error_message
                        .unwrap_or_else(|| "Token 验证失败".to_string());
                    // warn 而不是 error：客户端 token 过期是**常态**，
                    // 端上会带着旧 token 重连、拿到 401 后再刷新，一次过期
                    // 就是十几条日志。用 ERROR 记它会把真正的服务端故障淹掉
                    // （实测一个开着的客户端 10 分钟刷了 30 条 ERROR）。
                    // 鉴权失败本身仍然照常拒绝，只是不再冒充服务端错误。
                    warn!(
                        "[ServerCore] Token 验证失败: connection_id={}, error={}",
                        connection_id, error_msg
                    );
                    Err(crate::common::error::FlareError::authentication_failed(
                        error_msg,
                    ))
                }
            }
            Err(e) => {
                // 认证器出错（按 trait 约定是系统错误，如依赖读不到），不是对令牌的结论。与
                // 暂时性拒绝一样按服务不可用拒绝，客户端退避重连；报成鉴权失败会让客户端去换一个
                // 本来有效的令牌。错误文本只进服务端日志，下发给客户端的是错误码本身。
                error!(
                    "[ServerCore] 认证过程出错，按可重试拒绝: connection_id={}, error={}",
                    connection_id, e
                );
                Err(crate::common::error::FlareError::localized(
                    crate::common::error::ErrorCode::ServiceUnavailable,
                    format!("认证过程出错: {}", e),
                ))
            }
        }
    }

    /// 从 Frame 中提取 token（内部辅助方法）
    fn extract_token_from_frame(frame: &Frame) -> Option<String> {
        frame.command.as_ref().and_then(|cmd| {
            if let Some(crate::common::protocol::flare::core::commands::command::Type::System(
                sys_cmd,
            )) = &cmd.r#type
            {
                sys_cmd
                    .metadata
                    .get("token")
                    .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
            } else {
                None
            }
        })
    }

    /// 从 Frame 中提取系统命令的 metadata（内部辅助方法）
    fn extract_system_command_metadata(
        frame: &Frame,
    ) -> Option<&std::collections::HashMap<String, Vec<u8>>> {
        frame.command.as_ref().and_then(|cmd| {
            if let Some(crate::common::protocol::flare::core::commands::command::Type::System(
                sys_cmd,
            )) = &cmd.r#type
            {
                Some(&sys_cmd.metadata)
            } else {
                None
            }
        })
    }

    /// 更新连接信息（内部辅助方法）
    ///
    /// 注意：此方法不标记协商完成，只更新基本信息
    /// 协商完成将在 CONNECT_ACK 发送完成后由 finalize_negotiation_after_ack 标记
    async fn update_connection_info(
        &self,
        connection_id: &str,
        negotiation: &negotiation::NegotiationResult,
        update: NegotiatedConnectionUpdate,
    ) {
        let manager = Arc::clone(&self.connection_manager);

        // update.user_id 就是连接身份里的用户（见 handle_connect_message），这里不再回落到自报值
        let auth_user_id = update.user_id.clone();
        let user_id = auth_user_id.clone();

        // 只更新基本信息，不标记协商完成，也不创建 parser/pipeline
        // parser/pipeline 将在 CONNECT_ACK 发送完成后创建
        if let Err(e) = manager.update_connection_negotiation(
            connection_id,
            negotiation.device_info.clone(),
            update.format,
            update.compression,
            update.encryption,
            user_id.clone(),
            update.metadata,
        ) {
            error!("[ServerCore] 更新连接协商信息失败: {}", e);
            return;
        }

        // 验证更新是否成功
        if let Some(user_id) = &user_id {
            debug!(
                "[ServerCore] 已更新连接协商信息: connection_id={}, user_id={}",
                connection_id, user_id
            );

            if let Some((_, conn_info)) = manager.get_connection(connection_id) {
                match conn_info.user_id {
                    Some(ref updated_user_id) => {
                        debug!(
                            "[ServerCore] 验证成功: 连接信息中的 user_id={}",
                            updated_user_id
                        );
                    }
                    None => {
                        error!("[ServerCore] 验证失败: 连接信息中的 user_id 仍为 None");
                    }
                }
            }
        } else {
            warn!(
                "[ServerCore] 连接信息中没有 user_id: connection_id={}, negotiation.user_id={:?}, auth_user_id={:?}",
                connection_id, negotiation.user_id, auth_user_id
            );
        }
    }

    /// 标记连接为已验证（内部辅助方法）
    async fn mark_connection_authenticated(
        &self,
        connection_id: &str,
        auth_user_id: &Option<String>,
    ) {
        let manager = Arc::clone(&self.connection_manager);
        let manager_trait = manager as Arc<dyn ConnectionManagerTrait>;
        let auth_enabled = self.auth_enabled();

        if let Err(e) = manager_trait
            .set_connection_authenticated(connection_id, auth_user_id.clone())
            .await
        {
            error!("[ServerCore] 标记连接为已验证失败: {}", e);
            return;
        }

        if auth_enabled {
            debug!(
                "[ServerCore] 连接已标记为已验证（认证通过）: connection_id={}, user_id={:?}",
                connection_id, auth_user_id
            );
        } else {
            debug!(
                "[ServerCore] 连接已标记为已验证（无需认证）: connection_id={}, user_id={:?}",
                connection_id, auth_user_id
            );
        }
    }

    /// 创建 CONNECT_ACK（内部辅助方法）
    fn create_connect_ack(
        &self,
        final_format: SerializationFormat,
        final_compression: CompressionAlgorithm,
        final_encryption: EncryptionAlgorithm,
        conflict_connections: &[String],
        access_serial: u64,
    ) -> Frame {
        let mut ack_metadata = std::collections::HashMap::new();

        self.connect_ack_info(access_serial)
            .write_metadata(&mut ack_metadata);

        // 如果有冲突连接，通知客户端
        if !conflict_connections.is_empty() {
            let conflicts_json =
                serde_json::to_string(conflict_connections).unwrap_or_else(|_| "[]".to_string());
            ack_metadata.insert(
                "conflict_connections".to_string(),
                conflicts_json.into_bytes(),
            );
        }

        // 创建 CONNECT_ACK，包含完整的协商结果：格式、压缩、加密
        let connect_ack_cmd = negotiation::create_connect_ack(
            final_format,
            final_compression,
            final_encryption, // 使用配置的默认加密算法
            Some(ack_metadata),
        );

        frame_with_system_command(connect_ack_cmd, Reliability::AtLeastOnce)
    }
}

/// 让 ServerCore 实现 ServerHandle trait
/// 这样可以在任何需要发送消息的地方注入 ServerCore，而不需要整个 Server
#[async_trait]
impl ServerHandle for ServerCore {
    async fn send_to(&self, connection_id: &str, frame: &Frame) -> Result<()> {
        self.send_to(connection_id, frame).await
    }

    async fn send_to_user(&self, owner: &DeviceOwner, frame: &Frame) -> Result<()> {
        self.send_to_user(owner, frame).await
    }

    async fn broadcast(&self, frame: &Frame) -> Result<()> {
        self.broadcast(frame).await
    }

    async fn broadcast_except(&self, frame: &Frame, exclude_connection_id: &str) -> Result<()> {
        self.broadcast_except(frame, exclude_connection_id).await
    }

    async fn disconnect(&self, connection_id: &str) -> Result<()> {
        self.disconnect(connection_id).await
    }

    fn connection_count(&self) -> usize {
        self.connection_count()
    }

    fn user_count(&self) -> usize {
        self.user_count()
    }
}

/// 拒绝帧最多等这么久；对端不读也不能拖住关连接。
const CONNECT_REJECTION_SEND_TIMEOUT: Duration = Duration::from_secs(1);

/// 认证器给出的失败原因是否为可下发的类型化错误码：大写字母开头，只含大写字母、数字、下划线。
fn is_typed_reason(reason: &str) -> bool {
    let mut chars = reason.chars();
    chars.next().is_some_and(|c| c.is_ascii_uppercase())
        && reason.len() <= 64
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::ValidationMiddleware;
    use crate::common::platform::{MonotonicInstant, monotonic_now};
    use crate::transport::connection::Connection;
    use crate::transport::events::ArcObserver;

    struct TestConnection {
        last_active: MonotonicInstant,
    }

    #[async_trait]
    impl Connection for TestConnection {
        fn add_observer(&mut self, _observer: ArcObserver) {}

        fn remove_observer(&mut self, _observer: ArcObserver) {}

        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            self.last_active = monotonic_now();
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn last_active_time(&self) -> MonotonicInstant {
            self.last_active
        }

        fn update_active_time(&mut self) {
            self.last_active = monotonic_now();
        }
    }

    #[test]
    fn negotiation_uses_server_default_when_client_format_unspecified() {
        let config = ServerConfig::default().with_format(SerializationFormat::Protobuf);
        let core = ServerCore::new(&config, None);
        let negotiation = negotiation::NegotiationResult::default();

        let (format, compression, encryption) = core.determine_negotiation_result(&negotiation);

        assert_eq!(format, SerializationFormat::Protobuf);
        assert_eq!(compression, config.default_compression);
        assert_eq!(encryption, config.default_encryption);
    }

    /// 客户端没指定压缩时，服务端默认压缩只在客户端声明能解时才用；客户端指定的压缩照用。
    /// 以前没指定就用服务端默认（网关是 gzip），不带 gzip 解压器的客户端解不开此后的帧。
    #[test]
    fn server_default_compression_needs_the_clients_declared_support() {
        let config = ServerConfig::default().with_compression(CompressionAlgorithm::Gzip);
        let core = ServerCore::new(&config, None);

        let undeclared = negotiation::NegotiationResult::default();
        let (_, compression, _) = core.determine_negotiation_result(&undeclared);
        assert_eq!(compression, CompressionAlgorithm::None);

        let accepts_gzip = negotiation::NegotiationResult {
            accepted_compressions: vec![CompressionAlgorithm::Gzip],
            ..Default::default()
        };
        let (_, compression, _) = core.determine_negotiation_result(&accepts_gzip);
        assert_eq!(compression, CompressionAlgorithm::Gzip);

        let accepts_other = negotiation::NegotiationResult {
            accepted_compressions: vec![CompressionAlgorithm::Zstd],
            ..Default::default()
        };
        let (_, compression, _) = core.determine_negotiation_result(&accepts_other);
        assert_eq!(compression, CompressionAlgorithm::None);

        let no_server_default = ServerCore::new(&ServerConfig::default(), None);
        let (_, compression, _) = no_server_default.determine_negotiation_result(&accepts_gzip);
        assert_eq!(compression, CompressionAlgorithm::None);

        let requests_gzip = negotiation::NegotiationResult {
            compression: CompressionAlgorithm::Gzip,
            ..Default::default()
        };
        let (_, compression, _) = no_server_default.determine_negotiation_result(&requests_gzip);
        assert_eq!(compression, CompressionAlgorithm::Gzip);
    }

    #[test]
    fn negotiation_honors_explicit_client_format_metadata() {
        let config = ServerConfig::default().with_format(SerializationFormat::Protobuf);
        let core = ServerCore::new(&config, None);
        let negotiation = negotiation::NegotiationResult {
            serialization_format: SerializationFormat::Json,
            serialization_format_specified: true,
            ..Default::default()
        };

        let (format, _, _) = core.determine_negotiation_result(&negotiation);

        assert_eq!(format, SerializationFormat::Json);
    }

    #[tokio::test]
    async fn shared_pipeline_is_reused_for_same_negotiation_profile() {
        let config = ServerConfig::default().with_format(SerializationFormat::Protobuf);
        let mut core = ServerCore::new(&config, None);
        core.add_middleware(Arc::new(ValidationMiddleware::new("noop", |_| Ok(()))))
            .await;

        for connection_id in ["conn1", "conn2"] {
            core.connection_manager
                .add_connection(
                    connection_id.to_string(),
                    Box::new(TestConnection {
                        last_active: monotonic_now(),
                    }),
                    Some("user1".to_string()),
                    false,
                )
                .unwrap();

            core.finalize_negotiation_after_ack(
                connection_id,
                SerializationFormat::Protobuf,
                CompressionAlgorithm::None,
                EncryptionAlgorithm::None,
            )
            .await;
        }

        let first_pipeline = core
            .connection_manager
            .get_connection("conn1")
            .and_then(|(_, info)| info.cached_pipeline)
            .expect("first connection should have cached pipeline");
        let second_pipeline = core
            .connection_manager
            .get_connection("conn2")
            .and_then(|(_, info)| info.cached_pipeline)
            .expect("second connection should have cached pipeline");

        assert!(
            Arc::ptr_eq(&first_pipeline, &second_pipeline),
            "connections with the same negotiated parser profile should share one pipeline"
        );
    }

    /// 记下写到连接上的每一帧。
    struct RecordingConnection {
        sent: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        last_active: MonotonicInstant,
    }

    #[async_trait]
    impl Connection for RecordingConnection {
        fn add_observer(&mut self, _observer: ArcObserver) {}

        fn remove_observer(&mut self, _observer: ArcObserver) {}

        async fn send(&mut self, data: &[u8]) -> Result<()> {
            self.sent.lock().unwrap().push(data.to_vec());
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn last_active_time(&self) -> MonotonicInstant {
            self.last_active
        }

        fn update_active_time(&mut self) {
            self.last_active = monotonic_now();
        }
    }

    /// 认证器总是给出同一个拒绝结果。
    struct Reject(crate::server::auth::AuthResult);

    #[async_trait]
    impl Authenticator for Reject {
        async fn authenticate(
            &self,
            _token: &str,
            _connection_id: &str,
            _device_info: Option<&crate::common::device::DeviceInfo>,
            _metadata: Option<&HashMap<String, Vec<u8>>>,
        ) -> Result<crate::server::auth::AuthResult> {
            Ok(self.0.clone())
        }
    }

    /// 被拒的 CONNECT 在关连接前收到一个 ERROR 帧（协商前 JSON）：类型化原因原样下发，
    /// 其余文本一律换成拒绝类别的错误码。认证器给了结论的是 AUTHENTICATION_FAILED；
    /// 暂时性拒绝（认证依赖读不到）是 SERVICE_UNAVAILABLE，客户端据此退避重连，
    /// 而不是换令牌或终止会话。
    #[tokio::test]
    async fn rejected_connect_tells_the_client_why_before_the_close() {
        use crate::common::error::ErrorCode;
        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        use crate::common::protocol::flare::core::commands::{command, system_command};
        use crate::server::auth::AuthResult;
        for (auth_result, expected_code, expected_message) in [
            (
                AuthResult::failure("TENANT_UNAVAILABLE".to_string()),
                ErrorCode::AuthenticationFailed,
                "TENANT_UNAVAILABLE",
            ),
            (
                AuthResult::failure("Token 无效或已过期".to_string()),
                ErrorCode::AuthenticationFailed,
                "AUTHENTICATION_FAILED",
            ),
            (
                AuthResult::retryable_failure("tenant runtime projection unavailable".to_string()),
                ErrorCode::ServiceUnavailable,
                "SERVICE_UNAVAILABLE",
            ),
            (
                AuthResult::retryable_failure("TENANT_RUNTIME_UNREADABLE".to_string()),
                ErrorCode::ServiceUnavailable,
                "TENANT_RUNTIME_UNREADABLE",
            ),
        ] {
            let config = ServerConfig::default().enable_auth();
            let core = ServerCore::new(&config, None)
                .with_authenticator(Some(Arc::new(Reject(auth_result))));
            let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
            let connection: Arc<tokio::sync::Mutex<Box<dyn Connection>>> =
                Arc::new(tokio::sync::Mutex::new(Box::new(RecordingConnection {
                    sent: sent.clone(),
                    last_active: monotonic_now(),
                })));
            let connect = frame_with_system_command(
                crate::common::protocol::builder::connect(
                    SerializationFormat::Json,
                    HashMap::from([("token".to_string(), b"t".to_vec())]),
                ),
                Reliability::AtLeastOnce,
            );

            let error = core
                .handle_connect_complete(&connect, "c1", connection)
                .await
                .expect_err("CONNECT must be rejected");

            assert_eq!(error.code(), Some(expected_code), "{error}");
            let sent = sent.lock().unwrap();
            assert_eq!(sent.len(), 1, "exactly the rejection frame, no CONNECT_ACK");
            let frame = PRE_NEGOTIATION_PARSER.parse(&sent[0]).expect("JSON frame");
            let Some(command::Type::System(system)) = frame.command.and_then(|c| c.r#type) else {
                panic!("system command expected");
            };
            assert_eq!(system.r#type, system_command::Type::Error as i32);
            assert_eq!(system.message, expected_message);
            assert_eq!(
                system.metadata.get("code").map(Vec::as_slice),
                Some(expected_code.as_str().as_bytes())
            );
        }
    }

    /// 记下被调用的次数，总是放行。
    #[derive(Default)]
    struct CountingAuthenticator(std::sync::atomic::AtomicUsize);

    #[async_trait]
    impl Authenticator for CountingAuthenticator {
        async fn authenticate(
            &self,
            _token: &str,
            _connection_id: &str,
            _device_info: Option<&crate::common::device::DeviceInfo>,
            _metadata: Option<&HashMap<String, Vec<u8>>>,
        ) -> Result<crate::server::auth::AuthResult> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::server::auth::AuthResult::failure(
                "Token 无效或已过期".to_string(),
            ))
        }
    }

    /// 协议版本不被支持：在认证之前就以 PROTOCOL_VERSION_MISMATCH（3001）拒绝，ERROR 帧里的原因与
    /// 类别都是它；支持的版本照常走认证。
    #[tokio::test]
    async fn an_unsupported_protocol_version_is_rejected_before_authentication() {
        use crate::common::constants::metadata_keys;
        use crate::common::error::ErrorCode;
        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        use crate::common::protocol::flare::core::commands::{command, system_command};

        let authenticator = Arc::new(CountingAuthenticator::default());
        let core = ServerCore::new(&ServerConfig::default().enable_auth(), None)
            .with_authenticator(Some(authenticator.clone()));
        let connect = |version: &str| {
            frame_with_system_command(
                crate::common::protocol::builder::connect(
                    SerializationFormat::Json,
                    HashMap::from([
                        ("token".to_string(), b"t".to_vec()),
                        (
                            metadata_keys::PROTOCOL_VERSION.to_string(),
                            version.as_bytes().to_vec(),
                        ),
                    ]),
                ),
                Reliability::AtLeastOnce,
            )
        };
        let recording = || {
            let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
            let connection: Arc<tokio::sync::Mutex<Box<dyn Connection>>> =
                Arc::new(tokio::sync::Mutex::new(Box::new(RecordingConnection {
                    sent: sent.clone(),
                    last_active: monotonic_now(),
                })));
            (sent, connection)
        };

        let (sent, connection) = recording();
        let error = core
            .handle_connect_complete(&connect("9.9.9"), "c1", connection)
            .await
            .expect_err("an unsupported version is rejected");
        assert_eq!(error.code(), Some(ErrorCode::ProtocolVersionMismatch));
        assert_eq!(
            authenticator.0.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the version is checked before authentication"
        );
        let frame = {
            let sent = sent.lock().unwrap();
            assert_eq!(sent.len(), 1);
            PRE_NEGOTIATION_PARSER.parse(&sent[0]).expect("JSON frame")
        };
        let Some(command::Type::System(system)) = frame.command.and_then(|c| c.r#type) else {
            panic!("system command expected");
        };
        assert_eq!(system.r#type, system_command::Type::Error as i32);
        assert_eq!(system.message, "PROTOCOL_VERSION_MISMATCH");
        assert_eq!(
            system.metadata.get("code").map(Vec::as_slice),
            Some(b"PROTOCOL_VERSION_MISMATCH".as_slice())
        );

        let (_, connection) = recording();
        let error = core
            .handle_connect_complete(
                &connect(crate::common::constants::PROTOCOL_VERSION),
                "c2",
                connection,
            )
            .await
            .expect_err("the token is rejected");
        assert_eq!(error.code(), Some(ErrorCode::AuthenticationFailed));
        assert_eq!(authenticator.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// CONNECT_ACK 带上服务端时间、清单 serial（没有认证器时是 0）、心跳建议、网关与接入点 ID；
    /// 没配置的不带。
    #[tokio::test]
    async fn the_connect_ack_carries_the_access_info() {
        use crate::common::link::ConnectAckInfo;
        use crate::common::protocol::flare::core::commands::command;

        let config = ServerConfig::default()
            .with_gateway_id("gw-bj-1")
            .with_ap_id("ap-bj-a");
        let core = ServerCore::new(&config, None);
        let connect = frame_with_system_command(
            crate::common::protocol::builder::connect(SerializationFormat::Json, HashMap::new()),
            Reliability::AtLeastOnce,
        );
        let ack_info = |frame: Frame| {
            let Some(command::Type::System(system)) = frame.command.and_then(|c| c.r#type) else {
                panic!("system command expected");
            };
            ConnectAckInfo::from_metadata(&system.metadata)
        };

        let before = crate::common::platform::wall_clock_ms();
        let (ack, _) = core
            .handle_connect_message(&connect, "c1")
            .await
            .expect("negotiated");
        let info = ack_info(ack);
        assert!(
            info.server_time_ms.is_some_and(|ms| ms >= before),
            "{info:?}"
        );
        assert_eq!(info.access_serial, Some(0));
        assert_eq!(
            info.heartbeat_interval_hint_sec,
            Some(config.default_heartbeat.interval.as_secs() as u32)
        );
        assert_eq!(info.gateway_id.as_deref(), Some("gw-bj-1"));
        assert_eq!(info.ap_id.as_deref(), Some("ap-bj-a"));

        let bare = ServerCore::new(&ServerConfig::default(), None);
        let (ack, _) = bare
            .handle_connect_message(&connect, "c2")
            .await
            .expect("negotiated");
        let info = ack_info(ack);
        assert_eq!(info.gateway_id, None);
        assert_eq!(info.ap_id, None);
    }

    /// 按租户给 serial 的认证器：两个应用共用一个网关，各自的连接只看到自己应用的 serial。
    struct AppSerials;

    #[async_trait]
    impl Authenticator for AppSerials {
        async fn authenticate(
            &self,
            token: &str,
            _connection_id: &str,
            _device_info: Option<&crate::common::device::DeviceInfo>,
            _metadata: Option<&HashMap<String, Vec<u8>>>,
        ) -> Result<crate::server::auth::AuthResult> {
            let serial = if token == "app-a" { 42 } else { 7 };
            Ok(
                crate::server::auth::AuthResult::success(Some(format!("user-{token}")))
                    .with_tenant_id(token)
                    .with_access_serial(serial),
            )
        }
    }

    #[tokio::test]
    async fn the_connect_ack_reports_the_serial_of_the_connection_app() {
        use crate::common::link::ConnectAckInfo;
        use crate::common::protocol::flare::core::commands::command;

        let mut config = ServerConfig::default();
        config.auth_enabled = true;
        let core = ServerCore::new(&config, None).with_authenticator(Some(Arc::new(AppSerials)));
        for (token, serial) in [("app-a", 42), ("app-b", 7)] {
            let connect = frame_with_system_command(
                crate::common::protocol::builder::connect(
                    SerializationFormat::Json,
                    HashMap::from([("token".to_string(), token.as_bytes().to_vec())]),
                ),
                Reliability::AtLeastOnce,
            );
            let (ack, _) = core
                .handle_connect_message(&connect, token)
                .await
                .expect("negotiated");
            let Some(command::Type::System(system)) = ack.command.and_then(|c| c.r#type) else {
                panic!("system command expected");
            };
            assert_eq!(
                ConnectAckInfo::from_metadata(&system.metadata).access_serial,
                Some(serial),
                "{token}"
            );
        }
    }

    /// 认证器本身出错（按 trait 约定是系统错误）。
    struct BrokenAuthenticator;

    #[async_trait]
    impl Authenticator for BrokenAuthenticator {
        async fn authenticate(
            &self,
            _token: &str,
            _connection_id: &str,
            _device_info: Option<&crate::common::device::DeviceInfo>,
            _metadata: Option<&HashMap<String, Vec<u8>>>,
        ) -> Result<crate::server::auth::AuthResult> {
            Err(crate::common::error::FlareError::System(
                "tenant db pool timed out".to_string(),
            ))
        }
    }

    /// 认证器出错不是对令牌的结论：按可重试的 SERVICE_UNAVAILABLE 拒绝（与暂时性拒绝一致），
    /// 客户端退避重连。报成 AUTHENTICATION_FAILED 会让客户端去换一个本来有效的令牌。错误文本
    /// 不下发给客户端。
    #[tokio::test]
    async fn authenticator_errors_are_retryable_rejections() {
        use crate::common::error::ErrorCode;
        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        use crate::common::protocol::flare::core::commands::{command, system_command};
        let core = ServerCore::new(&ServerConfig::default().enable_auth(), None)
            .with_authenticator(Some(Arc::new(BrokenAuthenticator)));
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connection: Arc<tokio::sync::Mutex<Box<dyn Connection>>> =
            Arc::new(tokio::sync::Mutex::new(Box::new(RecordingConnection {
                sent: sent.clone(),
                last_active: monotonic_now(),
            })));
        let connect = frame_with_system_command(
            crate::common::protocol::builder::connect(
                SerializationFormat::Json,
                HashMap::from([("token".to_string(), b"t".to_vec())]),
            ),
            Reliability::AtLeastOnce,
        );

        let error = core
            .handle_connect_complete(&connect, "c1", connection)
            .await
            .expect_err("CONNECT must be rejected");

        assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable), "{error}");
        assert!(ErrorCode::ServiceUnavailable.is_retryable());
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1, "exactly the rejection frame, no CONNECT_ACK");
        let frame = PRE_NEGOTIATION_PARSER.parse(&sent[0]).expect("JSON frame");
        let Some(command::Type::System(system)) = frame.command.and_then(|c| c.r#type) else {
            panic!("system command expected");
        };
        assert_eq!(system.r#type, system_command::Type::Error as i32);
        assert_eq!(system.message, "SERVICE_UNAVAILABLE");
        assert_eq!(
            system.metadata.get("code").map(Vec::as_slice),
            Some(b"SERVICE_UNAVAILABLE".as_slice())
        );
    }

    /// 认证器说通过却没给出用户（或给了空用户）时拒绝 CONNECT，不拿客户端自报的 user_id 顶上：
    /// 否则持任意有效令牌的客户端报上别人的 user_id，连接就绑到了别人名下。
    #[tokio::test]
    async fn authenticated_result_without_a_user_is_rejected() {
        use crate::common::error::ErrorCode;
        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        use crate::common::protocol::flare::core::commands::{command, system_command};
        use crate::server::auth::AuthResult;
        for auth_result in [
            AuthResult::success(None),
            AuthResult::success(Some("  ".to_string())),
            AuthResult::success_with_metadata(None, HashMap::new()).with_tenant_id("tenant-a"),
        ] {
            let devices = Arc::new(DeviceManager::new(
                crate::common::device::DeviceConflictStrategy::PlatformExclusive,
            ));
            let core = ServerCore::new(&ServerConfig::default().enable_auth(), None)
                .with_device_manager(Some(devices.clone()))
                .with_authenticator(Some(Arc::new(Reject(auth_result))));
            let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
            core.connection_manager
                .add_connection(
                    "c1".to_string(),
                    Box::new(RecordingConnection {
                        sent: sent.clone(),
                        last_active: monotonic_now(),
                    }),
                    None,
                    true,
                )
                .unwrap();
            let (connection, _) = core.connection_manager.get_connection("c1").unwrap();
            let connect = frame_with_system_command(
                crate::common::protocol::builder::connect(
                    SerializationFormat::Json,
                    HashMap::from([
                        ("token".to_string(), b"t".to_vec()),
                        ("user_id".to_string(), b"victim".to_vec()),
                        ("device_id".to_string(), b"d1".to_vec()),
                        ("platform".to_string(), b"web".to_vec()),
                    ]),
                ),
                Reliability::AtLeastOnce,
            );

            let error = core
                .handle_connect_complete(&connect, "c1", connection)
                .await
                .expect_err("CONNECT without an authenticated user must be rejected");

            assert_eq!(error.code(), Some(ErrorCode::AuthenticationFailed));
            let sent = sent.lock().unwrap().clone();
            assert_eq!(sent.len(), 1, "exactly the rejection frame, no CONNECT_ACK");
            let frame = PRE_NEGOTIATION_PARSER.parse(&sent[0]).expect("JSON frame");
            let Some(command::Type::System(system)) = frame.command.and_then(|c| c.r#type) else {
                panic!("system command expected");
            };
            assert_eq!(system.r#type, system_command::Type::Error as i32);
            let (_, info) = core.connection_manager.get_connection("c1").unwrap();
            assert_eq!(info.user_id, None, "the declared user must not be bound");
            assert!(
                core.connection_manager
                    .get_user_connections(&DeviceOwner::new(None, "victim"))
                    .is_empty()
            );
            assert!(
                devices
                    .get_user_connections(&DeviceOwner::new(None, "victim"))
                    .await
                    .is_empty()
            );
        }
    }

    #[test]
    fn only_typed_reasons_reach_the_client() {
        assert!(is_typed_reason("TENANT_UNAVAILABLE"));
        assert!(is_typed_reason("TOKEN_REVOKED2"));
        for reason in [
            "",
            "tenant",
            "Token 无效或已过期",
            "验证过程出错: db",
            "_X",
            "A B",
        ] {
            assert!(!is_typed_reason(reason), "{reason}");
        }
        assert!(!is_typed_reason(&"A".repeat(65)));
    }

    /// `on_connect` 总是按给定错误失败，模拟业务层登记会话时被拒（如租户刚停用）。
    struct FailingConnect(crate::common::error::FlareError);

    #[async_trait]
    impl ServerEventHandler for FailingConnect {
        async fn on_connect(&self, _connection_id: &str) -> Result<()> {
            Err(self.0.clone())
        }
    }

    /// CONNECT_ACK 之后 `on_connect` 才失败（认证时租户缓存还是 active，登记会话时租户已停用）：
    /// 客户端先收到 ACK，再收到按协商格式编码、带原因的 KICKED，然后才关连接。原因只在是类型化
    /// 错误码时原样下发，否则只给错误码本身，内部错误文本不下发。
    #[tokio::test]
    async fn connect_failing_after_the_ack_tells_the_client_why_before_the_close() {
        use crate::common::error::{ErrorCode, FlareError};
        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        use crate::common::protocol::flare::core::commands::{command, system_command};
        for (error, reason, code) in [
            (
                FlareError::localized(ErrorCode::PermissionDenied, "TENANT_UNAVAILABLE"),
                "TENANT_UNAVAILABLE",
                "PERMISSION_DENIED",
            ),
            (
                FlareError::localized(
                    ErrorCode::ServiceUnavailable,
                    "Signaling login failed: online unreachable",
                ),
                "SERVICE_UNAVAILABLE",
                "SERVICE_UNAVAILABLE",
            ),
            (
                FlareError::System("internal state".to_string()),
                "CONNECTION_FAILED",
                "CONNECTION_FAILED",
            ),
        ] {
            let core = ServerCore::new(&ServerConfig::default().enable_auth(), None)
                .with_authenticator(Some(Arc::new(TenantTokens)))
                .with_event_handler(Some(Arc::new(FailingConnect(error.clone()))));
            let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
            core.connection_manager
                .add_connection(
                    "c1".to_string(),
                    Box::new(RecordingConnection {
                        sent: sent.clone(),
                        last_active: monotonic_now(),
                    }),
                    None,
                    true,
                )
                .unwrap();
            let (connection, _) = core.connection_manager.get_connection("c1").unwrap();
            // 协商成 Protobuf：KICKED 必须按协商格式编码，而不是协商前的 JSON
            let connect = frame_with_system_command(
                crate::common::protocol::builder::connect(
                    SerializationFormat::Protobuf,
                    HashMap::from([
                        ("token".to_string(), b"u1@tenant-a".to_vec()),
                        ("device_id".to_string(), b"d1".to_vec()),
                        ("platform".to_string(), b"web".to_vec()),
                    ]),
                ),
                Reliability::AtLeastOnce,
            );
            let (format, compression, encryption) = core.determine_negotiation_result(
                &negotiation::parse_connect_message(&connect).unwrap(),
            );
            assert_eq!(format, SerializationFormat::Protobuf);

            let failed = core
                .handle_connect_complete(&connect, "c1", connection)
                .await
                .expect_err("on_connect failure must fail the CONNECT");

            assert_eq!(failed.code(), error.code());
            let sent = sent.lock().unwrap();
            assert_eq!(sent.len(), 2, "{reason}: CONNECT_ACK, then the rejection");
            let system_of = |frame: Frame| {
                let Some(command::Type::System(system)) = frame.command.and_then(|c| c.r#type)
                else {
                    panic!("system command expected");
                };
                system
            };
            // parser 解析时会自动识别格式，编码只能看字节：ACK 是协商前 JSON，KICKED 是协商出的
            // Protobuf（默认配置不压缩、不加密）
            assert!(
                sent[0].starts_with(b"{"),
                "CONNECT_ACK is pre-negotiation JSON"
            );
            assert!(
                !sent[1].starts_with(b"{"),
                "{reason}: KICKED must use the negotiated encoding, not pre-negotiation JSON"
            );
            let ack = system_of(PRE_NEGOTIATION_PARSER.parse(&sent[0]).expect("JSON ACK"));
            assert_eq!(ack.r#type, system_command::Type::ConnectAck as i32);
            let kicked = system_of(
                MessageParser::new(format, compression, encryption)
                    .parse(&sent[1])
                    .expect("KICKED in the negotiated format"),
            );
            assert_eq!(kicked.r#type, system_command::Type::Kicked as i32);
            assert_eq!(kicked.message, reason);
            assert_eq!(
                kicked.metadata.get("reason").map(Vec::as_slice),
                Some(reason.as_bytes())
            );
            assert_eq!(
                kicked.metadata.get("code").map(Vec::as_slice),
                Some(code.as_bytes())
            );
        }
    }

    /// 令牌形如 `user@tenant`，认证结果只看令牌，客户端自报的 user_id / tenant_id 一概不看。
    struct TenantTokens;

    #[async_trait]
    impl Authenticator for TenantTokens {
        async fn authenticate(
            &self,
            token: &str,
            _connection_id: &str,
            _device_info: Option<&crate::common::device::DeviceInfo>,
            _metadata: Option<&HashMap<String, Vec<u8>>>,
        ) -> Result<crate::server::auth::AuthResult> {
            let (user_id, tenant_id) = token.split_once('@').expect("user@tenant token");
            Ok(
                crate::server::auth::AuthResult::success(Some(user_id.to_string()))
                    .with_tenant_id(tenant_id),
            )
        }
    }

    fn device_core(config: &ServerConfig) -> (ServerCore, Arc<DeviceManager>) {
        let devices = Arc::new(DeviceManager::new(
            crate::common::device::DeviceConflictStrategy::PlatformExclusive,
        ));
        let core = ServerCore::new(config, None)
            .with_device_manager(Some(devices.clone()))
            .with_authenticator(Some(Arc::new(TenantTokens)));
        (core, devices)
    }

    /// 走一遍 CONNECT 协商，返回 CONNECT_ACK 里带回的被踢连接。
    async fn connect(
        core: &ServerCore,
        connection_id: &str,
        metadata: &[(&str, &str)],
    ) -> Vec<String> {
        use crate::common::protocol::flare::core::commands::command;
        core.connection_manager
            .add_connection(
                connection_id.to_string(),
                Box::new(TestConnection {
                    last_active: monotonic_now(),
                }),
                None,
                true,
            )
            .unwrap();
        let frame = frame_with_system_command(
            crate::common::protocol::builder::connect(
                SerializationFormat::Json,
                metadata
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.as_bytes().to_vec()))
                    .collect(),
            ),
            Reliability::AtLeastOnce,
        );
        let (ack, _) = core
            .handle_connect_message(&frame, connection_id)
            .await
            .expect("CONNECT accepted");
        let Some(command::Type::System(system)) = ack.command.and_then(|c| c.r#type) else {
            panic!("CONNECT_ACK expected");
        };
        system
            .metadata
            .get("conflict_connections")
            .map(|raw| serde_json::from_slice(raw).expect("conflict list"))
            .unwrap_or_default()
    }

    fn connection_tenant(core: &ServerCore, connection_id: &str) -> Option<String> {
        core.connection_manager
            .get_connection(connection_id)
            .and_then(|(_, info)| info.metadata.get(TENANT_ID_METADATA_KEY).cloned())
    }

    /// SEC-04/SEC-07（W3-01 §3.3）：两个租户里 user_id 相同、平台相同的设备互不顶掉；
    /// 同一租户内仍按平台互斥策略冲突。
    #[tokio::test]
    async fn connect_scopes_device_conflicts_to_the_authenticated_tenant() {
        let (core, devices) = device_core(&ServerConfig::default().enable_auth());
        let android = |token: &'static str, device: &'static str| {
            [
                ("token", token),
                ("device_id", device),
                ("platform", "android"),
            ]
        };

        assert!(
            connect(&core, "b-1", &android("shared@tenant-b", "b1"))
                .await
                .is_empty()
        );
        assert_eq!(
            connect(&core, "a-1", &android("shared@tenant-a", "a1")).await,
            Vec::<String>::new(),
            "其他租户的同平台设备不能被顶掉"
        );
        assert_eq!(
            connect(&core, "a-2", &android("shared@tenant-a", "a2")).await,
            vec!["a-1".to_string()],
            "同租户同平台仍然冲突"
        );

        assert!(core.connection_manager.get_connection("b-1").is_some());
        assert!(core.connection_manager.get_connection("a-1").is_none());
        assert_eq!(
            devices
                .get_user_connections(&DeviceOwner::new(Some("tenant-b"), "shared"))
                .await,
            vec!["b-1".to_string()]
        );
        assert_eq!(
            devices
                .get_user_connections(&DeviceOwner::new(Some("tenant-a"), "shared"))
                .await,
            vec!["a-2".to_string()]
        );
        assert_eq!(connection_tenant(&core, "a-2").as_deref(), Some("tenant-a"));
        assert_eq!(connection_tenant(&core, "b-1").as_deref(), Some("tenant-b"));
    }

    /// 启用认证时设备分桶只认令牌里的身份：客户端在 CONNECT 里报别人的 user_id / tenant_id
    /// 不能把自己塞进别人的桶，也就踢不掉别人的设备。
    #[tokio::test]
    async fn connect_ignores_client_declared_identity_when_auth_is_enabled() {
        let (core, devices) = device_core(&ServerConfig::default().enable_auth());
        let attacker = DeviceOwner::new(Some("tenant-a"), "attacker");

        // 受害者在两个租户里各有一台 iOS
        for (connection_id, token) in [
            ("victim-a", "victim@tenant-a"),
            ("victim-b", "victim@tenant-b"),
        ] {
            connect(
                &core,
                connection_id,
                &[("token", token), ("device_id", "v1"), ("platform", "ios")],
            )
            .await;
        }
        // 攻击者持自己的令牌，却在 CONNECT 里自报受害者的 user_id 和租户
        let kicked = connect(
            &core,
            "attacker-1",
            &[
                ("token", "attacker@tenant-a"),
                ("device_id", "x1"),
                ("platform", "ios"),
                ("user_id", "victim"),
                (TENANT_ID_METADATA_KEY, "tenant-b"),
            ],
        )
        .await;

        assert!(kicked.is_empty(), "{kicked:?}");
        for (connection_id, tenant) in [("victim-a", "tenant-a"), ("victim-b", "tenant-b")] {
            assert!(
                core.connection_manager
                    .get_connection(connection_id)
                    .is_some(),
                "{connection_id} must stay connected"
            );
            assert_eq!(
                devices
                    .get_user_connections(&DeviceOwner::new(Some(tenant), "victim"))
                    .await,
                vec![connection_id.to_string()]
            );
        }
        assert_eq!(
            devices.get_user_connections(&attacker).await,
            vec!["attacker-1".to_string()]
        );
        assert_eq!(
            connection_tenant(&core, "attacker-1").as_deref(),
            Some("tenant-a")
        );
    }

    /// 按用户投递只落在认证租户内：两个租户里 user_id 相同的用户各自登录，发给其中一个租户的
    /// 帧不会写到另一个租户的连接上。
    #[tokio::test]
    async fn send_to_user_is_scoped_to_the_authenticated_tenant() {
        let (core, _) = device_core(&ServerConfig::default().enable_auth());
        let mut sent = HashMap::new();
        for (connection_id, token) in [("a-1", "shared@tenant-a"), ("b-1", "shared@tenant-b")] {
            let frames = Arc::new(std::sync::Mutex::new(Vec::new()));
            core.connection_manager
                .add_connection(
                    connection_id.to_string(),
                    Box::new(RecordingConnection {
                        sent: frames.clone(),
                        last_active: monotonic_now(),
                    }),
                    None,
                    true,
                )
                .unwrap();
            let connect = frame_with_system_command(
                crate::common::protocol::builder::connect(
                    SerializationFormat::Json,
                    HashMap::from([
                        ("token".to_string(), token.as_bytes().to_vec()),
                        ("device_id".to_string(), connection_id.as_bytes().to_vec()),
                        ("platform".to_string(), b"web".to_vec()),
                    ]),
                ),
                Reliability::AtLeastOnce,
            );
            core.handle_connect_message(&connect, connection_id)
                .await
                .expect("CONNECT accepted");
            sent.insert(connection_id, frames);
        }

        let ping = frame_with_system_command(
            crate::common::protocol::builder::ping(),
            Reliability::BestEffort,
        );
        core.send_to_user(&DeviceOwner::new(Some("tenant-a"), "shared"), &ping)
            .await
            .expect("send to the tenant-a user");

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while sent["a-1"].lock().unwrap().is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "tenant-a never got the frame"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // 写队列异步落盘：给另一租户的连接留出同样的机会，确认它确实什么都没收到
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(sent["a-1"].lock().unwrap().len(), 1);
        assert!(
            sent["b-1"].lock().unwrap().is_empty(),
            "tenant-b's same-named user must not receive tenant-a's frame"
        );
    }

    /// 未启用认证时沿用客户端自报的租户；没报租户的连接落到默认租户，只和默认租户共桶。
    #[tokio::test]
    async fn connect_without_auth_uses_the_declared_tenant() {
        let (core, devices) = device_core(&ServerConfig::default());
        let web = |tenant: Option<&'static str>, device: &'static str| {
            let mut metadata = vec![("user_id", "u"), ("device_id", device), ("platform", "web")];
            if let Some(tenant) = tenant {
                metadata.push((TENANT_ID_METADATA_KEY, tenant));
            }
            metadata
        };

        assert!(
            connect(&core, "x-1", &web(Some("x"), "x1"))
                .await
                .is_empty()
        );
        assert!(
            connect(&core, "y-1", &web(Some("y"), "y1"))
                .await
                .is_empty()
        );
        assert!(connect(&core, "d-1", &web(None, "d1")).await.is_empty());
        assert_eq!(
            connect(&core, "d-2", &web(None, "d2")).await,
            vec!["d-1".to_string()]
        );

        assert_eq!(connection_tenant(&core, "x-1").as_deref(), Some("x"));
        assert_eq!(connection_tenant(&core, "d-2"), None);
        assert_eq!(
            devices
                .get_user_connections(&DeviceOwner::new(None, "u"))
                .await,
            vec!["d-2".to_string()]
        );
    }
}
