use crate::common::Frame;
use crate::server::{ConnectionHandler, ConnectionManagerTrait, ServerEventHandler};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::info;

/// 服务端事件包装器
///
/// 将 `DefaultServerMessageObserver` 的功能直接集成到 `ConnectionHandler` 中，
/// 提供统一的消息和连接事件处理
pub struct ServerMessageWrapper {
    /// 事件处理器，用于处理所有事件和消息
    pub(crate) event_handler: Arc<dyn ServerEventHandler>,
    /// 连接管理器（用于发送响应和更新连接状态）
    connection_manager: Option<Arc<crate::server::connection::ConnectionManager>>,
    /// 设备管理器（用于连接断开时清理设备）
    device_manager: Option<Arc<crate::server::device::DeviceManager>>,
    /// 消息解析器（用于序列化响应）
    parser: crate::common::MessageParser,
}

// 内部处理实现
impl ServerMessageWrapper {
    /// 创建新的服务端消息包装器
    ///
    /// # 参数
    /// - `event_handler`: 事件处理器，用于处理所有事件和消息
    /// - `connection_manager`: 连接管理器（可选，用于发送响应）
    /// - `device_manager`: 设备管理器（可选，用于设备管理）
    /// - `parser`: 消息解析器（用于序列化响应）
    pub fn new(
        event_handler: Arc<dyn ServerEventHandler>,
        connection_manager: Option<Arc<crate::server::connection::ConnectionManager>>,
        device_manager: Option<Arc<crate::server::device::DeviceManager>>,
        parser: crate::common::MessageParser,
    ) -> Self {
        Self {
            event_handler,
            connection_manager,
            device_manager,
            parser,
        }
    }

    /// 统一处理 handler 调用、响应处理和发送
    ///
    /// 封装完整流程：调用 handler → 自动 ACK（如果返回 None）→ 发送响应
    async fn handle_and_send_response<F>(
        &self,
        handler_future: F,
        message_id: String,
        connection_id: &str,
        log_context: &str,
    ) -> crate::common::error::Result<()>
    where
        F: std::future::Future<Output = crate::common::error::Result<Option<Frame>>>,
    {
        let response_frame = match handler_future.await {
            Ok(Some(response)) => {
                tracing::trace!(
                    "[ServerMessageWrapper] {}: 自定义响应: connection_id={}, message_id={}",
                    log_context,
                    connection_id,
                    message_id
                );
                response
            }
            Ok(None) => {
                tracing::trace!(
                    "[ServerMessageWrapper] {}: 自动 ACK: connection_id={}, message_id={}",
                    log_context,
                    connection_id,
                    message_id
                );
                use crate::common::protocol::Reliability;
                use crate::common::protocol::builder::{ack_message, frame_with_payload_command};
                frame_with_payload_command(ack_message(message_id, None), Reliability::AtLeastOnce)
            }
            Err(e) => {
                // Handler 处理失败，发送包含错误信息的 ACK
                tracing::error!(
                    "[ServerMessageWrapper] {}: 处理失败，发送错误 ACK, connection_id={}, message_id={}, error={}",
                    log_context,
                    connection_id,
                    message_id,
                    e
                );
                use crate::common::protocol::Reliability;
                use crate::common::protocol::builder::{ack_message, frame_with_payload_command};
                use std::collections::HashMap;

                // 在 metadata 中放入错误信息
                let mut metadata = HashMap::new();
                metadata.insert("error".to_string(), b"true".to_vec());
                metadata.insert("error_message".to_string(), e.to_string().into_bytes());

                frame_with_payload_command(
                    ack_message(message_id, Some(metadata)),
                    Reliability::AtLeastOnce,
                )
            }
        };

        // 发送响应（如果有连接管理器）
        if let Some(manager) = &self.connection_manager {
            self.send_response_frame_async(response_frame, connection_id, log_context, manager)
                .await;
        }

        Ok(())
    }

    /// 异步发送响应 Frame 到客户端
    async fn send_response_frame_async(
        &self,
        frame_to_send: Frame,
        connection_id: &str,
        log_context: &str,
        manager: &Arc<crate::server::connection::ConnectionManager>,
    ) {
        let manager_trait = Arc::clone(manager) as Arc<dyn ConnectionManagerTrait>;
        let conn_id: Arc<str> = Arc::from(connection_id);
        let message_id = frame_to_send.message_id.clone();
        let log_ctx: Arc<str> = Arc::from(log_context);

        tokio::spawn(async move {
            match manager_trait
                .send_frame_to(conn_id.as_ref(), &frame_to_send, None)
                .await
            {
                Ok(()) => tracing::debug!(
                    "[ServerMessageWrapper] {}: 已发送, connection_id={}, message_id={}",
                    log_ctx,
                    conn_id,
                    message_id
                ),
                Err(e) => {
                    tracing::warn!(
                        "[ServerMessageWrapper] {}: 发送失败, connection_id={}, message_id={}, error={}",
                        log_ctx,
                        conn_id,
                        message_id,
                        e
                    );
                }
            }
        });
    }

    /// 异步更新连接活跃时间
    fn update_connection_active_async(&self, connection_id: &str) {
        if let Some(manager) = &self.connection_manager {
            let manager_trait = Arc::clone(manager) as Arc<dyn ConnectionManagerTrait>;
            // 使用 Arc<str> 避免 String clone，减少内存分配
            let conn_id: Arc<str> = Arc::from(connection_id);
            tokio::spawn(async move {
                let _ = manager_trait.update_connection_active(&conn_id).await;
            });
        }
    }

    /// 处理系统命令
    async fn handle_system_command(
        &self,
        frame: &Frame,
        sys_type: i32,
        connection_id: &str,
    ) -> crate::common::error::Result<()> {
        use crate::common::protocol::flare::core::commands::system_command::Type as SysType;

        match SysType::try_from(sys_type) {
            Ok(SysType::Ping) => {
                // 处理 PING：回复 PONG 并更新连接活跃时间
                self.update_connection_active_async(connection_id);

                // 调用事件处理器，如果返回 None 则使用默认 PONG
                match self.event_handler.handle_ping(frame, connection_id).await {
                    Ok(Some(custom_response)) => {
                        if let Some(manager) = &self.connection_manager {
                            self.send_response_frame_async(
                                custom_response,
                                connection_id,
                                "handle_ping",
                                manager,
                            )
                            .await;
                        }
                    }
                    _ => {
                        // 默认处理：回复 PONG
                        use crate::common::protocol::{
                            Reliability, frame_with_system_command, pong,
                        };
                        let pong_frame =
                            frame_with_system_command(pong(), Reliability::AtLeastOnce);
                        if let Some(manager) = &self.connection_manager {
                            self.send_response_frame_async(
                                pong_frame,
                                connection_id,
                                "handle_ping",
                                manager,
                            )
                            .await;
                        }
                    }
                }
            }
            Ok(SysType::Pong) => {
                // 处理 PONG：更新连接活跃时间
                let _ = self.event_handler.handle_pong(frame, connection_id).await;
                self.update_connection_active_async(connection_id);
            }
            Ok(SysType::Event) => {
                // 处理 System::Event
                self.update_connection_active_async(connection_id);

                // Frame 需要 clone，因为需要移动到异步任务中
                let frame_clone = frame.clone();
                // message_id 是 String，需要 clone（因为需要移动到异步任务中）
                let frame_message_id = frame.message_id.clone();
                // 使用 Arc<str> 避免 String clone，减少内存分配
                let conn_id: Arc<str> = Arc::from(connection_id);
                let wrapper = self.clone_for_async();

                tokio::spawn(async move {
                    if let Err(e) = wrapper
                        .handle_and_send_response(
                            wrapper
                                .event_handler
                                .handle_system_event(&frame_clone, &conn_id),
                            frame_message_id,
                            &conn_id,
                            "handle_system_event",
                        )
                        .await
                    {
                        tracing::error!(
                            "[ServerMessageWrapper] handle_system_event: 处理失败, connection_id={}, error={}",
                            conn_id,
                            e
                        );
                    }
                });
            }
            Ok(SysType::DeviceState) => {
                // 处理 DEVICE_STATE：在读循环里就地等待，保证同一条连接的多次上报按到达顺序处理；
                // 回复帧排在此前已写出的帧之后，客户端据此判断「确认之前的帧都已到达」。
                self.update_connection_active_async(connection_id);

                match self
                    .event_handler
                    .handle_device_state(frame, connection_id)
                    .await
                {
                    Ok(Some(response)) => {
                        if let Some(manager) = &self.connection_manager {
                            self.send_response_frame_async(
                                response,
                                connection_id,
                                "handle_device_state",
                                manager,
                            )
                            .await;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            "[ServerMessageWrapper] handle_device_state: 处理失败, connection_id={}, error={}",
                            connection_id,
                            e
                        );
                    }
                }
            }
            Ok(SysType::NegotiationReady) => {
                // 处理 NEGOTIATION_READY：标记协商已确认
                self.update_connection_active_async(connection_id);

                if let Some(manager) = &self.connection_manager {
                    let manager_clone = Arc::clone(manager);
                    let conn_id: Arc<str> = Arc::from(connection_id);

                    tokio::spawn(async move {
                        if let Err(e) = (*manager_clone).mark_negotiation_confirmed(&conn_id) {
                            // 都不是服务端故障，不记 ERROR，见 negotiation_confirm_failure_level
                            if negotiation_confirm_failure_level(&manager_clone, &conn_id)
                                == tracing::Level::DEBUG
                            {
                                tracing::debug!(
                                    "[ServerMessageWrapper] 连接已移除，忽略协商确认: connection_id={}, error={}",
                                    conn_id,
                                    e
                                );
                            } else {
                                tracing::warn!(
                                    "[ServerMessageWrapper] 标记协商确认失败: connection_id={}, error={}",
                                    conn_id,
                                    e
                                );
                            }
                        } else {
                            tracing::debug!(
                                "[ServerMessageWrapper] 协商已确认: connection_id={}",
                                conn_id
                            );
                        }
                    });
                }
            }
            _ => {
                tracing::debug!("[ServerMessageWrapper] 未处理的系统命令类型: {}", sys_type);
            }
        }

        Ok(())
    }

    /// 处理载荷命令
    async fn handle_message_command(
        &self,
        _frame: &Frame,
        command: &crate::common::protocol::PayloadCommand,
        connection_id: &str,
    ) -> crate::common::error::Result<()> {
        let message_id = command.message_id.clone();

        use crate::common::protocol::flare::core::commands::payload_command::Type as PayloadType;

        if let Ok(payload_type) = PayloadType::try_from(command.r#type) {
            let handler_future = match payload_type {
                PayloadType::Message => self.event_handler.handle_message(command, connection_id),
                PayloadType::Event => self.event_handler.handle_event(command, connection_id),
                PayloadType::Ack => self.event_handler.handle_ack(command, connection_id),
                PayloadType::Data => self.event_handler.handle_data(command, connection_id),
                PayloadType::Unspecified => {
                    tracing::warn!(
                        "[ServerMessageWrapper] handle_message_command: Unspecified payload type, connection_id={}",
                        connection_id
                    );
                    return Ok(());
                }
            };

            // 统一处理并发送响应
            self.handle_and_send_response(
                handler_future,
                message_id,
                connection_id,
                "handle_message_command",
            )
            .await?;

            return Ok(());
        }

        tracing::error!(
            "[ServerMessageWrapper] handle_message_command: 无法识别载荷类型, connection_id={}, message_id={}",
            connection_id,
            message_id
        );
        Err(crate::common::error::FlareError::general_error(
            "Unknown message type",
        ))
    }

    /// 处理通知命令
    async fn handle_notification_command(
        &self,
        frame: &Frame,
        command: &crate::common::protocol::NotificationCommand,
        connection_id: &str,
    ) -> crate::common::error::Result<()> {
        // 统一处理并发送响应
        self.handle_and_send_response(
            self.event_handler
                .handle_notification_command(command, connection_id),
            frame.message_id.clone(),
            connection_id,
            "handle_notification_command",
        )
        .await
    }

    /// 处理自定义命令
    async fn handle_custom_command(
        &self,
        frame: &Frame,
        command: &crate::common::protocol::flare::core::commands::CustomCommand,
        connection_id: &str,
    ) -> crate::common::error::Result<()> {
        self.update_connection_active_async(connection_id);

        let cmd_name = command.name.clone();
        let log_ctx = format!("handle_custom_command[{}]", cmd_name);

        // 统一处理并发送响应
        self.handle_and_send_response(
            self.event_handler
                .handle_custom_command(command, connection_id),
            frame.message_id.clone(),
            connection_id,
            &log_ctx,
        )
        .await
    }

    /// 克隆用于异步任务（只克隆必要的字段）
    fn clone_for_async(&self) -> Self {
        Self {
            event_handler: self.event_handler.clone(),
            connection_manager: self.connection_manager.clone(),
            device_manager: self.device_manager.clone(),
            parser: self.parser.clone(),
        }
    }
}

impl Clone for ServerMessageWrapper {
    fn clone(&self) -> Self {
        self.clone_for_async()
    }
}

/// 实现连接处理的接口
#[async_trait]
impl ConnectionHandler for ServerMessageWrapper {
    /// 处理消息
    async fn handle_frame(
        &self,
        frame: &Frame,
        connection_id: &str,
    ) -> crate::common::error::Result<Option<Frame>> {
        // 根据命令类型路由到相应的处理方法
        if let Some(cmd) = &frame.command {
            match &cmd.r#type {
                Some(crate::common::protocol::flare::core::commands::command::Type::System(
                    sys_cmd,
                )) => {
                    // 处理系统命令（CONNECT 由 ServerCore 处理，这里只处理 PING/PONG/Event/NEGOTIATION_READY）
                    let sys_type = sys_cmd.r#type;
                    // CONNECT 命令由 ServerCore 统一处理，这里跳过
                    use crate::common::protocol::flare::core::commands::system_command::Type as SysType;
                    if sys_type != SysType::Connect as i32 {
                        let wrapper = self.clone_for_async();
                        let frame_clone = frame.clone();
                        // 使用 Arc<str> 避免 String clone，减少内存分配
                        let conn_id: Arc<str> = Arc::from(connection_id);
                        tokio::spawn(async move {
                            if let Err(e) = wrapper
                                .handle_system_command(&frame_clone, sys_type, &conn_id)
                                .await
                            {
                                tracing::error!("[ServerMessageWrapper] 处理系统命令失败: {}", e);
                            }
                        });
                    }
                }
                Some(crate::common::protocol::flare::core::commands::command::Type::Payload(
                    msg_cmd,
                )) => {
                    // 处理载荷命令
                    let wrapper = self.clone_for_async();
                    let msg_cmd_clone = msg_cmd.clone();
                    let frame_clone = frame.clone();
                    // 使用 Arc<str> 避免 String clone，减少内存分配
                    let conn_id: Arc<str> = Arc::from(connection_id);
                    // 不能 spawn：同一连接的帧必须按序处理。上游（观察者）已经是
                    // 「每连接一个消费任务」，这里直接 await 既保序，也不会阻塞其它连接。
                    // 以前每帧一个任务，等于把排好的顺序又打散——连发的消息会乱序拿到 seq。
                    if let Err(e) = wrapper
                        .handle_message_command(&frame_clone, &msg_cmd_clone, &conn_id)
                        .await
                    {
                        tracing::error!("[ServerMessageWrapper] 处理消息命令失败: {}", e);
                    }
                }
                Some(
                    crate::common::protocol::flare::core::commands::command::Type::Notification(
                        notif_cmd,
                    ),
                ) => {
                    // 处理通知命令
                    let wrapper = self.clone_for_async();
                    let notif_cmd_clone = notif_cmd.clone();
                    let frame_clone = frame.clone();
                    // 使用 Arc<str> 避免 String clone，减少内存分配
                    let conn_id: Arc<str> = Arc::from(connection_id);
                    // 不能 spawn：同一连接的帧必须按序处理。上游（观察者）已经是
                    // 「每连接一个消费任务」，这里直接 await 既保序，也不会阻塞其它连接。
                    // 以前每帧一个任务，等于把排好的顺序又打散——连发的消息会乱序拿到 seq。
                    if let Err(e) = wrapper
                        .handle_notification_command(&frame_clone, &notif_cmd_clone, &conn_id)
                        .await
                    {
                        tracing::error!("[ServerMessageWrapper] 处理通知命令失败: {}", e);
                    }
                }
                Some(crate::common::protocol::flare::core::commands::command::Type::Custom(
                    custom_cmd,
                )) => {
                    // 处理自定义命令
                    let wrapper = self.clone_for_async();
                    let custom_cmd_clone = custom_cmd.clone();
                    let frame_clone = frame.clone();
                    // 使用 Arc<str> 避免 String clone，减少内存分配
                    let conn_id: Arc<str> = Arc::from(connection_id);
                    tokio::spawn(async move {
                        if let Err(e) = wrapper
                            .handle_custom_command(&frame_clone, &custom_cmd_clone, &conn_id)
                            .await
                        {
                            tracing::error!("[ServerMessageWrapper] 处理自定义命令失败: {}", e);
                        }
                    });
                }
                None => {
                    tracing::debug!("[ServerMessageWrapper] 未处理的命令类型");
                }
            }
        }

        // 消息处理是异步的，这里不返回响应
        Ok(None)
    }

    async fn on_connect(&self, connection_id: &str) -> crate::common::error::Result<()> {
        info!("[ServerMessageWrapper] 新连接: {}", connection_id);
        self.event_handler.on_connect(connection_id).await
    }

    async fn on_disconnect(&self, connection_id: &str) -> crate::common::error::Result<()> {
        info!("[ServerMessageWrapper] 连接断开: {}", connection_id);

        // 通知事件处理器
        let _ = self.event_handler.on_disconnect(connection_id, None).await;

        // 从连接表移除。放在通知之后，业务层处理断开时还能读到这条连接的信息。
        //
        // 以前只通知业务层、不移除：客户端自己断开（登出、关页面）的连接留在连接表里，要等心跳
        // 超时才被清掉，期间仍计入连接数与用户数、仍在用户的投递路由里。设备冲突踢线时旧连接由
        // 冲突处理同步移除，第二设备随后登出就只剩它自己这条残留，表现为计数一直是 1。
        // 已经被移除的连接（踢线、写失败隔离、服务端主动断开）这里找不到，属正常。
        if let Some(manager) = &self.connection_manager
            && manager.remove_connection(connection_id).is_ok()
        {
            tracing::debug!(
                "[ServerMessageWrapper] 已从连接表移除断开的连接: connection_id={}",
                connection_id
            );
        }

        // 清理设备（如果有设备管理器）
        //
        // 按连接 ID 反查设备归属（租户，用户）后精确移除，不依赖连接信息此刻是否还在连接管理器里，
        // 也不会误删其他租户同名用户的设备。
        if let Some(device_mgr) = &self.device_manager {
            let device_mgr_clone = device_mgr.clone();
            // 使用 Arc<str> 避免 String clone，减少内存分配
            let conn_id: Arc<str> = Arc::from(connection_id);

            tokio::spawn(async move {
                if let Some(owner) = device_mgr_clone.remove_connection(&conn_id).await {
                    tracing::info!(
                        "[ServerMessageWrapper] Successfully removed device from DeviceManager: tenant_id={}, user_id={}, connection_id={}",
                        owner.tenant_id(),
                        owner.user_id(),
                        conn_id
                    );
                }
            });
        }

        Ok(())
    }
}

/// NEGOTIATION_READY 确认失败时的日志级别。
///
/// 确认是 spawn 出去执行的，排队期间连接被移除（设备冲突踢线、吊销、对端断开）是常态，
/// 连接已不在连接管理器里时记 debug。连接还在却确认失败，说明客户端在协商完成前发了
/// NEGOTIATION_READY，是客户端时序问题，记 warn。两种都不是服务端故障，不记 ERROR。
fn negotiation_confirm_failure_level(
    manager: &crate::server::connection::ConnectionManager,
    connection_id: &str,
) -> tracing::Level {
    if manager.get_connection(connection_id).is_none() {
        tracing::Level::DEBUG
    } else {
        tracing::Level::WARN
    }
}

// 扩展方法（不在 trait 中）
impl ServerMessageWrapper {
    /// 处理错误事件
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `error`: 错误信息
    pub(crate) async fn on_error(
        &self,
        connection_id: &str,
        error: &str,
    ) -> crate::common::error::Result<()> {
        // warn 而不是 error：这是单条连接的传输错误（QUIC 对端直接关连接、读流出错、协议错误），
        // 不是服务端故障，客户端进程被杀、网络切换都会走到这里；连接随后按断开清理。
        tracing::warn!(
            "[ServerMessageWrapper] 连接错误: connection_id={}, error={}",
            connection_id,
            error
        );

        // 通知事件处理器
        if let Err(e) = self.event_handler.on_error(connection_id, error).await {
            tracing::error!(
                "[ServerMessageWrapper] 事件处理器处理错误失败: connection_id={}, error={}",
                connection_id,
                e
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::connection::ConnectionManager;
    use crate::transport::connection::Connection;
    use crate::transport::events::ArcObserver;

    struct IdleConnection;

    #[async_trait]
    impl Connection for IdleConnection {
        fn add_observer(&mut self, _observer: ArcObserver) {}
        fn remove_observer(&mut self, _observer: ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> crate::common::error::Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> crate::common::error::Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> std::time::Instant {
            std::time::Instant::now()
        }
        fn update_active_time(&mut self) {}
    }

    /// NEGOTIATION_READY 确认失败从不记 ERROR：连接已被移除（踢线、断开）是良性竞争，记 debug；
    /// 连接还在但协商未完成是客户端时序问题，记 warn。
    #[test]
    fn negotiation_confirm_failures_are_not_server_errors() {
        let manager = ConnectionManager::new();
        assert!(manager.mark_negotiation_confirmed("gone").is_err());
        assert_eq!(
            negotiation_confirm_failure_level(&manager, "gone"),
            tracing::Level::DEBUG
        );

        manager
            .add_connection("c1".to_string(), Box::new(IdleConnection), None, true)
            .unwrap();
        assert!(manager.mark_negotiation_confirmed("c1").is_err());
        assert_eq!(
            negotiation_confirm_failure_level(&manager, "c1"),
            tracing::Level::WARN
        );
    }
}
