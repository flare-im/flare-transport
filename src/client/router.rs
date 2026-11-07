//! 客户端消息路由
//!
//! 根据消息类型将消息路由到不同的处理器
//! 支持自定义路由规则和处理器

use crate::common::error::Result;
use crate::common::protocol::Frame;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

/// 消息处理器
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// 处理消息
    ///
    /// # 参数
    /// - `frame`: 接收到的消息帧
    ///
    /// # 返回
    /// 如果需要回复，返回 `Some(Frame)`，否则返回 `None`
    async fn handle(&self, frame: &Frame) -> Result<Option<Frame>>;
}

/// 消息路由
///
/// 根据消息类型将消息路由到不同的处理器
pub struct MessageRouter {
    /// 路由规则：消息类型 -> 处理器
    handlers: HashMap<String, Vec<Arc<dyn MessageHandler>>>,
    /// 默认处理器（当没有匹配的路由时使用）
    default_handler: Option<Arc<dyn MessageHandler>>,
}

impl MessageRouter {
    /// 创建新的消息路由
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            default_handler: None,
        }
    }

    /// 注册处理器
    ///
    /// # 参数
    /// - `route`: 路由键（例如 "system.ping", "message.chat" 等）
    /// - `handler`: 消息处理器
    pub fn register(&mut self, route: impl Into<String>, handler: Arc<dyn MessageHandler>) {
        let route = route.into();
        self.handlers.entry(route).or_default().push(handler);
    }

    /// 设置默认处理器
    pub fn set_default_handler(&mut self, handler: Arc<dyn MessageHandler>) {
        self.default_handler = Some(handler);
    }

    /// 路由消息
    ///
    /// # 参数
    /// - `frame`: 要路由的消息帧
    ///
    /// # 返回
    /// 所有处理器的回复（如果有）
    pub async fn route(&self, frame: &Frame) -> Result<Vec<Frame>> {
        let route_key = Self::extract_route_key(frame);
        debug!(
            "Routing message: route={}, frame_id={}",
            route_key, frame.message_id
        );

        let mut replies = Vec::new();

        // 查找匹配的处理器
        if let Some(handlers) = self.handlers.get(&route_key) {
            for handler in handlers {
                match handler.handle(frame).await {
                    Ok(Some(reply)) => {
                        replies.push(reply);
                    }
                    Ok(None) => {
                        // 处理器不需要回复
                    }
                    Err(e) => {
                        warn!("Handler error for route {}: {}", route_key, e);
                    }
                }
            }
        } else if let Some(ref default_handler) = self.default_handler {
            // 使用默认处理器
            match default_handler.handle(frame).await {
                Ok(Some(reply)) => {
                    replies.push(reply);
                }
                Ok(None) => {
                    // 默认处理器不需要回复
                }
                Err(e) => {
                    warn!("Default handler error: {}", e);
                }
            }
        } else {
            debug!("No handler found for route: {}", route_key);
        }

        Ok(replies)
    }

    /// 从 Frame 中提取路由键
    fn extract_route_key(frame: &Frame) -> String {
        // 根据 Command 类型提取路由键
        if let Some(ref command) = frame.command {
            match &command.r#type {
                Some(crate::common::protocol::command::Type::System(sys_cmd)) => {
                    // SystemCommand 的 type 是 i32，使用 TryFrom 替代已弃用的 from_i32
                    use crate::common::protocol::system_command::Type as SysType;
                    use std::convert::TryFrom;
                    match SysType::try_from(sys_cmd.r#type) {
                        Ok(SysType::Connect) => "system.connect".to_string(),
                        Ok(SysType::ConnectAck) => "system.connect_ack".to_string(),
                        Ok(SysType::Close) => "system.close".to_string(),
                        Ok(SysType::Ping) => "system.ping".to_string(),
                        Ok(SysType::Pong) => "system.pong".to_string(),
                        Ok(SysType::Error) => "system.error".to_string(),
                        Ok(SysType::Event) => "system.event".to_string(),
                        _ => "system.unknown".to_string(),
                    }
                }
                Some(crate::common::protocol::command::Type::Payload(msg_cmd)) => {
                    // 使用消息类型作为路由键（使用 TryFrom 替代已弃用的 from_i32）
                    use crate::common::protocol::payload_command::Type as MsgType;
                    use std::convert::TryFrom;
                    match MsgType::try_from(msg_cmd.r#type) {
                        Ok(MsgType::Message) => "payload.message".to_string(),
                        Ok(MsgType::Event) => "payload.event".to_string(),
                        Ok(MsgType::Ack) => "payload.ack".to_string(),
                        Ok(MsgType::Data) => "payload.data".to_string(),
                        _ => format!("payload.{}", msg_cmd.r#type),
                    }
                }
                Some(crate::common::protocol::command::Type::Notification(notif_cmd)) => {
                    use crate::common::protocol::notification_command::Type as NotifType;
                    use std::convert::TryFrom;
                    match NotifType::try_from(notif_cmd.r#type) {
                        Ok(NotifType::System) => "notification.system".to_string(),
                        Ok(NotifType::Broadcast) => "notification.broadcast".to_string(),
                        Ok(NotifType::Alert) => "notification.alert".to_string(),
                        Ok(NotifType::User) => "notification.user".to_string(),
                        Ok(NotifType::Connection) => "notification.connection".to_string(),
                        _ => format!("notification.{}", notif_cmd.r#type),
                    }
                }
                Some(crate::common::protocol::command::Type::Custom(custom_cmd)) => {
                    format!("custom.{}", custom_cmd.name)
                }
                None => "unknown".to_string(),
            }
        } else {
            "unknown".to_string()
        }
    }

    /// 移除指定路由的所有处理器
    pub fn remove_route(&mut self, route: &str) {
        self.handlers.remove(route);
    }

    /// 清除所有路由
    pub fn clear(&mut self) {
        self.handlers.clear();
        self.default_handler = None;
    }
}

impl Default for MessageRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// 简单的消息处理器实现
pub struct SimpleHandler {
    #[allow(clippy::type_complexity)]
    handler: Box<dyn Fn(&Frame) -> Result<Option<Frame>> + Send + Sync>,
}

impl SimpleHandler {
    /// 创建新的简单处理器
    pub fn new<F>(handler: F) -> Self
    where
        F: Fn(&Frame) -> Result<Option<Frame>> + Send + Sync + 'static,
    {
        Self {
            handler: Box::new(handler),
        }
    }
}

#[async_trait]
impl MessageHandler for SimpleHandler {
    async fn handle(&self, frame: &Frame) -> Result<Option<Frame>> {
        (self.handler)(frame)
    }
}

/// 异步消息处理器实现
pub struct AsyncHandler {
    #[allow(clippy::type_complexity)]
    handler: Arc<
        dyn Fn(
                &Frame,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Option<Frame>>> + Send + '_>,
            > + Send
            + Sync,
    >,
}

impl AsyncHandler {
    /// 创建新的异步处理器
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(&Frame) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Option<Frame>>> + Send + 'static,
    {
        Self {
            handler: Arc::new(move |frame| Box::pin(handler(frame))),
        }
    }
}

#[async_trait]
impl MessageHandler for AsyncHandler {
    async fn handle(&self, frame: &Frame) -> Result<Option<Frame>> {
        (self.handler)(frame).await
    }
}
