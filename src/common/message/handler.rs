//! 消息处理器模块
//!
//! 使用观察者模式处理不同类型的消息

use crate::common::error::Result;
use crate::common::protocol::flare::core::commands::{
    notification_command::Type as NotificationType, payload_command::Type as PayloadType,
    system_command::Type as SystemType,
};
use crate::common::protocol::{
    CustomCommand, Frame, NotificationCommand, PayloadCommand, SystemCommand,
};
use std::sync::Arc;
use std::sync::Mutex;

/// 消息事件类型
#[derive(Debug, Clone)]
pub enum MessageEvent {
    /// 系统命令事件
    System {
        frame: Frame,
        command: SystemCommand,
        command_type: SystemType,
    },
    /// 载荷命令事件
    Message {
        frame: Frame,
        command: PayloadCommand,
        command_type: PayloadType,
    },
    /// 通知命令事件
    Notification {
        frame: Frame,
        command: NotificationCommand,
        command_type: NotificationType,
    },
    /// 自定义命令事件
    Custom {
        frame: Frame,
        command: CustomCommand,
    },
    /// 未知或无效的命令
    Unknown(Frame),
}

/// 消息观察者 trait
///
/// 实现此 trait 以处理不同类型的消息事件
pub trait MessageObserver: Send + Sync {
    /// 处理系统命令
    fn on_system_command(
        &self,
        frame: &Frame,
        command: &SystemCommand,
        command_type: SystemType,
    ) -> Result<()> {
        let _ = (frame, command, command_type);
        Ok(())
    }

    /// 处理载荷命令
    fn on_message_command(
        &self,
        frame: &Frame,
        command: &PayloadCommand,
        command_type: PayloadType,
    ) -> Result<()> {
        let _ = (frame, command, command_type);
        Ok(())
    }

    /// 处理通知命令
    fn on_notification_command(
        &self,
        frame: &Frame,
        command: &NotificationCommand,
        command_type: NotificationType,
    ) -> Result<()> {
        let _ = (frame, command, command_type);
        Ok(())
    }

    /// 处理自定义命令
    fn on_custom_command(&self, frame: &Frame, command: &CustomCommand) -> Result<()> {
        let _ = (frame, command);
        Ok(())
    }

    /// 处理未知命令
    fn on_unknown_command(&self, frame: &Frame) -> Result<()> {
        let _ = frame;
        Ok(())
    }
}

/// 线程安全的消息观察者类型别名
pub type ArcMessageObserver = Arc<dyn MessageObserver>;

/// 消息处理器
///
/// 使用观察者模式处理消息，支持注册多个观察者
pub struct MessageHandler {
    observers: Arc<Mutex<Vec<ArcMessageObserver>>>,
}

impl MessageHandler {
    /// 创建新的消息处理器
    pub fn new() -> Self {
        Self {
            observers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// 添加观察者
    pub fn add_observer(&self, observer: ArcMessageObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.push(observer);
        }
    }

    /// 移除观察者
    pub fn remove_observer(&self, observer: ArcMessageObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.retain(|o| !Arc::ptr_eq(o, &observer));
        }
    }

    /// 处理消息 Frame
    ///
    /// 解析 Frame 并分发到相应的观察者
    pub fn handle_frame(&self, frame: Frame) -> Result<MessageEvent> {
        let event = Self::parse_frame_to_event(frame.clone())?;

        let observers = self.observers.lock().map_err(|_| {
            crate::common::error::FlareError::general_error("Failed to lock observers")
        })?;

        match &event {
            MessageEvent::System {
                frame,
                command,
                command_type,
            } => {
                for observer in observers.iter() {
                    if let Err(e) = observer.on_system_command(frame, command, *command_type) {
                        // 记录错误但继续处理其他观察者
                        eprintln!("Observer error handling system command: {:?}", e);
                    }
                }
            }
            MessageEvent::Message {
                frame,
                command,
                command_type,
            } => {
                for observer in observers.iter() {
                    if let Err(e) = observer.on_message_command(frame, command, *command_type) {
                        eprintln!("Observer error handling message command: {:?}", e);
                    }
                }
            }
            MessageEvent::Notification {
                frame,
                command,
                command_type,
            } => {
                for observer in observers.iter() {
                    if let Err(e) = observer.on_notification_command(frame, command, *command_type)
                    {
                        eprintln!("Observer error handling notification command: {:?}", e);
                    }
                }
            }
            MessageEvent::Custom { frame, command } => {
                for observer in observers.iter() {
                    if let Err(e) = observer.on_custom_command(frame, command) {
                        eprintln!("Observer error handling custom command: {:?}", e);
                    }
                }
            }
            MessageEvent::Unknown(frame) => {
                for observer in observers.iter() {
                    if let Err(e) = observer.on_unknown_command(frame) {
                        eprintln!("Observer error handling unknown command: {:?}", e);
                    }
                }
            }
        }

        Ok(event)
    }

    /// 将 Frame 解析为 MessageEvent
    fn parse_frame_to_event(frame: Frame) -> Result<MessageEvent> {
        let command = frame.command.as_ref().ok_or_else(|| {
            crate::common::error::FlareError::protocol_error("Frame missing command")
        })?;

        match &command.r#type {
            Some(crate::common::protocol::flare::core::commands::command::Type::System(
                system_cmd,
            )) => {
                // 直接从 i32 转换为枚举（使用 unsafe，因为 prost 生成的枚举是 repr(i32)）
                let command_type = match system_cmd.r#type {
                    0 => SystemType::Unspecified,
                    1 => SystemType::Connect,
                    2 => SystemType::ConnectAck,
                    3 => SystemType::Close,
                    4 => SystemType::Ping,
                    5 => SystemType::Pong,
                    6 => SystemType::Error,
                    7 => SystemType::Event,
                    _ => {
                        return Err(crate::common::error::FlareError::protocol_error(
                            "Invalid system command type",
                        ));
                    }
                };
                Ok(MessageEvent::System {
                    frame: frame.clone(),
                    command: system_cmd.clone(),
                    command_type,
                })
            }
            Some(crate::common::protocol::flare::core::commands::command::Type::Payload(
                msg_cmd,
            )) => {
                let command_type = match msg_cmd.r#type {
                    0 => PayloadType::Unspecified,
                    1 => PayloadType::Message,
                    2 => PayloadType::Event,
                    3 => PayloadType::Ack,
                    4 => PayloadType::Data,
                    _ => {
                        return Err(crate::common::error::FlareError::protocol_error(
                            "Invalid payload command type",
                        ));
                    }
                };
                Ok(MessageEvent::Message {
                    frame: frame.clone(),
                    command: msg_cmd.clone(),
                    command_type,
                })
            }
            Some(crate::common::protocol::flare::core::commands::command::Type::Notification(
                notif_cmd,
            )) => {
                let command_type = match notif_cmd.r#type {
                    0 => NotificationType::System,
                    1 => NotificationType::Broadcast,
                    2 => NotificationType::Alert,
                    3 => NotificationType::User,
                    4 => NotificationType::Connection,
                    _ => {
                        return Err(crate::common::error::FlareError::protocol_error(
                            "Invalid notification command type",
                        ));
                    }
                };
                Ok(MessageEvent::Notification {
                    frame: frame.clone(),
                    command: notif_cmd.clone(),
                    command_type,
                })
            }
            Some(crate::common::protocol::flare::core::commands::command::Type::Custom(
                custom_cmd,
            )) => Ok(MessageEvent::Custom {
                frame: frame.clone(),
                command: custom_cmd.clone(),
            }),
            None => Ok(MessageEvent::Unknown(frame)),
        }
    }

    /// 获取观察者数量
    pub fn observer_count(&self) -> usize {
        self.observers.lock().map(|obs| obs.len()).unwrap_or(0)
    }
}

impl Default for MessageHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::{Command, FrameBuilder, ping};

    struct TestObserver {
        system_count: Arc<Mutex<usize>>,
    }

    impl MessageObserver for TestObserver {
        fn on_system_command(
            &self,
            _frame: &Frame,
            _command: &SystemCommand,
            _command_type: SystemType,
        ) -> Result<()> {
            let mut count = self.system_count.lock().unwrap();
            *count += 1;
            Ok(())
        }
    }

    #[test]
    fn test_message_handler() {
        let handler = MessageHandler::new();
        let observer = Arc::new(TestObserver {
            system_count: Arc::new(Mutex::new(0)),
        });
        handler.add_observer(observer.clone());

        let frame = FrameBuilder::new()
            .with_command(Command {
                r#type: Some(crate::common::protocol::command::Type::System(ping())),
            })
            .build();

        let event = handler.handle_frame(frame).unwrap();
        match event {
            MessageEvent::System { .. } => {
                let count = observer.system_count.lock().unwrap();
                assert_eq!(*count, 1);
            }
            _ => panic!("Expected System event"),
        }
    }
}
