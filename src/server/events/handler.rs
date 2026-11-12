//! 服务端事件处理器 Trait
//!
//! 提供细化的命令处理方法，支持按命令类型处理

use crate::common::error::Result;
use crate::common::protocol::{
    Frame, NotificationCommand, PayloadCommand, flare::core::commands::CustomCommand,
};
use async_trait::async_trait;

/// 服务端事件处理器
///
/// 提供细化的命令处理方法，每个命令类型对应一个方法
/// 用户可以实现这个 trait 来自定义业务逻辑
#[async_trait]
pub trait ServerEventHandler: Send + Sync {
    /// 处理 CONNECT 系统命令（协商）
    ///
    /// 默认实现返回 None，表示使用默认处理
    async fn handle_connect(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let _ = (frame, connection_id);
        Ok(None)
    }

    /// 处理 PING 系统命令
    ///
    /// 默认实现返回 None，表示使用默认处理（自动回复 PONG）
    async fn handle_ping(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let _ = (frame, connection_id);
        Ok(None)
    }

    /// 处理 PONG 系统命令
    ///
    /// 默认实现返回 None，表示使用默认处理（更新连接活跃时间）
    async fn handle_pong(&self, frame: &Frame, connection_id: &str) -> Result<Option<Frame>> {
        let _ = (frame, connection_id);
        Ok(None)
    }

    /// 处理 MESSAGE 载荷命令（业务消息，需 ACK）
    async fn handle_message(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (command, connection_id);
        Ok(None)
    }

    /// 处理 EVENT 载荷命令（事件）
    async fn handle_event(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (command, connection_id);
        Ok(None)
    }

    /// 处理 ACK 载荷命令（确认）
    async fn handle_ack(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (command, connection_id);
        Ok(None)
    }

    /// 处理 DATA 载荷命令（普通数据传输）
    async fn handle_data(
        &self,
        command: &PayloadCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (command, connection_id);
        Ok(None)
    }

    /// 处理通知命令
    ///
    /// # 参数
    /// - `command`: 通知命令
    /// - `connection_id`: 连接 ID
    ///
    /// # 返回
    /// 可选回复 Frame
    async fn handle_notification_command(
        &self,
        command: &NotificationCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (command, connection_id);
        Ok(None)
    }

    /// 处理连接断开事件
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `reason`: 断开原因（如果有）
    async fn on_disconnect(&self, connection_id: &str, reason: Option<&str>) -> Result<()> {
        let _ = (connection_id, reason);
        Ok(())
    }

    /// 处理连接错误事件
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    /// - `error`: 错误信息
    async fn on_error(&self, connection_id: &str, error: &str) -> Result<()> {
        let _ = (connection_id, error);
        Ok(())
    }

    /// 处理自定义命令
    ///
    /// # 参数
    /// - `command`: 自定义命令
    /// - `connection_id`: 连接 ID
    ///
    /// # 返回
    /// 可选回复 Frame
    ///
    /// # 说明
    /// 默认实现返回 None，表示不处理自定义命令
    /// 如果需要处理自定义命令，可以重写此方法
    async fn handle_custom_command(
        &self,
        command: &CustomCommand,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (command, connection_id);
        Ok(None)
    }

    /// 处理连接建立完成事件（在 CONNECT 协商完成后调用）
    ///
    /// # 参数
    /// - `connection_id`: 连接 ID
    ///
    /// # 说明
    /// 此方法在 CONNECT 协商完成后调用，用于处理连接建立后的业务逻辑
    /// 默认实现为空，如果需要处理连接建立逻辑，可以重写此方法
    async fn on_connect(&self, connection_id: &str) -> Result<()> {
        let _ = connection_id;
        Ok(())
    }

    /// 处理系统事件（System::Event）
    ///
    /// # 参数
    /// - `frame`: 包含系统事件的 Frame
    /// - `connection_id`: 连接 ID
    ///
    /// # 返回
    /// 可选回复 Frame
    ///
    /// # 说明
    /// 默认实现返回 None，表示不处理系统事件
    /// 如果需要处理系统事件，可以重写此方法
    async fn handle_system_event(
        &self,
        frame: &Frame,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (frame, connection_id);
        Ok(None)
    }

    /// 处理设备状态上报（System::DeviceState）
    ///
    /// # 返回
    /// 可选回复 Frame，通常是一条 DEVICE_STATE_ACK。
    ///
    /// # 说明
    /// 在连接的读循环里按到达顺序依次调用，同一条连接上的多次上报不会乱序，
    /// 所以实现必须很快返回：只改内存状态，耗时的工作自行派发到后台任务。
    /// 默认实现返回 None，表示不处理也不回复。
    async fn handle_device_state(
        &self,
        frame: &Frame,
        connection_id: &str,
    ) -> Result<Option<Frame>> {
        let _ = (frame, connection_id);
        Ok(None)
    }
}
