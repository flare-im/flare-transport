//! 服务端停机时关闭长连接的约定。
//!
//! 服务端停止前先给每条在线连接下发一帧 CLOSE 系统命令：`message` 与 metadata `reason` 都是
//! [`SERVER_SHUTDOWN_REASON`]，metadata `code` 是 `SERVICE_UNAVAILABLE`，并带上建议的重连窗口
//! `retry_after_min_ms` / `retry_after_max_ms`（现为 [`SERVER_SHUTDOWN_RECONNECT_MIN`]–
//! [`SERVER_SHUTDOWN_RECONNECT_MAX`]，编码见 [`crate::common::link::ServerClose`]）。认得它的客户端
//! 随即以这个原因自行断开，并在窗口内随机挑一个时刻重连：停机是计划内的，新实例通常几秒内就绪；
//! 所有客户端在同一刻重连会把刚起来的实例冲垮，按错误立即重试只会撞上还没起来的实例。
//!
//! 不认得 CLOSE 的旧客户端照旧把它当作未知系统帧忽略，由服务端在宽限期满后关闭连接，行为与此前
//! 相同。服务端关闭时 WebSocket 的关闭帧是 1001（Going Away），原因同为 [`SERVER_SHUTDOWN_REASON`]；
//! QUIC 以应用错误码 0 关闭连接，原因也是它。

use crate::common::link::{CloseKind, ServerClose};
use crate::common::protocol::builder::frame_with_system_command;
use crate::common::protocol::flare::core::commands::system_command::Type as SystemType;
use crate::common::protocol::{Frame, Reliability, SystemCommand};
use std::time::Duration;

/// 服务端停机的类型化原因。
pub const SERVER_SHUTDOWN_REASON: &str = "SERVER_SHUTDOWN";

/// 停机 CLOSE 帧 metadata `code` 的取值：服务暂不可用，客户端应稍后重连。
pub const SERVER_SHUTDOWN_CODE: &str = "SERVICE_UNAVAILABLE";

/// 服务端下发停机 CLOSE 后等客户端自行断开的默认宽限；期满还在的连接由服务端关闭。
/// 按部署调整用 `ServerConfig::shutdown_close_grace`（各服务端构建器的 `with_shutdown_close_grace`）。
pub const SERVER_SHUTDOWN_CLOSE_GRACE: Duration = Duration::from_secs(1);

/// 服务端停机后，客户端第一次重连前至少等这么久。
pub const SERVER_SHUTDOWN_RECONNECT_MIN: Duration = Duration::from_secs(1);

/// 服务端停机后，客户端第一次重连前至多等这么久。
pub const SERVER_SHUTDOWN_RECONNECT_MAX: Duration = Duration::from_secs(5);

/// 停机时下发给在线连接的 CLOSE 系统命令，带 [`SERVER_SHUTDOWN_RECONNECT_MIN`]–
/// [`SERVER_SHUTDOWN_RECONNECT_MAX`] 的重连窗口。窗口怎么随集群规模计算属于服务端集群方案，
/// 这里沿用固定值。
pub fn server_shutdown_close() -> SystemCommand {
    ServerClose::new(CloseKind::ServerShutdown)
        .with_retry_window(SERVER_SHUTDOWN_RECONNECT_MIN, SERVER_SHUTDOWN_RECONNECT_MAX)
        .to_command()
}

/// 停机时下发给在线连接的 CLOSE 帧。
pub fn server_shutdown_close_frame() -> Frame {
    frame_with_system_command(server_shutdown_close(), Reliability::BestEffort)
}

/// 这条 CLOSE 系统命令是否表示服务端停机（看 `message` 或 metadata `reason`）。
pub fn is_server_shutdown_close(command: &SystemCommand) -> bool {
    command.r#type == SystemType::Close as i32
        && (command.message.trim() == SERVER_SHUTDOWN_REASON
            || command
                .metadata
                .get("reason")
                .is_some_and(|reason| reason.as_slice() == SERVER_SHUTDOWN_REASON.as_bytes()))
}

/// 断开原因是否为服务端停机：原因就是 [`SERVER_SHUTDOWN_REASON`]（WebSocket 关闭帧、QUIC 应用关闭
/// 带来的原因），或以 `SERVER_SHUTDOWN:` 开头（客户端收到 CLOSE 后自行断开时给出的原因）。
pub fn is_server_shutdown_reason(reason: &str) -> bool {
    let reason = reason.trim();
    reason
        .strip_prefix(SERVER_SHUTDOWN_REASON)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(':'))
}

/// 服务端停机后第一次重连前的等待：在 [`SERVER_SHUTDOWN_RECONNECT_MIN`] 与
/// [`SERVER_SHUTDOWN_RECONNECT_MAX`] 之间均匀随机，按毫秒取值。
pub fn server_shutdown_reconnect_delay() -> Duration {
    use rand::Rng;
    let min = SERVER_SHUTDOWN_RECONNECT_MIN.as_millis() as u64;
    let max = SERVER_SHUTDOWN_RECONNECT_MAX.as_millis() as u64;
    Duration::from_millis(rand::thread_rng().gen_range(min..=max))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::flare::core::commands::command::Type as CommandType;

    #[test]
    fn shutdown_close_carries_the_typed_reason_and_code() {
        let frame = server_shutdown_close_frame();
        let Some(CommandType::System(command)) =
            frame.command.as_ref().and_then(|c| c.r#type.as_ref())
        else {
            panic!("shutdown frame must carry a system command");
        };
        assert_eq!(command.r#type, SystemType::Close as i32);
        assert_eq!(command.message, SERVER_SHUTDOWN_REASON);
        assert_eq!(
            command.metadata.get("reason").map(Vec::as_slice),
            Some(SERVER_SHUTDOWN_REASON.as_bytes())
        );
        assert_eq!(
            command.metadata.get("code").map(Vec::as_slice),
            Some(SERVER_SHUTDOWN_CODE.as_bytes())
        );
        assert!(is_server_shutdown_close(command));
        let close = ServerClose::from_command(command).expect("typed shutdown close");
        assert_eq!(close.kind, CloseKind::ServerShutdown);
        assert_eq!(
            close.retry_window(),
            Some((SERVER_SHUTDOWN_RECONNECT_MIN, SERVER_SHUTDOWN_RECONNECT_MAX))
        );
    }

    #[test]
    fn only_shutdown_closes_are_recognised() {
        use crate::common::protocol::builder::close;
        assert!(!is_server_shutdown_close(&close(None, None)));
        assert!(!is_server_shutdown_close(&close(
            Some("MAINTENANCE".to_string()),
            None
        )));
        // 其他类型的系统命令带同样的文本也不算
        assert!(!is_server_shutdown_close(
            &crate::common::protocol::builder::kicked(SERVER_SHUTDOWN_REASON, None)
        ));
    }

    #[test]
    fn shutdown_reasons_are_matched_by_prefix_token_only() {
        assert!(is_server_shutdown_reason("SERVER_SHUTDOWN"));
        assert!(is_server_shutdown_reason(" SERVER_SHUTDOWN "));
        assert!(is_server_shutdown_reason(
            "SERVER_SHUTDOWN: server is shutting down"
        ));
        assert!(!is_server_shutdown_reason("SERVER_SHUTDOWNX"));
        assert!(!is_server_shutdown_reason("Closed by client"));
        assert!(!is_server_shutdown_reason("TENANT_UNAVAILABLE"));
        assert!(!is_server_shutdown_reason(""));
    }

    #[test]
    fn shutdown_reconnect_delay_stays_inside_the_jitter_window() {
        let mut distinct = std::collections::HashSet::new();
        for _ in 0..200 {
            let delay = server_shutdown_reconnect_delay();
            assert!(
                (SERVER_SHUTDOWN_RECONNECT_MIN..=SERVER_SHUTDOWN_RECONNECT_MAX).contains(&delay),
                "{delay:?}"
            );
            distinct.insert(delay);
        }
        // 抖动：200 次取值不该几乎全一样
        assert!(
            distinct.len() > 50,
            "only {} distinct delays",
            distinct.len()
        );
    }
}
