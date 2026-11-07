//! 客户端传输协议公共模块
//!
//! 提供 WebSocket 和 QUIC 客户端共享的逻辑和辅助函数

use crate::client::transports::ClientCore;
use crate::common::error::{FlareError, Result};
use crate::common::protocol::Frame;
use crate::transport::connection::Connection;
use crate::transport::events::{ConnectionEvent, ConnectionObserver};
use std::sync::Arc;
use tokio::sync::Mutex;

/// 客户端消息观察者（公共实现）
///
/// 委托给 ClientCore 处理消息和连接事件
pub struct ClientMessageObserver {
    core: Arc<ClientCore>,
}

impl ClientMessageObserver {
    /// 创建新的消息观察者
    pub fn new(core: Arc<ClientCore>) -> Self {
        Self { core }
    }
}

impl ClientMessageObserver {
    /// 协商完成前读到的帧只可能是 CONNECT_ACK、拒绝 CONNECT 的 ERROR，或服务端停机时的 CLOSE。
    /// ERROR 与 CLOSE 必须在读循环里同步登记：服务端发完它紧接着就关连接，关连接事件是同步处理的，
    /// 而帧的处理在另一个任务里，晚一步就只剩一个没有原因的「连接被关」。
    fn record_pre_negotiation_rejection(&self, data: &[u8]) {
        use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        use crate::common::protocol::flare::core::commands::command::Type;
        use crate::common::protocol::flare::core::commands::system_command::Type as SystemType;

        if let Ok(frame) = PRE_NEGOTIATION_PARSER.parse(data)
            && let Some(Type::System(system)) =
                frame.command.as_ref().and_then(|c| c.r#type.as_ref())
            && (system.r#type == SystemType::Error as i32
                || system.r#type == SystemType::Close as i32)
        {
            self.core.record_connect_rejection(system);
        }
    }
}

impl ConnectionObserver for ClientMessageObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                let core = Arc::clone(&self.core);
                let data_clone = data.clone();
                // 帧读出这一刻的协商状态：处理任务可能晚于随后的关连接事件执行（见
                // `ClientCore::handle_received_message`）。
                let negotiated = core.is_negotiation_completed();
                if !negotiated {
                    self.record_pre_negotiation_rejection(data);
                }
                #[cfg(target_arch = "wasm32")]
                {
                    // Browser onmessage is sync; queue bytes and drain on the LocalSet task
                    // that is already running `wait_for_negotiation` / `run_async`.
                    // 入队时记一帧在途，出队处理完或被丢弃时记完成。
                    let _ = negotiated;
                    core.push_wasm_inbound(data_clone);
                    let core_drain = Arc::clone(&core);
                    crate::client::wasm_tokio::spawn_detached(async move {
                        core_drain.drain_wasm_inbound().await;
                    });
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    // 记一帧在途；守卫随处理任务结束 drop 时记完成——任务 panic、被取消或没跑起来
                    // 也一样，在途数不会卡住而让之后每次对端关闭都白等 INBOUND_DRAIN_BOUND。
                    let done = core.inbound_frame_read();
                    crate::client::runtime::spawn_client_task(async move {
                        let _done = done;
                        core.handle_received_message(data_clone, negotiated).await;
                    });
                }
            }
            ConnectionEvent::Disconnected(_) | ConnectionEvent::Error(_)
                if self.core.should_defer_close_until_inbound_drained() =>
            {
                // 协商完成后对端关了连接，而读出的帧还没处理完（典型：服务端发完 KICKED 紧接着关连接）。
                // 等它们处理完再处理断开：被踢先报给观察者，随后的对端关闭不再重复上报。最多等
                // INBOUND_DRAIN_BOUND，处理卡住时照常上报断开。
                let core = Arc::clone(&self.core);
                let event = event.clone();
                crate::client::runtime::spawn_client_task(async move {
                    if !core
                        .wait_inbound_drained(super::client_core::INBOUND_DRAIN_BOUND)
                        .await
                    {
                        tracing::debug!(
                            "[ClientCore] inbound frames still in flight after the close; reporting the close anyway"
                        );
                    }
                    core.handle_connection_event(&event);
                });
            }
            ConnectionEvent::Connected
            | ConnectionEvent::Disconnected(_)
            | ConnectionEvent::Error(_) => {
                // 单一生命周期入口：pending 清理、协商状态、上层观察者均由 ClientCore 负责
                self.core.handle_connection_event(event);
            }
        }
    }
}

/// 客户端连接辅助函数
pub struct ClientConnectionHelper;

impl ClientConnectionHelper {
    /// 发送 Frame（内部实现）
    ///
    /// 统一处理消息序列化和发送逻辑
    pub async fn send_frame_internal(
        core: &ClientCore,
        connection: Option<&Arc<Mutex<Box<dyn Connection>>>>,
        frame: &Frame,
    ) -> Result<()> {
        if !core.can_send() {
            return Err(FlareError::connection_failed(
                "Cannot send: connection state is not ready".to_string(),
            ));
        }

        // 检查协商状态
        let negotiation_completed = core.is_negotiation_completed();

        let parser = core.parser.lock().await;
        tracing::trace!(
            "[ClientConnectionHelper] 发送消息: message_id={}, format={:?}",
            frame.message_id,
            parser.default_format()
        );

        // 如果协商未完成，记录警告（但允许发送，因为可能是系统消息）
        if !negotiation_completed {
            tracing::warn!(
                "[ClientConnectionHelper] 协商未完成但尝试发送消息: message_id={}, format={:?}, compression={:?}, encryption={:?}",
                frame.message_id,
                parser.default_format(),
                parser.default_compression(),
                parser.default_encryption()
            );
        }

        let data = parser.serialize(frame)?;
        drop(parser);

        let conn =
            connection.ok_or_else(|| FlareError::connection_failed("Not connected".to_string()))?;

        let mut c = conn.lock().await;
        c.send(&data).await?;
        Ok(())
    }

    /// 断开连接（统一处理）
    pub async fn disconnect_internal(
        connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
        core: &mut ClientCore,
    ) -> Result<()> {
        core.state_manager
            .set_state(crate::client::connection::ConnectionState::Disconnecting);

        // 标记我方主动断开，后续读循环若收到 KICK 不再向观察者通知「被踢」（重复登录/协议竞速场景）
        core.set_disconnect_requested(true);
        tracing::info!("[ClientCore] 主动断开（disconnect 请求）");

        // 停止心跳
        core.stop_heartbeat();

        // 先清掉共享连接槽，再用局部 Arc 关闭连接，避免
        // connection -> observer -> cloned ClientCore -> client_connection -> connection
        // 形成强引用环，导致切账号后旧引擎和上层 listener 无法释放。
        core.clear_client_connection();

        let close_result = if let Some(conn) = connection {
            let mut c = conn.lock().await;
            c.close().await
        } else {
            Ok(())
        };

        core.cancel_all_pending_responses().await;

        // 通知连接断开事件
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            crate::client::link::CLIENT_DISCONNECTED_REASON.to_string(),
        ));

        close_result
    }

    /// 设置连接观察者并发送 CONNECT 消息
    ///
    /// 统一处理连接设置和 CONNECT 消息发送
    pub async fn setup_connection_and_send_connect(
        connection: Arc<Mutex<Box<dyn Connection>>>,
        core: &mut ClientCore,
        observer: Arc<dyn ConnectionObserver>,
    ) -> Result<()> {
        // 立即设置 client_connection（用于被踢时断开连接）
        core.set_client_connection(Arc::clone(&connection));

        // 添加观察者
        {
            let mut conn = connection.lock().await;
            conn.add_observer(observer);
        }

        // 发送 CONNECT 消息进行协商
        core.send_connect_message(Arc::clone(&connection)).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::config::ClientConfig;

    struct RecordingObserver(std::sync::Mutex<Vec<String>>);

    impl ConnectionObserver for RecordingObserver {
        fn on_event(&self, event: &ConnectionEvent) {
            if let ConnectionEvent::Disconnected(reason) = event {
                self.0.lock().unwrap().push(reason.clone());
            }
        }
    }

    /// 对端已关闭的连接：本端再关不上报任何事件。
    struct ClosedConnection;

    #[async_trait::async_trait]
    impl Connection for ClosedConnection {
        fn add_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        fn remove_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            Err(FlareError::connection_closed("closed".to_string()))
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> crate::common::platform::MonotonicInstant {
            crate::common::platform::monotonic_now()
        }
        fn update_active_time(&mut self) {}
    }

    /// 服务端发完 KICKED 紧接着关连接：读循环先读出 KICKED（交给处理任务），随即同步收到对端关闭。
    /// 断开要等 KICKED 处理完再处理：观察者只收到一次 DEVICE_CONFLICT，前面没有对端关闭。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_close_right_after_a_kick_is_reported_as_the_kick_only() {
        use crate::common::kick::{DEVICE_CONFLICT_REASON, device_conflict_kicked};
        use crate::common::protocol::{Reliability, frame_with_system_command};

        let mut core = ClientCore::new(&ClientConfig::default());
        core.set_client_connection(Arc::new(Mutex::new(
            Box::new(ClosedConnection) as Box<dyn Connection>
        )));
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            crate::common::protocol::SerializationFormat::Protobuf,
            crate::common::compression::CompressionAlgorithm::Gzip,
            crate::common::encryption::EncryptionAlgorithm::None,
        )
        .await;
        core.stop_heartbeat();
        let recorder = Arc::new(RecordingObserver(std::sync::Mutex::new(Vec::new())));
        core.add_observer(recorder.clone());
        let kicked = frame_with_system_command(
            device_conflict_kicked(&crate::common::device::DevicePlatform::PC, "new-device"),
            Reliability::AtLeastOnce,
        );
        let data = core
            .parser
            .lock()
            .await
            .serialize(&kicked)
            .expect("serialize");

        let observer = ClientMessageObserver::new(Arc::new(core.clone()));
        observer.on_event(&ConnectionEvent::Message(data));
        observer.on_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".to_string(),
        ));

        for _ in 0..100 {
            if !recorder.0.lock().unwrap().is_empty() && !core.has_inbound_in_flight() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            *recorder.0.lock().unwrap(),
            vec![DEVICE_CONFLICT_REASON.to_string()]
        );
        assert!(!core.can_send());
        assert!(!core.is_negotiation_completed());
    }

    /// 上层观察者处理入站帧时 panic。
    struct PanicsOnFrames;

    impl ConnectionObserver for PanicsOnFrames {
        fn on_event(&self, event: &ConnectionEvent) {
            if let ConnectionEvent::Message(_) = event {
                panic!("observer panicked while handling a frame");
            }
        }
    }

    /// 处理入站帧的任务 panic（上层观察者出错）：这帧照样记为处理完。否则在途数卡在 1，此后每次
    /// 协商完成后的对端关闭都要先白等满 INBOUND_DRAIN_BOUND 才处理。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_frame_whose_handler_panics_does_not_delay_later_closes() {
        use crate::common::protocol::{Reliability, builder::pong, frame_with_system_command};

        let mut core = ClientCore::new(&ClientConfig::default());
        core.set_client_connection(Arc::new(Mutex::new(
            Box::new(ClosedConnection) as Box<dyn Connection>
        )));
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            crate::common::protocol::SerializationFormat::Protobuf,
            crate::common::compression::CompressionAlgorithm::Gzip,
            crate::common::encryption::EncryptionAlgorithm::None,
        )
        .await;
        core.stop_heartbeat();
        core.add_observer(Arc::new(PanicsOnFrames));
        let data = core
            .parser
            .lock()
            .await
            .serialize(&frame_with_system_command(pong(), Reliability::BestEffort))
            .expect("serialize");

        let observer = ClientMessageObserver::new(Arc::new(core.clone()));
        observer.on_event(&ConnectionEvent::Message(data));
        for _ in 0..50 {
            if !core.has_inbound_in_flight() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            !core.has_inbound_in_flight(),
            "a frame whose handler panicked is still counted as in flight"
        );
        assert!(
            !core.should_defer_close_until_inbound_drained(),
            "a later peer close would wait INBOUND_DRAIN_BOUND for a frame that is gone"
        );
    }

    /// 在途帧的守卫在处理任务被取消、或任务还没跑起来就被丢掉时也记完成。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn an_inbound_frame_counts_as_done_when_its_task_is_cancelled_or_never_runs() {
        let core = ClientCore::new(&ClientConfig::default());

        let done = core.inbound_frame_read();
        let cancelled = tokio::spawn(async move {
            let _done = done;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        assert!(core.has_inbound_in_flight());
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        assert!(!core.has_inbound_in_flight());

        let done = core.inbound_frame_read();
        let never_polled = async move {
            let _done = done;
        };
        drop(never_polled);
        assert!(!core.has_inbound_in_flight());
        assert!(
            core.wait_inbound_drained(std::time::Duration::from_millis(50))
                .await
        );
    }
}
