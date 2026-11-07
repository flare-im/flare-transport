//! 客户端构建器通用组件

use crate::client::Client;
use crate::common::config_types::{HeartbeatAppState, HeartbeatConfig, TransportProtocol};
use crate::common::error::Result;
use crate::common::protocol::Frame;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[cfg(not(target_arch = "wasm32"))]
use crate::client::HybridClient;

#[cfg(target_arch = "wasm32")]
use crate::client::WebSocketClient;

/// 未连接（或还没协商完成）时发送返回的错误。客户端不自己重连：重连由上层新建客户端完成。
fn not_connected() -> crate::common::error::FlareError {
    crate::common::error::FlareError::localized(
        crate::common::error::ErrorCode::NotConnected,
        "not connected; reconnecting is left to the caller",
    )
}

/// 服务端以结束会话的原因（设备冲突、租户不可用）踢掉这个客户端之后，发送返回的错误：
/// 不是可重试的连接错误，恢复要由应用重新登录后显式建连。
fn session_ended_by_server_error(reason: &str) -> crate::common::error::FlareError {
    crate::common::error::FlareError::localized(
        crate::common::error::ErrorCode::NotConnected,
        format!("{reason}: the server ended this session; not reconnecting"),
    )
}

/// 客户端包装器 — native 使用 HybridClient，WASM 使用 WebSocketClient。
#[derive(Clone)]
pub struct ClientWrapper {
    #[cfg(not(target_arch = "wasm32"))]
    client: Arc<Mutex<HybridClient>>,
    #[cfg(target_arch = "wasm32")]
    client: Arc<Mutex<WebSocketClient>>,
    /// 心跳验活唤醒，建客户端时从它的 core 取一份。core 的这份句柄在客户端整个生命周期内不变
    /// （竞速中各协议的 core 共享同一份），所以请求验活不必拿 `client` 锁，见 [`Self::probe_heartbeat`]。
    heartbeat_probe_wake: Arc<tokio::sync::Notify>,
}

impl ClientWrapper {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(client: HybridClient) -> Self {
        let heartbeat_probe_wake = client.core().heartbeat_probe_handle();
        Self {
            client: Arc::new(Mutex::new(client)),
            heartbeat_probe_wake,
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn new(client: WebSocketClient) -> Self {
        let heartbeat_probe_wake = client.core().heartbeat_probe_handle();
        Self {
            client: Arc::new(Mutex::new(client)),
            heartbeat_probe_wake,
        }
    }

    /// 建连并等到 CONNECT_ACK（native 走交错竞速，竞速返回即已协商完成）。最多等配置的单次握手时限。
    pub async fn connect(&self) -> Result<()> {
        let (core, limit) = {
            let mut client = self.client.lock().await;
            client.connect().await?;
            let core = client.core().clone();
            let limit = core.config.connect_timeout;
            (core, limit)
        };
        core.wait_for_negotiation(limit).await
    }

    pub async fn disconnect(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        client.disconnect().await
    }

    pub async fn send_frame_and_wait(&self, frame: &Frame, timeout: Duration) -> Result<Frame> {
        self.ensure_ready_for_send().await?;
        let mut client = self.client.lock().await;
        client.send_frame_and_wait(frame, timeout).await
    }

    pub async fn send_frame(&self, frame: &Frame) -> Result<()> {
        self.ensure_ready_for_send().await?;
        let mut client = self.client.lock().await;
        client.send_frame(frame).await
    }

    /// 发送前检查连接可用：未连接（含协商未完成）直接报错，不去连服务端。只有上层能建连，
    /// 包装层自己连上会与上层新建的连接并存（同一设备两条连接，服务端按设备冲突互踢）。
    async fn ensure_ready_for_send(&self) -> Result<()> {
        if self.is_send_ready().await {
            return Ok(());
        }
        let client = self.client.lock().await;
        if let Some(reason) = client.core().session_ended_by_server() {
            return Err(session_ended_by_server_error(&reason));
        }
        Err(not_connected())
    }

    async fn is_send_ready(&self) -> bool {
        let client = self.client.lock().await;
        client.is_connected()
            && client.core().can_send()
            && client.core().is_negotiation_completed()
    }

    pub async fn add_observer(&self, observer: crate::transport::events::ArcObserver) {
        let mut client = self.client.lock().await;
        client.add_observer(observer);
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn set_event_handler(
        &self,
        handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
    ) {
        let mut client = self.client.lock().await;
        client.core_mut().set_event_handler(handler);
    }

    pub async fn is_connected_async(&self) -> bool {
        let client = self.client.lock().await;
        client.is_connected()
    }

    pub async fn connection_id_async(&self) -> Option<String> {
        let client = self.client.lock().await;
        client.connection_id()
    }

    pub fn active_protocol(&self) -> TransportProtocol {
        #[cfg(not(target_arch = "wasm32"))]
        {
            crate::client::runtime::run_client_async(async {
                let client = self.client.lock().await;
                client.active_protocol()
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            TransportProtocol::WebSocket
        }
    }

    pub async fn with_core<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&crate::client::transports::ClientCore) -> R,
    {
        let client = self.client.lock().await;
        f(client.core())
    }

    pub async fn update_heartbeat_config(&self, config: HeartbeatConfig) {
        self.with_core(|core| {
            core.update_heartbeat_config(|current| {
                *current = config;
            });
        })
        .await;
    }

    pub async fn set_heartbeat_app_state(&self, state: HeartbeatAppState) {
        self.with_core(|core| {
            core.set_heartbeat_app_state(state);
        })
        .await;
    }

    /// 请求一次即时验活，不拿 `client` 锁：`send_frame_and_wait` 等回执期间一直占着这把锁，
    /// 而「发出去迟迟没回执」正是最需要验活的时候。
    pub fn probe_heartbeat(&self) {
        self.heartbeat_probe_wake.notify_one();
    }

    pub async fn set_heartbeat_nat_timeout(&self, timeout: Option<Duration>) {
        self.with_core(|core| {
            core.set_heartbeat_nat_timeout(timeout);
        })
        .await;
    }

    pub async fn heartbeat_effective_interval(&self) -> Duration {
        self.with_core(|core| core.heartbeat_effective_interval())
            .await
    }

    /// 最近一次心跳往返时长（心跳未启动或还没有应答过的 ping 时为 `None`）。
    pub async fn heartbeat_round_trip(&self) -> Option<Duration> {
        self.with_core(|core| core.heartbeat_round_trip()).await
    }

    /// 获取协商后的消息解析器快照（连接完成后用于解析 `ConnectionEvent::Message` 原始字节）
    pub async fn parser_snapshot(&self) -> crate::common::MessageParser {
        let client = self.client.lock().await;
        client.core().parser.lock().await.clone()
    }

    /// 等待 CONNECT_ACK 协商完成（不长期占用客户端锁，便于 WASM 侧 drain 入站帧）
    pub async fn wait_for_negotiation(&self, timeout: Duration) -> Result<()> {
        let core = {
            let client = self.client.lock().await;
            client.core().clone()
        };
        core.wait_for_negotiation(timeout).await
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::client::config::ClientConfig;

    /// 只绑 127.0.0.1 的 TCP 监听，记下被接受的连接数（不说 WebSocket，握手会超时）。
    async fn counting_listener() -> (
        String,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address");
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = accepted.clone();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                held.push(stream);
            }
        });
        (format!("ws://{address}/ws"), accepted, task)
    }

    fn ping() -> Frame {
        crate::common::protocol::frame_with_system_command(
            crate::common::protocol::ping(),
            crate::common::protocol::Reliability::AtLeastOnce,
        )
    }

    /// 未连接的客户端发送立即失败（`NOT_CONNECTED`），不去连服务端：建连只有上层一条路径，
    /// 旧客户端不会在下一次发送时自己连上、与上层新建的客户端并存。主动断开过的也一样。
    #[tokio::test]
    async fn sending_never_dials_the_server() {
        let (url, accepted, server) = counting_listener().await;
        let config = ClientConfig::new(url)
            .websocket()
            .with_connect_timeout(Duration::from_millis(300));
        let wrapper = ClientWrapper::new(HybridClient::new(config).expect("hybrid client"));

        let started = std::time::Instant::now();
        let err = wrapper
            .send_frame(&ping())
            .await
            .expect_err("must not send");
        assert_eq!(
            err.code(),
            Some(crate::common::error::ErrorCode::NotConnected),
            "{err}"
        );
        assert!(started.elapsed() < Duration::from_millis(200));
        let err = wrapper
            .send_frame_and_wait(&ping(), Duration::from_millis(200))
            .await
            .expect_err("must not send");
        assert_eq!(
            err.code(),
            Some(crate::common::error::ErrorCode::NotConnected),
            "{err}"
        );

        wrapper
            .disconnect()
            .await
            .expect("disconnect without a connection");
        assert!(wrapper.send_frame(&ping()).await.is_err());

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            accepted.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the client must not dial the server on its own"
        );
        server.abort();
    }

    /// 发送等回执时客户端锁一直被占着；验活请求不能排在它后面，要立即送到心跳。
    #[tokio::test]
    async fn a_probe_request_does_not_wait_for_the_client_lock() {
        let config = ClientConfig::new("ws://127.0.0.1:9/ws".to_string()).websocket();
        let client = HybridClient::new(config).expect("hybrid client");
        let heartbeat_wake = client.core().heartbeat_probe_handle();
        let wrapper = ClientWrapper::new(client);

        let _held_by_a_pending_send = wrapper.client.lock().await;
        wrapper.probe_heartbeat();

        tokio::time::timeout(Duration::from_millis(100), heartbeat_wake.notified())
            .await
            .expect("the heartbeat is woken while the client lock is held");
    }
}
