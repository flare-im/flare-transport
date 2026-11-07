//! 混合客户端：按协议优先序交错竞速建连（见 [`crate::client::race`]），单协议就是只有一个协议的竞速。
//!
//! 统一管理连接状态、心跳、消息路由等功能。

use crate::client::config::ClientConfig;
use crate::client::link::{AttemptFailure, ConnectFailure, ConnectReport};
use crate::client::race::staggered_race;
use crate::client::transports::attempt::TransportAttempt;
use crate::client::transports::client_core::LinkBinding;
use crate::client::transports::{Client, ClientCore};
use crate::common::config_types::TransportProtocol;
use crate::common::error::{FlareError, Result};
use crate::common::platform::timeout as platform_timeout;
use crate::common::protocol::Frame;
use crate::transport::events::{ArcObserver, observer_snapshot};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[cfg(feature = "quic")]
use crate::client::transports::quic::QUICClient;
#[cfg(feature = "tcp")]
use crate::client::transports::tcp::TCPClient;
#[cfg(feature = "websocket")]
use crate::client::transports::websocket::WebSocketClient;

/// 混合客户端
///
/// 建连走交错竞速，胜出的那条连接留下；连接断开后不自己重连，由上层新建客户端再连。
pub struct HybridClient {
    /// 胜出的协议客户端（建连之前是首选协议的占位客户端）
    inner: Arc<Mutex<Box<dyn Client>>>,
    /// 最近一次**成功读到**的连接状态。
    ///
    /// `is_connected` 是同步 trait 方法，撞上 `inner` 锁时没有阻塞以外的选择——
    /// 而阻塞在这里两头不讨好（见 `is_connected` 处的说明）。缓存让它可以无锁降级。
    last_known_connected: Arc<std::sync::atomic::AtomicBool>,
    /// 使用的协议
    active_protocol: TransportProtocol,
    /// 胜出连接的 core（建连之前是占位 core：上层在建连前挂的观察者、事件处理器、心跳策略都在它上面）
    core: ClientCore,
    /// 建连用的配置
    config: ClientConfig,
    /// 最近一次建连的结果
    report: Option<ConnectReport>,
}

impl HybridClient {
    /// 创建未连接的客户端；[`Client::connect`] 时按配置交错竞速。
    ///
    /// 建连前挂上的观察者在收到 CONNECT_ACK 时挂到胜出的连接上，落败连接的事件到不了它们。
    pub fn new(config: ClientConfig) -> Result<Self> {
        let core = ClientCore::new(&config);
        let protocol = config.get_protocols()[0];
        let client = Self::protocol_client(&config, protocol, core.clone())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            last_known_connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            active_protocol: protocol,
            core,
            config,
            report: None,
        })
    }

    /// 交错竞速建连：返回时已经收到 CONNECT_ACK。结果见 [`Self::connect_report`]。
    pub async fn connect_with_race(
        config: ClientConfig,
    ) -> std::result::Result<Self, ConnectFailure> {
        Self::connect_bound(config, LinkBinding::default()).await
    }

    /// 交错竞速建连，收到 CONNECT_ACK 时把 `binding` 挂到胜出的连接上。
    pub(crate) async fn connect_bound(
        config: ClientConfig,
        binding: LinkBinding,
    ) -> std::result::Result<Self, ConnectFailure> {
        let base = ClientCore::new(&config);
        let (client, core, report) = Self::race(&config, &base, binding).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
            last_known_connected: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            active_protocol: report.protocol,
            core,
            config,
            report: Some(report),
        })
    }

    /// 跑一次交错竞速：每个协议一个独立的客户端与 core（心跳策略、验活唤醒与事件处理器取自
    /// `base`），胜出的留下。
    async fn race(
        config: &ClientConfig,
        base: &ClientCore,
        binding: LinkBinding,
    ) -> std::result::Result<(Box<dyn Client>, ClientCore, ConnectReport), ConnectFailure> {
        let params = config.race_params();
        let (attempt, report) = staggered_race(&params, |protocol| {
            let protocol_config = ClientConfig {
                server_url: config.get_protocol_url(&protocol),
                transport: protocol,
                transports: None,
                ..config.clone()
            };
            let core = ClientCore::for_attempt(&protocol_config, base);
            let client = Self::protocol_client(&protocol_config, protocol, core.clone())
                .map_err(|error| AttemptFailure::from_establish_error(&error))?;
            Ok(TransportAttempt::new(client, core, binding.clone()))
        })
        .await?;
        let (client, core) = attempt.into_parts();
        Ok((client, core, report))
    }

    /// 为一个协议创建客户端（不建连）
    fn protocol_client(
        config: &ClientConfig,
        protocol: TransportProtocol,
        core: ClientCore,
    ) -> Result<Box<dyn Client>> {
        let config = ClientConfig {
            server_url: config.get_protocol_url(&protocol),
            transport: protocol,
            transports: None,
            ..config.clone()
        };
        match protocol {
            TransportProtocol::WebSocket => {
                #[cfg(feature = "websocket")]
                {
                    Ok(Box::new(WebSocketClient::with_core(config, core)))
                }
                #[cfg(not(feature = "websocket"))]
                {
                    let _ = (config, core);
                    Err(Self::transport_feature_disabled(protocol))
                }
            }
            TransportProtocol::QUIC => {
                #[cfg(feature = "quic")]
                {
                    QUICClient::with_core(config, core).map(|c| Box::new(c) as Box<dyn Client>)
                }
                #[cfg(not(feature = "quic"))]
                {
                    let _ = (config, core);
                    Err(Self::transport_feature_disabled(protocol))
                }
            }
            TransportProtocol::TCP => {
                #[cfg(feature = "tcp")]
                {
                    Ok(Box::new(TCPClient::with_core(config, core)))
                }
                #[cfg(not(feature = "tcp"))]
                {
                    let _ = (config, core);
                    Err(Self::transport_feature_disabled(protocol))
                }
            }
        }
    }

    #[allow(dead_code)]
    fn transport_feature_disabled(protocol: TransportProtocol) -> FlareError {
        FlareError::operation_not_supported(format!("{protocol:?} transport feature is disabled"))
    }

    /// 获取当前使用的协议
    pub fn active_protocol(&self) -> TransportProtocol {
        self.active_protocol
    }

    /// 最近一次建连的结果（胜出协议、握手耗时、CONNECT_ACK 信息、失败的协议）；还没连过时为 `None`。
    pub fn connect_report(&self) -> Option<&ConnectReport> {
        self.report.as_ref()
    }

    /// 获取 ClientCore（用于外部访问）
    pub fn core(&self) -> &ClientCore {
        &self.core
    }

    /// 获取 ClientCore 的可变引用（用于外部修改）
    pub fn core_mut(&mut self) -> &mut ClientCore {
        &mut self.core
    }
}

#[async_trait]
impl Client for HybridClient {
    /// 交错竞速建连；已经连着时什么也不做。失败时错误里是服务端拒绝的类别，或各协议的失败汇总。
    async fn connect(&mut self) -> Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        let binding = LinkBinding {
            observers: observer_snapshot(&self.core.observers),
            reason_slot: None,
        };
        let (client, core, report) = Self::race(&self.config, &self.core, binding).await?;
        *self.inner.lock().await = client;
        self.core = core;
        self.active_protocol = report.protocol;
        self.report = Some(report);
        self.last_known_connected
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        let mut client = self.inner.lock().await;
        client.disconnect().await
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        let mut client = self.inner.lock().await;
        client.send_frame(frame).await
    }

    fn is_connected(&self) -> bool {
        use std::sync::atomic::Ordering;
        match self.inner.try_lock() {
            Ok(client) => {
                let connected = client.is_connected();
                self.last_known_connected
                    .store(connected, Ordering::Relaxed);
                connected
            }
            // 撞锁不阻塞，退回上一次观察到的状态。
            //
            // 这里原来是 `block_in_place` + `blocking_lock`，注释说它"避免阻塞 Tokio
            // 运行时"——恰恰相反：`block_in_place` 要把本 worker 的整个任务队列交接给
            // 另一个线程，而 `blocking_lock` 随后把这个线程真的挂住，只为读一个布尔；
            // 在 current-thread runtime 上它更是直接 panic。
            //
            // 而"拿不到锁"本身就意味着底层正在收发，缓存值是那一刻的真实状态；
            // 每次成功读取都会刷新它，所以最多只会陈旧一个临界区的时间。
            Err(_) => self.last_known_connected.load(Ordering::Relaxed),
        }
    }

    fn add_observer(&mut self, observer: ArcObserver) {
        // 通过 ClientCore 添加观察者
        self.core.add_observer(observer);
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        self.core.remove_observer(observer);
    }

    fn connection_id(&self) -> Option<String> {
        if let Ok(client) = self.inner.try_lock() {
            client.connection_id()
        } else {
            None
        }
    }
}

impl std::fmt::Debug for HybridClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridClient")
            .field("active_protocol", &self.active_protocol)
            .finish_non_exhaustive()
    }
}

impl HybridClient {
    /// 发送消息并等待服务端响应（按 Frame.message_id 匹配）
    pub async fn send_frame_and_wait(&mut self, frame: &Frame, timeout: Duration) -> Result<Frame> {
        if frame.message_id.is_empty() {
            return Err(FlareError::protocol_error(
                "message_id is empty".to_string(),
            ));
        }

        // 在发送前注册等待器
        let rx = self.core.register_pending_response(&frame.message_id).await;

        // 发送消息
        {
            let mut client = self.inner.lock().await;
            client.send_frame(frame).await?;
        }

        // 等待响应或超时
        match platform_timeout(timeout, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => {
                // 发送失败（通道已关闭），清理等待项
                self.core.cancel_pending_response(&frame.message_id).await;
                Err(FlareError::protocol_error(
                    "Response channel closed".to_string(),
                ))
            }
            Err(_) => {
                // 超时，清理等待项
                tracing::warn!(
                    "[HybridClient] send_frame_and_wait: 响应超时, message_id={}",
                    frame.message_id
                );
                self.core.cancel_pending_response(&frame.message_id).await;
                Err(FlareError::protocol_error(format!(
                    "Response timeout for message_id {}",
                    frame.message_id
                )))
            }
        }
    }
}

#[cfg(all(
    test,
    not(target_arch = "wasm32"),
    feature = "websocket",
    feature = "quic"
))]
mod hybrid_race_tests {
    use super::*;
    use crate::client::link::AttemptFailure;

    /// 用真实传输跑一次竞速：WebSocket 指向一个已关闭的本地端口（立即被拒，记为不可达），随即
    /// 发起 QUIC，指向没人监听的 UDP 端口（握手一直没有回应，记为传输建立超时）。两个协议的失败
    /// 类别都带回，按失败先后排列。
    #[tokio::test]
    async fn real_transports_report_their_failure_classes() {
        let closed_tcp = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            listener.local_addr().expect("addr").port()
        };
        let silent_udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind udp");
        let silent_port = silent_udp.local_addr().expect("addr").port();

        let config = ClientConfig::new(format!("ws://127.0.0.1:{closed_tcp}/ws"))
            .with_protocol_race(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
            .with_protocol_url(
                TransportProtocol::QUIC,
                format!("quic://127.0.0.1:{silent_port}"),
            )
            .with_connect_timeout(Duration::from_millis(400))
            .with_race_stagger(Duration::from_millis(50));

        let started = std::time::Instant::now();
        let failure = HybridClient::connect_with_race(config)
            .await
            .expect_err("nothing is listening");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(failure.attempts.len(), 2, "{failure}");
        assert_eq!(failure.attempts[0].protocol, TransportProtocol::WebSocket);
        assert!(
            matches!(
                failure.attempts[0].failure,
                AttemptFailure::Unreachable { .. }
            ),
            "{failure}"
        );
        assert_eq!(failure.attempts[1].protocol, TransportProtocol::QUIC);
        assert!(
            matches!(
                failure.attempts[1].failure,
                AttemptFailure::Timeout | AttemptFailure::Unreachable { .. }
            ),
            "{failure}"
        );
        assert!(failure.attempts[0].failure.counts_against_access_point());
        drop(silent_udp);
    }
}
