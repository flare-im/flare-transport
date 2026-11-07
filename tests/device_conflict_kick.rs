//! 设备冲突踢线：被同平台新设备顶下的一端可靠地收到带原因的断开，会话就此结束、不再自动重连。
//!
//! 本机回环起一个真实的 flare-core WebSocket 服务端（平台互斥的设备管理器），客户端走
//! `FlareClientBuilder` + 单协议 WebSocket，保留默认的发送前重连。复现的是 W8-S1 F1：
//! 服务端发完 KICKED 紧接着关连接，同一次踢线随帧到达顺序有两种结果——KICKED 先被处理时客户端以
//! `Closed by client` 关闭、观察者收不到被踢；关连接先被处理时观察者只看到对端关闭，随后下一次
//! 发送把被踢的设备连回去，又把新设备顶下线。两种都不是「已在别处登录、会话结束」。
//!
//! 本机回环上几乎总是关连接先被处理；每轮换一个用户再跑一次。

#![cfg(all(
    feature = "client",
    feature = "server",
    feature = "websocket",
    not(target_arch = "wasm32")
))]

use async_trait::async_trait;
use flare_core::client::builder::flare::{FlareClient, FlareClientBuilder, MessageListener};
use flare_core::client::link::DisconnectReason;
use flare_core::common::config_types::TransportProtocol;
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::error::{ErrorCode, Result};
use flare_core::common::kick::DEVICE_CONFLICT_REASON;
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::flare::core::commands::system_command::Type as SystemType;
use flare_core::common::protocol::{Frame, Reliability, frame_with_system_command, ping};
use flare_core::server::{
    DeviceConflictStrategyBuilder, DeviceManager, FlareServer, FlareServerBuilder,
    ServerEventHandler,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 记下业务层收到的断开（服务端一侧的观察者）。
#[derive(Default)]
struct ServerDisconnects(Mutex<Vec<String>>);

#[async_trait]
impl ServerEventHandler for ServerDisconnects {
    async fn on_disconnect(&self, connection_id: &str, _reason: Option<&str>) -> Result<()> {
        self.0.lock().unwrap().push(connection_id.to_string());
        Ok(())
    }
}

/// `RUST_LOG=flare_core=debug` 时打印客户端与服务端日志，便于定位。
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a probe TCP socket")
        .local_addr()
        .expect("probe address")
        .port()
}

async fn start_server() -> (FlareServer, u16, Arc<ServerDisconnects>) {
    init_tracing();
    let port = free_tcp_port();
    let address = format!("127.0.0.1:{port}");
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive()
            .build(),
    ));
    let disconnects = Arc::new(ServerDisconnects::default());
    let server = FlareServerBuilder::new(address.clone(), disconnects.clone())
        .with_protocols(vec![TransportProtocol::WebSocket])
        .with_protocol_address(TransportProtocol::WebSocket, address)
        .with_device_manager(device_manager)
        .build()
        .expect("build WebSocket server");
    server.start().await.expect("start WebSocket server");
    (server, port, disconnects)
}

/// 记下 CONNECT_ACK 数与断开原因的监听器。
#[derive(Default)]
struct Recorder {
    acks: AtomicUsize,
    disconnects: Mutex<Vec<String>>,
}

impl Recorder {
    fn acks(&self) -> usize {
        self.acks.load(Ordering::SeqCst)
    }

    fn disconnects(&self) -> Vec<String> {
        self.disconnects.lock().unwrap().clone()
    }
}

#[async_trait]
impl MessageListener for Recorder {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        if let Some(CommandType::System(system)) =
            frame.command.as_ref().and_then(|c| c.r#type.as_ref())
            && system.r#type == SystemType::ConnectAck as i32
        {
            self.acks.fetch_add(1, Ordering::SeqCst);
        }
        Ok(None)
    }

    async fn on_disconnect(&self, reason: &DisconnectReason) -> Result<()> {
        self.disconnects.lock().unwrap().push(reason.to_string());
        Ok(())
    }
}

async fn connect(port: u16, user_id: &str, device_id: &str) -> (FlareClient, Arc<Recorder>) {
    let url = format!("ws://127.0.0.1:{port}");
    let recorder = Arc::new(Recorder::default());
    let client = FlareClientBuilder::new(url.clone())
        .with_protocol_url(TransportProtocol::WebSocket, url)
        .with_protocol_race(vec![TransportProtocol::WebSocket])
        .with_user_id(user_id.to_string())
        .with_listener(recorder.clone())
        .with_device_info(DeviceInfo::new(device_id.to_string(), DevicePlatform::PC))
        .with_connect_timeout(Duration::from_secs(5))
        .build_with_race()
        .await
        .expect("client connects");
    wait_until("CONNECT_ACK", || async { recorder.acks() == 1 }).await;
    (client, recorder)
}

async fn wait_until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Duration::from_secs(3);
    let started = Instant::now();
    while !check().await {
        assert!(
            started.elapsed() < deadline,
            "{what} not reached within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_counts(server: &FlareServer, connections: usize, users: usize) {
    wait_until(
        &format!("server counts back to {connections} connection(s) / {users} user(s)"),
        || async { server.connection_count() == connections && server.user_count() == users },
    )
    .await;
}

fn ping_frame() -> Frame {
    frame_with_system_command(ping(), Reliability::AtLeastOnce)
}

/// 同平台第二设备登录：旧设备的观察者收到 `DEVICE_CONFLICT`（不是 `Closed by client`），
/// 服务端一侧的业务层收到它的断开；旧设备此后不再自己重连——下一次发送立即以 DEVICE_CONFLICT
/// 报错，而不是把它连回去再把新设备顶下线。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn device_conflict_kick_ends_the_old_session_without_reconnecting() {
    let (server, port, server_disconnects) = start_server().await;

    for round in 0..8 {
        let user = format!("user-{round}");
        let (owner, owner_events) = connect(port, &user, "owner-device").await;
        wait_for_counts(&server, 1, 1).await;
        let server_disconnects_before = server_disconnects.0.lock().unwrap().len();

        let (second, second_events) = connect(port, &user, "second-device").await;
        wait_until("owner reports the kick", || async {
            owner_events
                .disconnects()
                .iter()
                .any(|reason| reason == DEVICE_CONFLICT_REASON)
        })
        .await;
        // 被踢原因恰好上报一次：前面没有 `Closed by client` 或对端关闭，后面也不再补报
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            owner_events.disconnects(),
            vec![DEVICE_CONFLICT_REASON.to_string()],
            "round {round}: the kick is the one and only disconnect"
        );
        assert!(!owner.is_connected_async().await, "round {round}");
        wait_until("server notifies the kicked connection", || async {
            server_disconnects.0.lock().unwrap().len() == server_disconnects_before + 1
        })
        .await;

        // 不再自动重连：发送立即失败并报出踢线原因，不发新的 CONNECT
        let started = Instant::now();
        let error = tokio::time::timeout(Duration::from_secs(2), owner.send_frame(&ping_frame()))
            .await
            .expect("send after the kick must fail at once")
            .expect_err("a kicked session must not reconnect on send");
        assert_eq!(
            error.code(),
            Some(ErrorCode::NotConnected),
            "round {round}: {error}"
        );
        assert!(
            error.to_string().contains(DEVICE_CONFLICT_REASON),
            "round {round}: {error}"
        );
        assert!(started.elapsed() < Duration::from_secs(1), "round {round}");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(owner_events.acks(), 1, "round {round}: no new CONNECT_ACK");
        assert!(second.is_connected_async().await, "round {round}");
        assert!(
            second_events.disconnects().is_empty(),
            "round {round}: the new device stays online: {:?}",
            second_events.disconnects()
        );
        wait_for_counts(&server, 1, 1).await;

        second.disconnect().await.expect("second device logs out");
        wait_for_counts(&server, 0, 0).await;
        drop(owner);
    }

    server.stop().await.expect("stop server");
}
