//! QUIC 客户端被服务端踢下线之后的行为，以及 CONNECT 被拒时客户端拿到的错误。
//!
//! 本机回环起一个真实的 flare-core QUIC 服务端（平台互斥的设备管理器），客户端走
//! `FlareClientBuilder` + 单协议 QUIC，与 SDK 在 `race_order=[QUIC]` 下的建连路径相同。
//!
//! 客户端不自己重连：被踢、被服务端关连接之后，发送直接报未连接，重连由上层新建客户端完成——
//! 新客户端是一条新连接，不会把 CONNECT 写进服务端已经关掉的旧流。设备冲突的踢线结束会话
//! （见第一条用例）。

#![cfg(all(
    feature = "client",
    feature = "server",
    feature = "quic",
    not(target_arch = "wasm32")
))]

use async_trait::async_trait;
use flare_core::client::builder::flare::{FlareClient, FlareClientBuilder, MessageListener};
use flare_core::client::link::DisconnectReason;
use flare_core::common::cert::{SERVER_CERT_ENV, SERVER_KEY_ENV};
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::{TlsConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::error::{ErrorCode, Result};
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::flare::core::commands::system_command::Type as SystemType;
use flare_core::common::protocol::{
    Frame, Reliability, SerializationFormat, frame_with_system_command, kicked, ping,
};
use flare_core::server::{
    AuthResult, Authenticator, DeviceConflictStrategyBuilder, DeviceManager, FlareServer,
    FlareServerBuilder, ServerEventHandler,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// 服务端身份只能在进程里注入一次（证书按进程缓存），所有用例共用这一张自签证书。
fn server_identity() -> &'static Vec<u8> {
    static CERT_DER: OnceLock<Vec<u8>> = OnceLock::new();
    CERT_DER.get_or_init(|| {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("self-signed certificate");
        // SAFETY: 在任何服务端线程读取这两个变量之前、由 OnceLock 保证只写一次。
        unsafe {
            std::env::set_var(SERVER_CERT_ENV, certified.cert.pem());
            std::env::set_var(SERVER_KEY_ENV, certified.signing_key.serialize_pem());
        }
        certified.cert.der().to_vec()
    })
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .expect("bind a probe UDP socket")
        .local_addr()
        .expect("probe address")
        .port()
}

struct NoopHandler;

#[async_trait]
impl ServerEventHandler for NoopHandler {}

/// 认证器按同一个结果拒绝全部 CONNECT。
struct RejectAll(AuthResult);

#[async_trait]
impl Authenticator for RejectAll {
    async fn authenticate(
        &self,
        _token: &str,
        _connection_id: &str,
        _device_info: Option<&DeviceInfo>,
        _metadata: Option<&HashMap<String, Vec<u8>>>,
    ) -> Result<AuthResult> {
        Ok(self.0.clone())
    }
}

/// `RUST_LOG=flare_core=debug` 时打印客户端与服务端日志，便于定位。
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

async fn start_server(authenticator: Option<Arc<dyn Authenticator>>) -> (FlareServer, u16) {
    init_tracing();
    server_identity();
    let port = free_udp_port();
    let address = format!("127.0.0.1:{port}");
    let device_manager = Arc::new(DeviceManager::new(
        DeviceConflictStrategyBuilder::new()
            .platform_exclusive()
            .build(),
    ));
    let mut builder = FlareServerBuilder::new(address.clone(), Arc::new(NoopHandler))
        .with_protocols(vec![TransportProtocol::QUIC])
        .with_protocol_address(TransportProtocol::QUIC, address)
        .with_device_manager(device_manager)
        .with_default_format(SerializationFormat::Protobuf)
        .with_default_compression(CompressionAlgorithm::Gzip);
    if let Some(authenticator) = authenticator {
        builder = builder.enable_auth().with_authenticator(authenticator);
    }
    let server = builder.build().expect("build QUIC server");
    server.start().await.expect("start QUIC server");
    (server, port)
}

/// 数 CONNECT_ACK：SDK 也是靠这一帧判定「已就绪」。
#[derive(Default)]
struct AckCounter {
    acks: AtomicUsize,
}

impl AckCounter {
    fn acks(&self) -> usize {
        self.acks.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MessageListener for AckCounter {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        if let Some(CommandType::System(system)) =
            frame.command.as_ref().and_then(|c| c.r#type.as_ref())
            && system.r#type == SystemType::ConnectAck as i32
        {
            self.acks.fetch_add(1, Ordering::SeqCst);
        }
        Ok(None)
    }
}

fn client_builder(port: u16, device_id: &str, listener: Arc<AckCounter>) -> FlareClientBuilder {
    let tls = TlsConfig {
        ca_cert_data: Some(server_identity().clone()),
        ..TlsConfig::none()
    };
    let url = format!("quic://127.0.0.1:{port}");
    FlareClientBuilder::new(url.clone())
        .with_protocol_url(TransportProtocol::QUIC, url)
        .with_protocol_race(vec![TransportProtocol::QUIC])
        .with_user_id("bob".to_string())
        .with_token("test-token".to_string())
        .with_listener(listener)
        .with_device_info(DeviceInfo::new(device_id.to_string(), DevicePlatform::PC))
        .with_format(SerializationFormat::Protobuf)
        .with_tls(tls)
        .with_connect_timeout(Duration::from_secs(5))
}

/// 与 [`client_builder`] 相同，监听器换成记录断开原因的 [`CloseRecorder`]。
fn recorder_client_builder(
    port: u16,
    device_id: &str,
    listener: Arc<CloseRecorder>,
) -> FlareClientBuilder {
    let tls = TlsConfig {
        ca_cert_data: Some(server_identity().clone()),
        ..TlsConfig::none()
    };
    let url = format!("quic://127.0.0.1:{port}");
    FlareClientBuilder::new(url.clone())
        .with_protocol_url(TransportProtocol::QUIC, url)
        .with_protocol_race(vec![TransportProtocol::QUIC])
        .with_user_id("bob".to_string())
        .with_token("test-token".to_string())
        .with_listener(listener)
        .with_device_info(DeviceInfo::new(device_id.to_string(), DevicePlatform::PC))
        .with_format(SerializationFormat::Protobuf)
        .with_tls(tls)
        .with_connect_timeout(Duration::from_secs(5))
}

/// 被踢之后上层新建客户端重新拿到 CONNECT_ACK 的时限。
const RECOVERY_BOUND: Duration = Duration::from_secs(2);

async fn wait_until<F, Fut>(what: &str, deadline: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    while !check().await {
        assert!(
            started.elapsed() < deadline,
            "{what} not reached within {deadline:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn ping_frame() -> Frame {
    frame_with_system_command(ping(), Reliability::AtLeastOnce)
}

/// 被同平台第二设备踢掉（设备冲突）：QUIC 下同样只报一次 `DEVICE_CONFLICT`，会话就此结束——
/// 第二设备登出之后，主客户端的下一次发送立即以这个原因失败，不发新的 CONNECT，不把自己连回去。
///
/// 以前这里断言的是「下一次发送在有界时间内重新 CONNECT」（W4-D）；按设备冲突的产品语义
/// （被顶下的一端终止会话、由应用重新登录），被踢的客户端不再自动重连。被踢后新建连接不复用
/// 旧流的约束由下一条用例（非终止原因的踢线）继续守。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn device_conflict_kick_ends_the_quic_session() {
    use flare_core::common::kick::DEVICE_CONFLICT_REASON;

    let (server, port) = start_server(None).await;

    let recorder = Arc::new(CloseRecorder::default());
    let owner: FlareClient = recorder_client_builder(port, "owner-device", recorder.clone())
        .build_with_race()
        .await
        .expect("owner connects");
    wait_until("owner CONNECT_ACK", Duration::from_secs(5), || async {
        recorder.acks.load(Ordering::SeqCst) == 1
    })
    .await;

    // 同一用户、同一平台的第二设备登录：服务端按平台互斥把主客户端踢掉。
    let second = client_builder(port, "second-device", Arc::new(AckCounter::default()))
        .build_with_race()
        .await
        .expect("second device connects");
    wait_until("owner reports the kick", Duration::from_secs(5), || async {
        !recorder.disconnects.lock().unwrap().is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        *recorder.disconnects.lock().unwrap(),
        vec![DEVICE_CONFLICT_REASON.to_string()],
        "the kick is the one and only disconnect"
    );
    assert!(!owner.is_connected_async().await);
    second.disconnect().await.expect("second device logs out");

    let error = tokio::time::timeout(Duration::from_secs(2), owner.send_frame(&ping_frame()))
        .await
        .expect("send after the kick must fail at once")
        .expect_err("a kicked session must not reconnect on send");
    assert_eq!(error.code(), Some(ErrorCode::NotConnected), "{error}");
    assert!(
        error.to_string().contains(DEVICE_CONFLICT_REASON),
        "{error}"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        recorder.acks.load(Ordering::SeqCst),
        1,
        "no new CONNECT_ACK"
    );
    assert_eq!(
        server.connection_count(),
        0,
        "the kicked client stays offline"
    );

    server.stop().await.expect("stop server");
}

/// 服务端踢线（与网关一致：先发 KICKED 再关连接）之后，客户端立即以类型化原因感知断开，发送直接报
/// 未连接（不自己重连）；上层新建的客户端在有界时间内重新拿到 CONNECT_ACK。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_disconnect_is_seen_at_once_and_a_new_client_reconnects() {
    let (server, port) = start_server(None).await;

    let recorder = Arc::new(CloseRecorder::default());
    let owner = recorder_client_builder(port, "owner-device", recorder.clone())
        .build_with_race()
        .await
        .expect("owner connects");
    wait_until("owner CONNECT_ACK", Duration::from_secs(5), || async {
        recorder.acks.load(Ordering::SeqCst) == 1
    })
    .await;

    let connection_id = server
        .get_server_handle_components()
        .expect("connection manager")
        .list_connections()
        .await
        .into_iter()
        .next()
        .expect("owner connection id");
    // 与网关踢线一致：先发带原因的 KICKED，再关连接。
    let kicked = frame_with_system_command(
        kicked("DEVICE_KICKED".to_string(), None),
        Reliability::AtLeastOnce,
    );
    server
        .send_to(&connection_id, &kicked)
        .await
        .expect("server sends KICKED");
    let kicked_at = Instant::now();
    server
        .disconnect(&connection_id)
        .await
        .expect("server kicks");
    wait_until(
        "owner notices the close",
        Duration::from_secs(2),
        || async { !owner.is_connected_async().await },
    )
    .await;
    assert!(kicked_at.elapsed() < Duration::from_secs(2));
    wait_until("owner reports the kick", Duration::from_secs(2), || async {
        !recorder.typed.lock().unwrap().is_empty()
    })
    .await;
    // 服务端的 KICKED 走写队列，紧接着移除连接会丢掉还在队列里的帧：客户端看到的可能是被踢，也可能
    // 只是对端关闭。两种都不结束会话。
    let first = recorder.typed.lock().unwrap()[0].clone();
    assert!(
        matches!(
            &first,
            DisconnectReason::Kicked {
                reason,
                ends_session: false,
            } if reason == "DEVICE_KICKED"
        ) || matches!(first, DisconnectReason::PeerClosed { .. }),
        "{first:?}"
    );

    let error = owner
        .send_frame(&ping_frame())
        .await
        .expect_err("the kicked client does not reconnect on its own");
    assert_eq!(error.code(), Some(ErrorCode::NotConnected), "{error}");

    let started = Instant::now();
    let renewed_acks = Arc::new(AckCounter::default());
    let renewed = client_builder(port, "owner-device", renewed_acks.clone())
        .build_with_race()
        .await
        .expect("a new client connects");
    let recovered_in = started.elapsed();
    // build_with_race 返回即已协商完成；CONNECT_ACK 交给监听器是异步的，稍等一下
    wait_until("renewed CONNECT_ACK", Duration::from_secs(2), || async {
        renewed_acks.acks() == 1
    })
    .await;
    assert!(
        recovered_in < RECOVERY_BOUND,
        "re-CONNECT took {recovered_in:?}"
    );
    renewed
        .send_frame(&ping_frame())
        .await
        .expect("the new connection is usable");

    renewed.disconnect().await.expect("renewed disconnects");
    server.stop().await.expect("stop server");
}

/// CONNECT 被拒时，客户端拿到的错误按服务端下发的类型化原因分类：
/// 租户不可用不是鉴权失败，鉴权失败才是鉴权失败；认证依赖暂时读不到（如租户状态库抖动）
/// 是可重试的 SERVICE_UNAVAILABLE，既不是鉴权失败也不是租户不可用。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_connect_surfaces_the_typed_reason() {
    for (auth_result, expected_code, expected_text) in [
        (
            AuthResult::failure("TENANT_UNAVAILABLE".to_string()),
            ErrorCode::PermissionDenied,
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
    ] {
        let auth_reason = auth_result.error_message.clone().unwrap_or_default();
        let retryable = auth_result.retryable;
        let (server, port) = start_server(Some(Arc::new(RejectAll(auth_result)))).await;
        let started = Instant::now();
        let failure = match client_builder(port, "owner-device", Arc::new(AckCounter::default()))
            .build_with_race()
            .await
        {
            Ok(_) => panic!("CONNECT must be rejected ({auth_reason})"),
            Err(failure) => failure,
        };
        let rejection = failure.rejection().expect("a typed rejection").clone();
        assert_eq!(rejection.code, expected_code, "{failure}");
        let error = flare_core::common::error::FlareError::from(failure);
        assert_eq!(error.code(), Some(expected_code), "{error}");
        assert!(error.to_string().contains(expected_text), "{error}");
        assert_eq!(expected_code.is_retryable(), retryable, "{error}");
        if retryable {
            // 服务端内部文本不下发；也不能带上会让客户端终止会话的租户原因
            assert!(!error.to_string().contains(&auth_reason), "{error}");
            assert!(!error.to_string().contains("TENANT_UNAVAILABLE"), "{error}");
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "rejection must not wait for the negotiation timeout"
        );
        server.stop().await.expect("stop server");
    }
}

/// 记下断开原因（文本与类型化）与传输错误的监听器。
#[derive(Default)]
struct CloseRecorder {
    acks: AtomicUsize,
    disconnects: std::sync::Mutex<Vec<String>>,
    typed: std::sync::Mutex<Vec<DisconnectReason>>,
    errors: std::sync::Mutex<Vec<String>>,
}

#[async_trait]
impl MessageListener for CloseRecorder {
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
        if let DisconnectReason::TransportError { detail } = reason {
            self.errors.lock().unwrap().push(detail.clone());
        }
        self.disconnects.lock().unwrap().push(reason.to_string());
        self.typed.lock().unwrap().push(reason.clone());
        Ok(())
    }
}

/// QUIC 服务端停机：客户端收到 CLOSE(SERVER_SHUTDOWN)，以这个原因断开（不是报错），服务端不必
/// 等满宽限期。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_stop_tells_quic_clients_why() {
    use flare_core::common::shutdown::{SERVER_SHUTDOWN_CLOSE_GRACE, is_server_shutdown_reason};

    let (server, port) = start_server(None).await;
    let recorder = Arc::new(CloseRecorder::default());
    let tls = TlsConfig {
        ca_cert_data: Some(server_identity().clone()),
        ..TlsConfig::none()
    };
    let url = format!("quic://127.0.0.1:{port}");
    let client = FlareClientBuilder::new(url.clone())
        .with_protocol_url(TransportProtocol::QUIC, url)
        .with_protocol_race(vec![TransportProtocol::QUIC])
        .with_user_id("bob".to_string())
        .with_token("test-token".to_string())
        .with_listener(recorder.clone())
        .with_device_info(DeviceInfo::new(
            "owner-device".to_string(),
            DevicePlatform::PC,
        ))
        .with_format(SerializationFormat::Protobuf)
        .with_tls(tls)
        .with_connect_timeout(Duration::from_secs(5))
        .build_with_race()
        .await
        .expect("client connects");
    wait_until("CONNECT_ACK", Duration::from_secs(5), || async {
        recorder.acks.load(Ordering::SeqCst) == 1
    })
    .await;

    let started = Instant::now();
    server.stop().await.expect("stop server");
    let stop_took = started.elapsed();

    wait_until("client sees the close", Duration::from_secs(3), || async {
        !recorder.disconnects.lock().unwrap().is_empty()
    })
    .await;
    let reasons = recorder.disconnects.lock().unwrap().clone();
    assert!(
        reasons
            .iter()
            .all(|reason| is_server_shutdown_reason(reason)),
        "disconnect reasons must carry SERVER_SHUTDOWN: {reasons:?}"
    );
    assert!(
        recorder.errors.lock().unwrap().is_empty(),
        "a planned shutdown is not a connection error: {:?}",
        recorder.errors.lock().unwrap()
    );
    assert!(
        stop_took < SERVER_SHUTDOWN_CLOSE_GRACE,
        "the client closed by itself: {stop_took:?}"
    );
    // 类型化原因里带着服务端给的重连窗口
    let typed = recorder.typed.lock().unwrap().clone();
    let Some(DisconnectReason::ServerClose(close)) = typed.first() else {
        panic!("a typed planned close: {typed:?}");
    };
    assert_eq!(
        close.retry_window(),
        Some((
            flare_core::common::shutdown::SERVER_SHUTDOWN_RECONNECT_MIN,
            flare_core::common::shutdown::SERVER_SHUTDOWN_RECONNECT_MAX
        ))
    );
    assert!(!client.is_connected_async().await);
}
