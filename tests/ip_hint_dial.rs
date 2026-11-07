//! 原生端 `ipHints` 直连：主机名解析不了时连上给定的 IP，TLS 仍按主机名。
//!
//! 主机名用保留的 `.invalid`，一定解析不了；服务端都只绑 127.0.0.1。证书签给主机名，所以 TLS 通过就
//! 说明校验按主机名，钉扎不符或证书签给别的主机时必须失败。

#![cfg(all(
    feature = "client",
    feature = "server",
    feature = "websocket",
    feature = "quic",
    not(target_arch = "wasm32")
))]

use async_trait::async_trait;
use flare_core::client::builder::flare::{FlareClientBuilder, MessageListener};
use flare_core::client::dial::{DohAnswer, DohCache, DohLookup};
use flare_core::client::link::AttemptFailure;
use flare_core::client::{Client, ClientConfig, QUICClient, WebSocketClient};
use flare_core::common::cert::spki_sha256_pin;
use flare_core::common::config_types::{TlsConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::error::FlareError;
use flare_core::server::{FlareServerBuilder, ServerEventHandler};
use quinn::crypto::rustls::{HandshakeData, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite;

const HOST: &str = "ap-1.flare.invalid";
const OTHER_HOST: &str = "ap-2.flare.invalid";
const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
/// TEST-NET-1：不会有人应答。
const UNROUTABLE: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
/// 透明代理（TUN）接不到的黑洞：macOS 的 lo0 只配了 127.0.0.1，发到这里的包被吞掉。
const SILENT_LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
/// 同样没人应答，但是私有地址：解析到回环时不算结果可疑。
const UNROUTABLE_PRIVATE: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 255, 255, 1));
const WRONG_PIN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const LIMIT: Duration = Duration::from_secs(5);

struct Identity {
    cert: Vec<u8>,
    key: Vec<u8>,
}

fn identity(host: &str) -> Identity {
    let certified = rcgen::generate_simple_self_signed(vec![host.to_string()])
        .expect("self-signed certificate");
    Identity {
        cert: certified.cert.der().to_vec(),
        key: certified.signing_key.serialize_der(),
    }
}

fn server_tls(identity: &Identity) -> rustls::ServerConfig {
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(identity.cert.clone())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key.clone())),
        )
        .expect("server identity")
}

/// 只信这张证书（私有 CA 的做法）。
fn trusting(identity: &Identity) -> TlsConfig {
    TlsConfig {
        ca_cert_data: Some(identity.cert.clone()),
        ..TlsConfig::none()
    }
}

fn right_pin(identity: &Identity) -> String {
    spki_sha256_pin(&identity.cert).expect("SPKI pin")
}

fn failure(error: &FlareError) -> AttemptFailure {
    AttemptFailure::from_establish_error(error)
}

fn is_tls(error: &FlareError) -> bool {
    matches!(failure(error), AttemptFailure::Tls { .. })
}

/// 一次性的 wss 服务端（阻塞线程，直接用 rustls）：返回端口与它看到的 SNI。
fn wss_server(identity: &Identity) -> (u16, thread::JoinHandle<Option<String>>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind wss listener");
    let port = listener.local_addr().expect("wss address").port();
    let config = Arc::new(server_tls(identity));
    let server = thread::spawn(move || {
        let (tcp, _) = listener.accept().ok()?;
        let connection = rustls::ServerConnection::new(config).ok()?;
        let mut tls = rustls::StreamOwned::new(connection, tcp);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).ok()?;
        }
        let server_name = tls.conn.server_name().map(str::to_owned);
        tungstenite::accept(tls).ok()?;
        server_name
    });
    (port, server)
}

fn wss_client(port: u16, tls: TlsConfig) -> WebSocketClient {
    WebSocketClient::new(
        ClientConfig::new(format!("wss://{HOST}:{port}/ws"))
            .websocket()
            .with_tls(tls)
            .with_connect_timeout(LIMIT)
            .with_ip_hints(vec![LOOPBACK]),
    )
}

/// 等 [`wss_server`] 结束。客户端没拨到它时 `accept` 不会返回，这里拨一下放它出来：透明代理会把发往
/// 公网地址的连接接走，TLS 在代理那边失败。
fn wss_outcome(port: u16, server: thread::JoinHandle<Option<String>>) -> Option<String> {
    let _ = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port));
    server.join().unwrap()
}

/// TCP 上没人应答的地址。透明代理（TUN）的机器上 TEST-NET 也连得上，只能换到回环段。
async fn silent_tcp_address() -> IpAddr {
    let probe = tokio::time::timeout(
        Duration::from_millis(300),
        TcpStream::connect((UNROUTABLE, 80)),
    )
    .await;
    if matches!(probe, Ok(Ok(_))) {
        SILENT_LOOPBACK
    } else {
        UNROUTABLE
    }
}

/// 一次性的 ws 服务端（明文）。
async fn ws_server() -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ws listener");
    let port = listener.local_addr().expect("ws address").port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let _socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("upgrade");
    });
    (port, server)
}

/// QUIC 服务端接受的一条连接。留着两个句柄，测试结束前连接不被关掉。
struct QuicAccepted {
    server_name: Option<String>,
    _connection: quinn::Connection,
    _endpoint: quinn::Endpoint,
}

fn quic_server(identity: &Identity) -> (u16, JoinHandle<Option<QuicAccepted>>) {
    let crypto = QuicServerConfig::try_from(server_tls(identity)).expect("QUIC crypto");
    let config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().expect("address"))
        .expect("bind QUIC endpoint");
    let port = endpoint.local_addr().expect("QUIC address").port();
    let server = tokio::spawn(async move {
        let connection = endpoint.accept().await?.await.ok()?;
        let handshake = connection
            .handshake_data()?
            .downcast::<HandshakeData>()
            .ok()?;
        Some(QuicAccepted {
            server_name: handshake.server_name,
            _connection: connection,
            _endpoint: endpoint,
        })
    });
    (port, server)
}

fn quic_client(port: u16, tls: TlsConfig, ip_hints: Vec<IpAddr>, limit: Duration) -> QUICClient {
    QUICClient::new(
        ClientConfig::new(format!("quic://{HOST}:{port}"))
            .quic()
            .with_tls(tls)
            .with_connect_timeout(limit)
            .with_ip_hints(ip_hints),
    )
    .expect("QUIC endpoint")
}

/// DoH 查询的替身：什么主机都给这组地址。
struct FixedDoh(Vec<IpAddr>);

#[async_trait]
impl DohLookup for FixedDoh {
    fn covers(&self, _host: &str) -> bool {
        true
    }

    async fn lookup(&self, _host: &str) -> Option<DohAnswer> {
        Some(DohAnswer {
            addresses: self.0.clone(),
            ttl: Duration::from_secs(60),
        })
    }
}

/// 已经有这个主机答复的缓存。
async fn answered_doh(host: &str, addresses: Vec<IpAddr>) -> Arc<DohCache> {
    let doh = DohCache::new(Arc::new(FixedDoh(addresses)));
    doh.request(host);
    for _ in 0..100 {
        if !doh.addresses(host).is_empty() {
            return doh;
        }
        tokio::task::yield_now().await;
    }
    panic!("the lookup never finished");
}

#[tokio::test]
async fn wss_over_a_hint_keeps_sni_and_the_certificate_on_the_host() {
    let identity = identity(HOST);
    let (port, server) = wss_server(&identity);
    let mut client = wss_client(port, trusting(&identity));

    client
        .establish()
        .await
        .expect("TLS is verified against the host, not the dialed IP");

    let _ = client.disconnect().await;
    assert_eq!(server.join().unwrap().as_deref(), Some(HOST));
}

#[tokio::test]
async fn wss_over_a_hint_still_enforces_the_pin() {
    let identity = identity(HOST);

    let (port, server) = wss_server(&identity);
    let mut client = wss_client(
        port,
        trusting(&identity).with_spki_sha256_pin(right_pin(&identity)),
    );
    client.establish().await.expect("the matching pin passes");
    let _ = client.disconnect().await;
    server.join().unwrap();

    let (port, server) = wss_server(&identity);
    let mut client = wss_client(port, trusting(&identity).with_spki_sha256_pin(WRONG_PIN));
    let error = client.establish().await.unwrap_err();
    assert!(is_tls(&error), "{error}");
    assert_eq!(server.join().unwrap(), None);
}

#[tokio::test]
async fn wss_over_a_hint_rejects_a_certificate_for_another_host() {
    let identity = identity(OTHER_HOST);
    let (port, server) = wss_server(&identity);
    let mut client = wss_client(port, trusting(&identity));

    let error = client.establish().await.unwrap_err();

    assert!(is_tls(&error), "{error}");
    assert_eq!(server.join().unwrap(), None);
}

#[tokio::test]
async fn quic_over_a_hint_keeps_sni_and_the_certificate_on_the_host() {
    let identity = identity(HOST);
    let (port, server) = quic_server(&identity);
    let mut client = quic_client(port, trusting(&identity), vec![LOOPBACK], LIMIT);

    client
        .establish()
        .await
        .expect("TLS is verified against the host, not the dialed IP");

    let accepted = server.await.unwrap().expect("accepted");
    assert_eq!(accepted.server_name.as_deref(), Some(HOST));
    let _ = client.disconnect().await;
}

#[tokio::test]
async fn quic_over_a_hint_still_enforces_the_pin() {
    let identity = identity(HOST);

    let (port, server) = quic_server(&identity);
    let mut client = quic_client(
        port,
        trusting(&identity).with_spki_sha256_pin(right_pin(&identity)),
        vec![LOOPBACK],
        LIMIT,
    );
    client.establish().await.expect("the matching pin passes");
    let accepted = server.await.unwrap();
    assert!(accepted.is_some());
    let _ = client.disconnect().await;

    let (port, server) = quic_server(&identity);
    let mut client = quic_client(
        port,
        trusting(&identity).with_spki_sha256_pin(WRONG_PIN),
        vec![LOOPBACK],
        LIMIT,
    );
    let error = client.establish().await.unwrap_err();
    assert!(is_tls(&error), "{error}");
    server.abort();
}

#[tokio::test]
async fn quic_over_a_hint_rejects_a_certificate_for_another_host() {
    let identity = identity(OTHER_HOST);
    let (port, server) = quic_server(&identity);
    let mut client = quic_client(port, trusting(&identity), vec![LOOPBACK], LIMIT);

    let error = client.establish().await.unwrap_err();

    assert!(is_tls(&error), "{error}");
    server.abort();
}

#[tokio::test]
async fn quic_moves_on_from_a_dead_hint() {
    let identity = identity(HOST);
    let (port, server) = quic_server(&identity);
    let mut client = quic_client(
        port,
        trusting(&identity),
        vec![UNROUTABLE, LOOPBACK],
        Duration::from_secs(2),
    );

    client
        .establish()
        .await
        .expect("the dead hint only takes its share of the budget");

    let accepted = server.await.unwrap();
    assert!(accepted.is_some());
    let _ = client.disconnect().await;
}

#[tokio::test]
async fn websocket_moves_on_from_a_dead_hint() {
    let (port, server) = ws_server().await;
    let mut client = WebSocketClient::new(
        ClientConfig::new(format!("ws://{HOST}:{port}/ws"))
            .websocket()
            .with_connect_timeout(Duration::from_secs(2))
            .with_ip_hints(vec![silent_tcp_address().await, LOOPBACK]),
    );

    client
        .establish()
        .await
        .expect("the dead hint only takes its share of the budget");

    let _ = client.disconnect().await;
    server.await.unwrap();
}

/// `localhost` 解析得到：拨的是解析结果。提示没人应答，走了提示就连不上。
/// DoH 地址没人应答，往下拨到解析结果，TLS 又失败了：这份答复没帮上忙，丢掉，下次回到原来的顺序。
/// DoH 只用公网地址，所以这里的服务端只能靠解析结果拨到。
#[tokio::test]
async fn a_wss_tls_failure_drops_the_doh_answer_it_dialed() {
    let identity = identity(OTHER_HOST);
    let (port, server) = wss_server(&identity);
    let doh = answered_doh("localhost", vec![UNROUTABLE]).await;
    let mut client = WebSocketClient::new(
        ClientConfig::new(format!("wss://localhost:{port}/ws"))
            .websocket()
            .with_tls(trusting(&identity))
            .with_connect_timeout(LIMIT)
            .with_doh(doh.clone()),
    );

    let error = client.establish().await.unwrap_err();

    assert!(is_tls(&error), "{error}");
    assert_eq!(wss_outcome(port, server), None);
    assert!(doh.addresses("localhost").is_empty());
}

#[tokio::test]
async fn a_quic_tls_failure_drops_the_doh_answer_it_dialed() {
    let identity = identity(OTHER_HOST);
    let (port, server) = quic_server(&identity);
    let doh = answered_doh("localhost", vec![UNROUTABLE]).await;
    let mut client = QUICClient::new(
        ClientConfig::new(format!("quic://localhost:{port}"))
            .quic()
            .with_tls(trusting(&identity))
            .with_connect_timeout(LIMIT)
            .with_doh(doh.clone()),
    )
    .expect("QUIC endpoint");

    let error = client.establish().await.unwrap_err();

    assert!(is_tls(&error), "{error}");
    assert!(doh.addresses("localhost").is_empty());
    server.abort();
}

/// 解析到回环本身就算结果可疑，`resolve` 会先要求查一次。这个替身拒掉那一次，只认拨号之后的要求。
#[derive(Default)]
struct AfterResolve {
    asked: AtomicUsize,
}

#[async_trait]
impl DohLookup for AfterResolve {
    fn covers(&self, _host: &str) -> bool {
        self.asked.fetch_add(1, Ordering::SeqCst) > 0
    }

    async fn lookup(&self, _host: &str) -> Option<DohAnswer> {
        FixedDoh(vec![UNROUTABLE]).lookup("").await
    }
}

/// 等后台查询写进缓存。
async fn doh_answer_for(doh: &DohCache, host: &str) -> Vec<IpAddr> {
    for _ in 0..100 {
        let addresses = doh.addresses(host);
        if !addresses.is_empty() {
            return addresses;
        }
        tokio::task::yield_now().await;
    }
    Vec::new()
}

/// TCP 连上了，服务端一声不吭：拨号卡到时限。外层时限和竞速一样，一到就取消，传输层看不到超时。
#[tokio::test]
async fn a_stalled_websocket_dial_asks_doh_before_the_race_gives_up() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("address").port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        std::future::pending::<()>().await;
        drop(stream);
    });
    let limit = Duration::from_millis(400);
    let doh = DohCache::new(Arc::new(AfterResolve::default()));
    let mut client = WebSocketClient::new(
        ClientConfig::new(format!("ws://localhost:{port}/ws"))
            .websocket()
            .with_connect_timeout(limit)
            .with_doh(doh.clone()),
    );

    // 两个时限落在同一刻时谁先报不一定，这里只要拨号没成。
    let raced = tokio::time::timeout(limit, client.establish()).await;

    assert!(!matches!(raced, Ok(Ok(()))), "the dial never finished");
    assert_eq!(doh_answer_for(&doh, "localhost").await, [UNROUTABLE]);
    server.abort();
}

/// QUIC 握手没有回应：同样在竞速放弃之前就查了。
#[tokio::test]
async fn a_stalled_quic_dial_asks_doh_before_the_race_gives_up() {
    let silent = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind");
    let port = silent.local_addr().expect("address").port();
    let limit = Duration::from_millis(400);
    let doh = DohCache::new(Arc::new(AfterResolve::default()));
    let mut client = QUICClient::new(
        ClientConfig::new(format!("quic://localhost:{port}"))
            .quic()
            .with_tls(TlsConfig::none())
            .with_connect_timeout(limit)
            .with_doh(doh.clone()),
    )
    .expect("QUIC endpoint");

    // 两个时限落在同一刻时谁先报不一定，这里只要拨号没成。
    let raced = tokio::time::timeout(limit, client.establish()).await;

    assert!(!matches!(raced, Ok(Ok(()))), "the dial never finished");
    assert_eq!(doh_answer_for(&doh, "localhost").await, [UNROUTABLE]);
}

/// 很快失败的拨号不算超时：连接被拒，拨号这边不再要求查询。
#[tokio::test]
async fn a_refused_dial_leaves_doh_alone() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
        .port();
    let doh = DohCache::new(Arc::new(AfterResolve::default()));
    let mut client = WebSocketClient::new(
        ClientConfig::new(format!("ws://localhost:{port}/ws"))
            .websocket()
            .with_connect_timeout(LIMIT)
            .with_doh(doh.clone()),
    );

    let error = client.establish().await.unwrap_err();

    assert!(
        matches!(failure(&error), AttemptFailure::Unreachable { .. }),
        "{error}"
    );
    assert!(doh_answer_for(&doh, "localhost").await.is_empty());
}

#[tokio::test]
async fn a_resolvable_host_ignores_the_hints() {
    let (port, server) = ws_server().await;
    let mut client = WebSocketClient::new(
        ClientConfig::new(format!("ws://localhost:{port}/ws"))
            .websocket()
            .with_connect_timeout(LIMIT)
            .with_ip_hints(vec![UNROUTABLE_PRIVATE]),
    );

    client.establish().await.expect("DNS comes first");

    let _ = client.disconnect().await;
    server.await.unwrap();
}

#[tokio::test]
async fn without_hints_a_dns_failure_is_unreachable() {
    let mut websocket = WebSocketClient::new(
        ClientConfig::new(format!("wss://{HOST}/ws"))
            .websocket()
            .with_connect_timeout(LIMIT),
    );
    let error = websocket.establish().await.unwrap_err();
    assert!(
        matches!(failure(&error), AttemptFailure::Unreachable { .. }),
        "{error}"
    );

    let mut quic = quic_client(443, TlsConfig::none(), vec![], LIMIT);
    let error = quic.establish().await.unwrap_err();
    assert!(
        matches!(failure(&error), AttemptFailure::Unreachable { .. }),
        "{error}"
    );
}

struct NoopHandler;

#[async_trait]
impl ServerEventHandler for NoopHandler {}

struct Silent;

#[async_trait]
impl MessageListener for Silent {}

/// 上层的入口：`with_ip_hints` 经竞速带到传输，收到 CONNECT_ACK 才算连上。
#[tokio::test]
async fn the_race_reaches_the_gateway_through_a_hint() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a probe TCP socket")
        .local_addr()
        .expect("probe address")
        .port();
    let address = format!("127.0.0.1:{port}");
    let server = FlareServerBuilder::new(address.clone(), Arc::new(NoopHandler))
        .with_protocol_address(TransportProtocol::WebSocket, address)
        .with_protocols(vec![TransportProtocol::WebSocket])
        .build()
        .expect("build server");
    server.start().await.expect("start server");

    let url = format!("ws://{HOST}:{port}/ws");
    let client = FlareClientBuilder::new(url.clone())
        .with_protocol_url(TransportProtocol::WebSocket, url)
        .with_protocol_race(vec![TransportProtocol::WebSocket])
        .with_user_id("bob".to_string())
        .with_listener(Arc::new(Silent))
        .with_device_info(DeviceInfo::new("bob-pc".to_string(), DevicePlatform::PC))
        .with_ip_hints(vec![LOOPBACK])
        .build_with_race()
        .await
        .expect("CONNECT_ACK through the hint");

    assert_eq!(
        client.connect_report().protocol,
        TransportProtocol::WebSocket
    );
    let _ = client.disconnect().await;
    server.stop().await.expect("stop server");
}
