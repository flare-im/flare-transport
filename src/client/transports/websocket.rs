//! WebSocket 客户端实现
//!
//! 只处理协议层，连接状态管理、心跳、消息路由等功能委托给 ClientCore

use crate::client::config::ClientConfig;
#[cfg(not(target_arch = "wasm32"))]
use crate::client::link::AttemptFailure;
use crate::client::transports::common::{ClientConnectionHelper, ClientMessageObserver};
use crate::client::transports::{Client, ClientCore};
#[cfg(not(target_arch = "wasm32"))]
use crate::common::cert::create_client_config_with_tls;
use crate::common::error::{FlareError, Result};
use crate::common::generate_id;
use crate::common::platform::timeout;
use crate::common::protocol::Frame;
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
#[cfg(not(target_arch = "wasm32"))]
use crate::transport::websocket::WebSocketTransport;
#[cfg(target_arch = "wasm32")]
use crate::transport::websocket_wasm::WebSocketTransport;
use async_trait::async_trait;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::net::TcpStream;
use tokio::sync::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite::{
    self, client::IntoClientRequest, error::UrlError, handshake::client::Request,
};
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream, client_async_tls_with_config};

/// WebSocket 客户端
///
/// 只处理协议层，其他功能委托给 ClientCore
pub struct WebSocketClient {
    config: ClientConfig,
    connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
    /// `connection` 是协议竞速刚建好、还没发过 CONNECT 的网络连接（见 QUIC 客户端同名字段）。
    /// 只有这时 `connect()` 才直接复用它，不再重新拨号。
    connection_prepared: bool,
    connection_id: String,
    core: ClientCore,
}

impl WebSocketClient {
    /// 创建新的 WebSocket 客户端
    pub fn new(config: ClientConfig) -> Self {
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        let core = ClientCore::new(&config);

        Self {
            config,
            connection: None,
            connection_prepared: false,
            connection_id,
            core,
        }
    }

    /// 创建并连接
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let mut client = Self::new(config);
        client.connect().await?;
        Ok(client)
    }

    /// 使用 ClientCore 创建（用于 HybridClient）
    pub fn with_core(config: ClientConfig, core: ClientCore) -> Self {
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        Self {
            config,
            connection: None,
            connection_prepared: false,
            connection_id,
            core,
        }
    }

    /// 仅建立网络连接（不发送 CONNECT 消息）
    ///
    /// 用于协议竞速：先建立网络连接，选择最快协议，然后再发送 CONNECT
    pub async fn establish_network_connection(
        &mut self,
    ) -> Result<Arc<Mutex<Box<dyn Connection>>>> {
        let connection = self.establish_websocket_connection().await?;
        let connection_arc = Arc::new(Mutex::new(connection));
        // 保存连接，以便后续 connect() 时使用
        self.connection = Some(Arc::clone(&connection_arc));
        self.connection_prepared = true;
        Ok(connection_arc)
    }

    /// 内部连接实现
    async fn internal_connect(&mut self) -> Result<()> {
        // 协议竞速刚建好的网络连接直接发 CONNECT；其余情况照旧新建 WebSocket 连接。
        // 此前竞速胜出的 WebSocket 在这里再拨号一次，竞速建好的那条被直接丢掉。
        let connection_arc = match self.connection.as_ref() {
            Some(prepared) if std::mem::take(&mut self.connection_prepared) => Arc::clone(prepared),
            _ => {
                let connection = self.establish_network_connection().await?;
                self.connection_prepared = false;
                connection
            }
        };

        // 设置连接和观察者（会发送 CONNECT 消息）
        self.setup_connection_with_observer(connection_arc.clone())
            .await?;

        // 心跳在 CONNECT_ACK 协商完成后由 ClientCore 启动

        // 通知连接成功
        self.core
            .handle_connection_event(&ConnectionEvent::Connected);

        Ok(())
    }

    /// 建立 WebSocket 连接
    async fn establish_websocket_connection(&self) -> Result<Box<dyn Connection>> {
        let url_str = self
            .config
            .get_protocol_url(&crate::common::config_types::TransportProtocol::WebSocket);

        #[cfg(not(target_arch = "wasm32"))]
        {
            // 入站消息上限 10MB，防超大帧 OOM；与 TCP/QUIC MAX_FRAME_LENGTH 及服务端
            // max_message_size 对齐（tungstenite 默认 64MB 过大，也削弱 gzip 解压炸弹面）。
            let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                .max_message_size(Some(10 * 1024 * 1024))
                .max_frame_size(Some(10 * 1024 * 1024));
            let tls_connector = self.tls_connector(&url_str)?;
            let ws_stream = timeout(
                self.config.connect_timeout,
                self.dial_websocket(&url_str, ws_config, tls_connector),
            )
            .await
            .map_err(|_| FlareError::connection_timeout("Connection timeout".to_string()))??;

            let transport = WebSocketTransport::new(ws_stream);
            Ok(Box::new(transport))
        }

        #[cfg(target_arch = "wasm32")]
        {
            let transport = timeout(
                self.config.connect_timeout,
                WebSocketTransport::connect(&url_str),
            )
            .await
            .map_err(|_| FlareError::connection_timeout("Connection timeout".to_string()))??;
            Ok(Box::new(transport))
        }
    }

    /// 建 TCP、做 TLS、升级 WebSocket。TCP 连到 `crate::client::dial` 选出的地址，TLS 和升级请求
    /// 都按 URL：SNI、证书校验和钉扎用的是主机名，不是实际连上的 IP。
    #[cfg(not(target_arch = "wasm32"))]
    async fn dial_websocket(
        &self,
        url: &str,
        ws_config: tungstenite::protocol::WebSocketConfig,
        tls_connector: Option<Connector>,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let request = url
            .into_client_request()
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;
        let (host, port) = dial_endpoint(&request)?;
        let dial = async {
            let stream = crate::client::dial::connect_tcp(
                &host,
                port,
                &self.config.ip_hints,
                self.config.doh.as_ref(),
                self.config.connect_timeout,
            )
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::TimedOut => FlareError::connection_timeout(e.to_string()),
                _ => FlareError::connection_failed(e.to_string()),
            })?;
            let (ws_stream, _) =
                client_async_tls_with_config(request, stream, Some(ws_config), tls_connector)
                    .await
                    .map_err(|e| {
                        let error = FlareError::connection_failed(e.to_string());
                        if let Some(doh) = &self.config.doh
                            && matches!(
                                AttemptFailure::from_establish_error(&error),
                                AttemptFailure::Tls { .. }
                            )
                        {
                            doh.tls_failed(&host);
                        }
                        error
                    })?;
            Ok(ws_stream)
        };
        crate::client::dial::watch_dial(
            &host,
            self.config.doh.as_ref(),
            self.config.connect_timeout,
            dial,
        )
        .await
    }

    /// 设置连接和观察者
    async fn setup_connection_with_observer(
        &mut self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
    ) -> Result<()> {
        // 创建消息观察者（使用 Arc 包装，共享同一个 core 实例）
        // 注意：ClientCore::clone 现在会共享 client_connection，所以可以安全使用
        let core_arc = Arc::new(self.core.clone());
        let message_observer = Arc::new(ClientMessageObserver::new(core_arc));

        // 设置连接并发送 CONNECT 消息
        ClientConnectionHelper::setup_connection_and_send_connect(
            Arc::clone(&connection),
            &mut self.core,
            message_observer,
        )
        .await?;

        self.connection = Some(connection);
        Ok(())
    }

    /// 发送 Frame（内部实现）
    async fn send_frame_internal(&self, frame: &Frame) -> Result<()> {
        ClientConnectionHelper::send_frame_internal(&self.core, self.connection.as_ref(), frame)
            .await
    }

    /// 获取 ClientCore（用于外部访问）
    pub fn core(&self) -> &ClientCore {
        &self.core
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn tls_connector(&self, url: &str) -> Result<Option<Connector>> {
        if url.starts_with("ws://") {
            return Ok(None);
        }
        if !self.config.tls.requires_custom_client_tls() {
            return Ok(None);
        }
        if !url.starts_with("wss://") {
            return Err(FlareError::connection_failed(format!(
                "WebSocket URL must start with ws:// or wss://, got: {url}"
            )));
        }
        let tls_config = create_client_config_with_tls(&self.config.tls)
            .map_err(|error| FlareError::connection_failed(error.to_string()))?;
        Ok(Some(Connector::Rustls(Arc::new(tls_config))))
    }

    /// 发送消息并等待服务端响应（按 Frame.message_id 匹配）
    pub async fn send_frame_and_wait(
        &mut self,
        frame: &Frame,
        timeout_duration: std::time::Duration,
    ) -> Result<Frame> {
        if frame.message_id.is_empty() {
            return Err(FlareError::protocol_error(
                "message_id is empty".to_string(),
            ));
        }

        let rx = self.core.register_pending_response(&frame.message_id).await;
        self.send_frame(frame).await?;

        match timeout(timeout_duration, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => {
                self.core.cancel_pending_response(&frame.message_id).await;
                Err(FlareError::protocol_error(
                    "Response channel closed".to_string(),
                ))
            }
            Err(_) => {
                self.core.cancel_pending_response(&frame.message_id).await;
                Err(FlareError::protocol_error(format!(
                    "Response timeout for message_id {}",
                    frame.message_id
                )))
            }
        }
    }
}

/// 请求 URL 里要拨的主机与端口。IPv6 字面量去掉方括号，解析器不认带方括号的写法。
#[cfg(not(target_arch = "wasm32"))]
fn dial_endpoint(request: &Request) -> Result<(String, u16)> {
    let url_error =
        |error| FlareError::connection_failed(tungstenite::Error::Url(error).to_string());
    let uri = request.uri();
    let host = uri.host().ok_or_else(|| url_error(UrlError::NoHostName))?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    let port = uri
        .port_u16()
        .or(match uri.scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or_else(|| url_error(UrlError::UnsupportedUrlScheme))?;
    Ok((host.to_owned(), port))
}

#[cfg(target_arch = "wasm32")]
unsafe impl Send for WebSocketClient {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for WebSocketClient {}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::common::config_types::TlsConfig;
    use std::path::PathBuf;

    /// 客户端不自己重连：断开后的发送直接失败，不去拨号。
    #[tokio::test]
    async fn send_after_requested_disconnect_does_not_reconnect() {
        let config = ClientConfig::new("ws://127.0.0.1:9/ws".to_string()).websocket();
        let mut client = WebSocketClient::new(config);
        client
            .disconnect()
            .await
            .expect("disconnect without a connection");

        let ping = crate::common::protocol::frame_with_system_command(
            crate::common::protocol::ping(),
            crate::common::protocol::Reliability::AtLeastOnce,
        );
        let started = std::time::Instant::now();
        assert!(client.send_frame(&ping).await.is_err());
        assert!(client.connection.is_none(), "must not dial again");
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn dial_endpoint_reads_host_and_port_from_the_url() {
        let endpoint = |url: &str| dial_endpoint(&url.into_client_request().unwrap()).unwrap();
        assert_eq!(
            endpoint("wss://ap-1.example.com/ws"),
            ("ap-1.example.com".to_string(), 443)
        );
        assert_eq!(
            endpoint("ws://ap-1.example.com/ws"),
            ("ap-1.example.com".to_string(), 80)
        );
        assert_eq!(
            endpoint("wss://ap-1.example.com:8443/ws"),
            ("ap-1.example.com".to_string(), 8443)
        );
        assert_eq!(endpoint("ws://[::1]:8080/ws"), ("::1".to_string(), 8080));
    }

    #[test]
    fn plain_ws_ignores_custom_client_tls() {
        let mut config = ClientConfig::new("ws://127.0.0.1:60051/ws".to_string()).websocket();
        config.tls = TlsConfig::default().with_ca_cert(PathBuf::from("/tmp/flare-ca.crt"));
        let client = WebSocketClient::new(config);

        let connector = client
            .tls_connector("ws://127.0.0.1:60051/ws")
            .expect("plain ws must not require a TLS connector");

        assert!(connector.is_none());
    }

    #[test]
    fn secure_wss_uses_custom_client_tls() {
        let mut config = ClientConfig::new("wss://127.0.0.1:60051/ws".to_string()).websocket();
        config.tls = TlsConfig::default()
            .with_spki_sha256_pin("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        let client = WebSocketClient::new(config);

        let connector = client
            .tls_connector("wss://127.0.0.1:60051/ws")
            .expect("wss should accept custom TLS settings");

        assert!(connector.is_some());
    }
}

#[async_trait]
impl Client for WebSocketClient {
    async fn connect(&mut self) -> Result<()> {
        if !self.core.can_connect() {
            return Err(FlareError::protocol_error(
                "Cannot connect: state is unavailable".to_string(),
            ));
        }

        self.core.state_manager.start_connecting();

        // 只连这一次：失败交给上层，由它决定何时、向哪里重连
        match self.internal_connect().await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.core.state_manager.set_failed();
                Err(e)
            }
        }
    }

    async fn establish(&mut self) -> Result<()> {
        self.establish_network_connection().await.map(|_| ())
    }

    fn set_disconnect_requested(&mut self, value: bool) {
        self.core.set_disconnect_requested(value);
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connection_prepared = false;
        ClientConnectionHelper::disconnect_internal(self.connection.take(), &mut self.core).await
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        self.send_frame_internal(frame).await
    }

    fn is_connected(&self) -> bool {
        matches!(
            self.core.state(),
            crate::client::connection::ConnectionState::Connected
        ) && self.connection.is_some()
            && self.core.is_negotiation_completed()
    }

    fn add_observer(&mut self, observer: ArcObserver) {
        self.core.add_observer(observer);
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        self.core.remove_observer(observer);
    }

    fn connection_id(&self) -> Option<String> {
        Some(self.connection_id.clone())
    }
}
