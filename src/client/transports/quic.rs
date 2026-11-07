//! QUIC 客户端实现
//!
//! 只处理协议层，连接状态管理、心跳、消息路由等功能委托给 ClientCore

use crate::client::config::ClientConfig;
use crate::client::dial;
use crate::client::link::AttemptFailure;
use crate::client::transports::common::{ClientConnectionHelper, ClientMessageObserver};
use crate::client::transports::{Client, ClientCore};
use crate::common::config_types::TlsConfig;
use crate::common::error::{FlareError, Result};
use crate::common::generate_id;
use crate::common::platform::timeout;
use crate::common::protocol::Frame;
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use crate::transport::quic::QUICTransport;
use async_trait::async_trait;
use quinn::Endpoint;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// QUIC 客户端
///
/// 只处理协议层，其他功能委托给 ClientCore
pub struct QUICClient {
    config: ClientConfig,
    connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
    /// `connection` 是协议竞速刚建好、还没发过 CONNECT 的网络连接。只有这时 `connect()`
    /// 才能直接复用它；否则它是上一条会话留下的连接（可能已被服务端踢掉）。
    connection_prepared: bool,
    connection_id: String,
    core: ClientCore,
    endpoint: Option<Endpoint>,
    _client_config: Option<quinn::ClientConfig>, // 保持 client_config 的生命周期
}

impl QUICClient {
    /// 创建新的 QUIC 客户端
    pub fn new(config: ClientConfig) -> Result<Self> {
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);
        let core = ClientCore::new(&config);

        let (endpoint, client_config) = Self::create_quic_endpoint_with_tls(&config.tls)?;

        Ok(Self {
            config,
            connection: None,
            connection_prepared: false,
            connection_id,
            core,
            endpoint: Some(endpoint),
            _client_config: Some(client_config),
        })
    }

    /// 创建并连接
    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let mut client = Self::new(config)?;
        client.connect().await?;
        Ok(client)
    }

    /// 使用 ClientCore 创建（用于 HybridClient）
    pub fn with_core(config: ClientConfig, core: ClientCore) -> Result<Self> {
        Self::with_core_and_endpoint(config, core, None)
    }

    /// 使用 ClientCore 和预创建的 Endpoint 创建（用于协议竞速优化）
    ///
    /// 如果提供了 endpoint，则使用它；否则创建新的 endpoint
    pub fn with_core_and_endpoint(
        config: ClientConfig,
        core: ClientCore,
        endpoint_opt: Option<(Endpoint, quinn::ClientConfig)>,
    ) -> Result<Self> {
        let connection_id = config.connection_id.clone().unwrap_or_else(generate_id);

        // 端点建不出来（本机 UDP 不可用、TLS 配置有误）时报错，由竞速记为这个协议的失败；
        // 以前这里直接 panic，整个建连随之崩掉。
        let (endpoint, client_config) = match endpoint_opt {
            Some(prepared) => prepared,
            None => Self::create_quic_endpoint_with_tls(&config.tls)?,
        };

        Ok(Self {
            config,
            connection: None,
            connection_prepared: false,
            connection_id,
            core,
            endpoint: Some(endpoint),
            _client_config: Some(client_config),
        })
    }

    /// 创建 QUIC Endpoint（统一创建逻辑）
    ///
    /// 公开方法，允许外部预创建 endpoint（用于协议竞速优化）
    pub fn create_quic_endpoint() -> Result<(Endpoint, quinn::ClientConfig)> {
        Self::create_quic_endpoint_with_tls(&TlsConfig::none())
    }

    pub fn create_quic_endpoint_with_tls(
        tls: &TlsConfig,
    ) -> Result<(Endpoint, quinn::ClientConfig)> {
        use crate::common::cert::create_client_config;
        use crate::common::cert::create_client_config_with_tls;
        use quinn::crypto::rustls::QuicClientConfig;

        let rustls_config = if tls.requires_custom_client_tls() {
            create_client_config_with_tls(tls)
        } else {
            create_client_config()
        }
        .map_err(|e| {
            FlareError::protocol_error(format!("Failed to create client TLS config: {}", e))
        })?;

        let rustls_config_arc = Arc::new(rustls_config);
        let quic_crypto_config = QuicClientConfig::try_from(rustls_config_arc).map_err(|e| {
            FlareError::protocol_error(format!("Failed to create QUIC client config: {}", e))
        })?;

        let client_config = quinn::ClientConfig::new(Arc::new(quic_crypto_config));

        let mut endpoint = Endpoint::client("[::]:0".parse().unwrap()).map_err(|e| {
            FlareError::connection_failed(format!("Failed to create endpoint: {}", e))
        })?;

        endpoint.set_default_client_config(client_config.clone());

        Ok((endpoint, client_config))
    }

    /// 仅建立网络连接（不发送 CONNECT 消息）
    ///
    /// 用于协议竞速：先建立网络连接，选择最快协议，然后再发送 CONNECT
    pub async fn establish_network_connection(
        &mut self,
    ) -> Result<Arc<Mutex<Box<dyn Connection>>>> {
        let connection = self.establish_quic_connection().await?;
        let connection_arc = Arc::new(Mutex::new(connection));
        // 保存连接，以便后续 connect() 时使用
        self.connection = Some(Arc::clone(&connection_arc));
        self.connection_prepared = true;
        Ok(connection_arc)
    }

    /// 内部连接实现
    async fn internal_connect(&mut self) -> Result<()> {
        // 协议竞速刚建好的网络连接直接发 CONNECT；其余情况一律新建。
        //
        // 被踢/被服务端移除之后，旧连接还挂在 `self.connection` 上。再次 `connect()` 若复用它，
        // CONNECT 会写进服务端已经不读的旧流：传输层照样报 Connected，却永远等不到 CONNECT_ACK。
        let connection_arc = match self.connection.take() {
            Some(prepared) if std::mem::take(&mut self.connection_prepared) => prepared,
            stale => {
                self.connection_prepared = false;
                if let Some(stale) = stale {
                    let mut conn = stale.lock().await;
                    let _ = conn.close().await;
                }
                Arc::new(Mutex::new(self.establish_quic_connection().await?))
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

    /// 建立 QUIC 连接
    async fn establish_quic_connection(&self) -> Result<Box<dyn Connection>> {
        let deadline = Instant::now() + self.config.connect_timeout;

        // 解析服务器地址
        let (addresses, hostname) = self.parse_server_address().await?;

        // 获取 endpoint
        let endpoint = self
            .endpoint
            .as_ref()
            .ok_or_else(|| FlareError::connection_failed("Endpoint not initialized".to_string()))?;

        // 建立连接：IP 提示只换拨到的地址，握手的 SNI 与证书校验始终按 hostname
        let attempts = async {
            let mut last_error = None;
            for (index, address) in addresses.iter().enumerate() {
                let limit = dial::share_of(deadline, addresses.len() - index);
                match Self::connect_address(endpoint, *address, &hostname, limit).await {
                    Ok(connection) => return Ok(connection),
                    // 证书或钉扎不符：换 IP 也是同一张证书
                    Err(error)
                        if matches!(
                            AttemptFailure::from_establish_error(&error),
                            AttemptFailure::Tls { .. }
                        ) =>
                    {
                        if let Some(doh) = &self.config.doh {
                            doh.tls_failed(&hostname);
                        }
                        return Err(error);
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            Err(last_error
                .unwrap_or_else(|| FlareError::connection_failed("No address to dial".to_string())))
        };
        // 解析已经用掉一部分时限：按剩下的算，否则竞速的时限先到。
        dial::watch_dial(
            &hostname,
            self.config.doh.as_ref(),
            dial::share_of(deadline, 1),
            attempts,
        )
        .await
    }

    /// 向一个地址握手并打开双向流，握手最多等 `limit`
    async fn connect_address(
        endpoint: &Endpoint,
        address: SocketAddr,
        hostname: &str,
        limit: Duration,
    ) -> Result<Box<dyn Connection>> {
        let connecting = endpoint
            .connect(address, hostname)
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;

        let quinn_connection = timeout(limit, connecting)
            .await
            .map_err(|_| FlareError::connection_timeout("Connection timeout".to_string()))?
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;

        // 打开双向流
        let (send, recv) = quinn_connection
            .open_bi()
            .await
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;

        let transport = QUICTransport::new(send, recv);
        Ok(Box::new(transport))
    }

    /// 解析服务器地址：要依次拨的地址与握手用的主机名
    async fn parse_server_address(&self) -> Result<(Vec<SocketAddr>, String)> {
        let address_str = self
            .config
            .get_protocol_url(&crate::common::config_types::TransportProtocol::QUIC)
            .replace("quic://", "");

        // 尝试直接解析为 SocketAddr
        let addresses = match address_str.parse::<SocketAddr>() {
            Ok(addr) => vec![addr],
            Err(_) => self.resolve_host(&address_str).await?,
        };

        // 确定 hostname（返回 String 而不是 &str）
        let hostname =
            if address_str.starts_with("localhost") || address_str.starts_with("127.0.0.1") {
                "localhost".to_string()
            } else {
                address_str
                    .split(':')
                    .next()
                    .unwrap_or("localhost")
                    .to_string()
            };

        Ok((addresses, hostname))
    }

    /// DNS 解析 `host:port`：成功时只用第一个结果，失败时退到 IP 提示
    async fn resolve_host(&self, address_str: &str) -> Result<Vec<SocketAddr>> {
        let (host, port) = address_str
            .rsplit_once(':')
            .and_then(|(host, port)| Some((host, port.parse::<u16>().ok()?)))
            .ok_or_else(|| {
                FlareError::protocol_error(format!(
                    "DNS lookup failed: invalid socket address {address_str}"
                ))
            })?;
        let mut chosen = dial::resolve(
            host,
            port,
            &self.config.ip_hints,
            self.config.doh.as_ref(),
            self.config.connect_timeout,
        )
        .await
        .map_err(|e| FlareError::protocol_error(format!("DNS lookup failed: {}", e)))?;
        if !chosen.direct {
            chosen.addresses.truncate(1);
        }
        Ok(chosen.addresses)
    }

    /// 设置连接和观察者
    async fn setup_connection_with_observer(
        &mut self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
    ) -> Result<()> {
        // 创建消息观察者
        let core_clone = Arc::new(self.core.clone());
        let message_observer = Arc::new(ClientMessageObserver::new(core_clone));

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
}

#[async_trait]
impl Client for QUICClient {
    async fn connect(&mut self) -> Result<()> {
        if !self.core.can_connect() {
            return Err(FlareError::protocol_error(
                "Cannot connect: connection state is not ready".to_string(),
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
