//! TCP 客户端实现
//!
//! 使用 length-prefixed 原始 TCP 传输 Flare Frame（与 QUIC bi-stream 同帧格式）。

use crate::client::config::ClientConfig;
use crate::client::transports::common::{ClientConnectionHelper, ClientMessageObserver};
use crate::client::transports::{Client, ClientCore};
use crate::common::error::{FlareError, Result};
use crate::common::generate_id;
use crate::common::platform::timeout;
use crate::common::protocol::Frame;
use crate::transport::connection::Connection;
use crate::transport::events::{ArcObserver, ConnectionEvent};
use crate::transport::tcp::TCPTransport;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpStream, lookup_host};
use tokio::sync::Mutex;

/// TCP 客户端（Native only）
pub struct TCPClient {
    config: ClientConfig,
    connection: Option<Arc<Mutex<Box<dyn Connection>>>>,
    /// `connection` 是协议竞速刚建好、还没发过 CONNECT 的网络连接（见 QUIC 客户端同名字段）。
    connection_prepared: bool,
    connection_id: String,
    core: ClientCore,
}

impl TCPClient {
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

    pub async fn connect_with_config(config: ClientConfig) -> Result<Self> {
        let mut client = Self::new(config);
        client.connect().await?;
        Ok(client)
    }

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

    pub async fn establish_network_connection(
        &mut self,
    ) -> Result<Arc<Mutex<Box<dyn Connection>>>> {
        let connection = self.establish_tcp_connection().await?;
        let connection_arc = Arc::new(Mutex::new(connection));
        self.connection = Some(Arc::clone(&connection_arc));
        self.connection_prepared = true;
        Ok(connection_arc)
    }

    async fn internal_connect(&mut self) -> Result<()> {
        // 只复用协议竞速刚建好的网络连接；被踢后留下的旧连接关掉重建（原因见 QUIC 客户端）。
        let connection_arc = match self.connection.take() {
            Some(prepared) if std::mem::take(&mut self.connection_prepared) => prepared,
            stale => {
                self.connection_prepared = false;
                if let Some(stale) = stale {
                    let mut conn = stale.lock().await;
                    let _ = conn.close().await;
                }
                Arc::new(Mutex::new(self.establish_tcp_connection().await?))
            }
        };

        self.setup_connection_with_observer(connection_arc.clone())
            .await?;

        self.core
            .handle_connection_event(&ConnectionEvent::Connected);
        Ok(())
    }

    async fn establish_tcp_connection(&self) -> Result<Box<dyn Connection>> {
        let addr = self.parse_server_address().await?;
        let stream = timeout(self.config.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| FlareError::connection_timeout("Connection timeout".to_string()))?
            .map_err(|e| FlareError::connection_failed(e.to_string()))?;

        Ok(Box::new(TCPTransport::new(stream)))
    }

    async fn parse_server_address(&self) -> Result<SocketAddr> {
        let address_str = self
            .config
            .get_protocol_url(&crate::common::config_types::TransportProtocol::TCP)
            .replace("tcp://", "")
            .replace("TCP://", "");

        match address_str.parse::<SocketAddr>() {
            Ok(addr) => Ok(addr),
            Err(_) => lookup_host(&address_str)
                .await
                .map_err(|e| FlareError::protocol_error(format!("DNS lookup failed: {e}")))?
                .next()
                .ok_or_else(|| {
                    FlareError::protocol_error(format!("No address found for {address_str}"))
                }),
        }
    }

    async fn setup_connection_with_observer(
        &mut self,
        connection: Arc<Mutex<Box<dyn Connection>>>,
    ) -> Result<()> {
        let core_arc = Arc::new(self.core.clone());
        let message_observer = Arc::new(ClientMessageObserver::new(core_arc));
        ClientConnectionHelper::setup_connection_and_send_connect(
            Arc::clone(&connection),
            &mut self.core,
            message_observer,
        )
        .await?;
        self.connection = Some(connection);
        Ok(())
    }

    async fn send_frame_internal(&self, frame: &Frame) -> Result<()> {
        ClientConnectionHelper::send_frame_internal(&self.core, self.connection.as_ref(), frame)
            .await
    }

    pub fn core(&self) -> &ClientCore {
        &self.core
    }
}

#[async_trait]
impl Client for TCPClient {
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
