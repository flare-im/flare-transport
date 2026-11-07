//! TCP 服务端实现
//!
//! 接受原始 TCP 连接，使用 length-prefixed Flare Frame（与 QUIC bi-stream 同帧格式）。

use crate::common::error::Result;
use crate::server::config::ServerConfig;
use crate::server::connection::ConnectionManager;
use crate::server::transports::Server;
use crate::server::transports::common::{
    AcceptGate, ServerConnectionHelper, overload_reply_limiter, reject_overloaded,
};
use crate::server::transports::server_core::ServerCore;
use crate::transport::connection::Connection;
use crate::transport::tcp::TCPTransport;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tracing::debug;

pub struct TCPServer {
    config: ServerConfig,
    core: Arc<ServerCore>,
    is_running: Arc<Mutex<bool>>,
    /// 关闭后 accept 循环退出并释放监听端口；每次 start 换一个新的
    accept_gate: AcceptGate,
}

impl TCPServer {
    pub fn new(config: ServerConfig) -> Self {
        Self::with_connection_manager(config, None)
    }

    pub fn with_connection_manager(
        config: ServerConfig,
        connection_manager: Option<Arc<ConnectionManager>>,
    ) -> Self {
        let core = Arc::new(ServerCore::new(&config, connection_manager));
        Self {
            config,
            core,
            is_running: Arc::new(Mutex::new(false)),
            accept_gate: AcceptGate::new(),
        }
    }

    pub fn with_shared_core(config: ServerConfig, core: Arc<ServerCore>) -> Self {
        Self {
            config,
            core,
            is_running: Arc::new(Mutex::new(false)),
            accept_gate: AcceptGate::new(),
        }
    }
}

#[async_trait]
impl Server for TCPServer {
    async fn start(&mut self) -> Result<()> {
        let bind_str = self
            .config
            .get_protocol_address(&crate::common::config_types::TransportProtocol::TCP)
            .replace("tcp://", "")
            .replace("TCP://", "");
        let addr = bind_str.parse::<std::net::SocketAddr>().map_err(|e| {
            crate::common::error::FlareError::protocol_error(format!("Invalid address: {e}"))
        })?;

        let listener = TcpListener::bind(addr).await.map_err(|e| {
            crate::common::error::FlareError::connection_failed(format!("Failed to bind: {e}"))
        })?;

        *self.is_running.lock().await = true;
        self.accept_gate = AcceptGate::new();
        let mut accept_stopped = self.accept_gate.watch();
        self.core.start_heartbeat(&self.config);

        let manager = Arc::clone(&self.core.connection_manager);
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        let core = Arc::clone(&self.core);
        let accept_limiter = Arc::new(Semaphore::new(config.max_handshake_concurrency.max(1)));
        let overload_limiter = overload_reply_limiter(&config);

        tokio::spawn(async move {
            debug!("[TCPServer] listening on {addr}");
            while *is_running.lock().await {
                let accepted = tokio::select! {
                    biased;
                    () = accept_stopped.closed() => break,
                    accepted = listener.accept() => accepted,
                };
                match accepted {
                    Ok((stream, peer)) => {
                        debug!("[TCPServer] accepted connection from {peer}");
                        let manager_clone = Arc::clone(&manager);
                        let config_clone = config.clone();
                        let core_clone = Arc::clone(&core);
                        let permit = match Arc::clone(&accept_limiter).try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                debug!(
                                    "[TCPServer] accept concurrency saturated: {}",
                                    config.max_handshake_concurrency
                                );
                                // 回一帧 OVERLOADED 再关；回绝名额也满了就直接丢弃
                                if let Ok(reply) = Arc::clone(&overload_limiter).try_acquire_owned()
                                {
                                    let config = config.clone();
                                    tokio::spawn(async move {
                                        let _reply = reply;
                                        reject_overloaded(
                                            Box::new(TCPTransport::new(stream)),
                                            &config,
                                        )
                                        .await;
                                    });
                                }
                                continue;
                            }
                        };

                        tokio::spawn(async move {
                            let _permit = permit;
                            handle_tcp_connection(stream, manager_clone, &config_clone, core_clone)
                                .await;
                        });
                    }
                    Err(e) => {
                        debug!("[TCPServer] accept failed: {e}");
                    }
                }
            }
            debug!("[TCPServer] stopped listening");
        });

        Ok(())
    }

    async fn stop_accepting(&mut self) {
        self.accept_gate.close();
    }

    async fn stop(&mut self) -> Result<()> {
        self.accept_gate.close();
        ServerConnectionHelper::stop_server(&self.core, &self.is_running)
            .await
            .map_err(|e| {
                crate::common::error::FlareError::connection_failed(format!(
                    "Failed to stop TCP server: {e}"
                ))
            })
    }

    fn is_running(&self) -> bool {
        tokio::task::block_in_place(|| *self.is_running.blocking_lock())
    }
}

async fn handle_tcp_connection(
    stream: TcpStream,
    manager: Arc<ConnectionManager>,
    config: &ServerConfig,
    core: Arc<ServerCore>,
) {
    let transport = TCPTransport::new(stream);
    let connection: Box<dyn Connection> = Box::new(transport);

    if let Err(e) =
        ServerConnectionHelper::setup_new_connection(connection, manager, config, core).await
    {
        debug!("[TCPServer] setup connection failed: {e}");
    }
}
