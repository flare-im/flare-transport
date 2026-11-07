//! WebSocket 服务端实现
//!
//! 专注于 WebSocket 协议层面的连接处理，连接管理和心跳检测由 ServerCore 统一管理

use crate::common::error::Result;
use crate::server::config::ServerConfig;
use crate::server::connection::ConnectionManager;
use crate::server::transports::Server;
use crate::server::transports::common::{
    AcceptGate, ServerConnectionHelper, overload_reply_limiter, reject_overloaded,
};
use crate::server::transports::server_core::ServerCore;
use crate::transport::connection::Connection;
use crate::transport::websocket::WebSocketTransport;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;
use tokio_tungstenite::accept_async;
use tracing::debug;

/// WebSocket 服务端
///
/// 专注于 WebSocket 协议层面的连接处理
pub struct WebSocketServer {
    config: ServerConfig,
    core: Arc<ServerCore>,
    is_running: Arc<Mutex<bool>>,
    /// 关闭后 accept 循环退出并释放监听端口；每次 start 换一个新的
    accept_gate: AcceptGate,
}

impl WebSocketServer {
    /// 创建新的 WebSocket 服务端
    pub fn new(config: ServerConfig) -> Self {
        Self::with_connection_manager(config, None)
    }

    /// 使用指定的连接管理器创建 WebSocket 服务端
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

    /// 使用指定的 ServerCore 创建 WebSocket 服务端（用于共享 ServerCore）
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
impl Server for WebSocketServer {
    async fn start(&mut self) -> Result<()> {
        let bind_address = self
            .config
            .get_protocol_address(&crate::common::config_types::TransportProtocol::WebSocket);
        let addr = bind_address.parse::<std::net::SocketAddr>().map_err(|e| {
            crate::common::error::FlareError::protocol_error(format!("Invalid address: {}", e))
        })?;

        let listener = TcpListener::bind(addr).await.map_err(|e| {
            crate::common::error::FlareError::connection_failed(format!("Failed to bind: {}", e))
        })?;

        *self.is_running.lock().await = true;
        self.accept_gate = AcceptGate::new();
        let mut accept_stopped = self.accept_gate.watch();

        // 启动心跳检测
        self.core.start_heartbeat(&self.config);

        // 准备共享资源
        let manager = Arc::clone(&self.core.connection_manager);
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        let core = Arc::clone(&self.core);
        let handshake_limiter = Arc::new(Semaphore::new(config.max_handshake_concurrency.max(1)));
        let overload_limiter = overload_reply_limiter(&config);

        tokio::spawn(async move {
            debug!("[WebSocketServer] 开始监听连接");
            // 停止接受新连接时立即退出并释放监听端口。以前循环停在 accept 上，停机后还会再接一条
            // 连接，监听端口也一直占着，直到下一条连接到来。
            while *is_running.lock().await {
                let accepted = tokio::select! {
                    biased;
                    () = accept_stopped.closed() => break,
                    accepted = listener.accept() => accepted,
                };
                match accepted {
                    Ok((stream, _addr)) => {
                        debug!("[WebSocketServer] 收到新连接");
                        let manager_clone = Arc::clone(&manager);
                        let config_clone = config.clone();
                        let core_clone = Arc::clone(&core);
                        let permit = match Arc::clone(&handshake_limiter).try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => {
                                debug!(
                                    "[WebSocketServer] 握手并发已满: {}",
                                    config.max_handshake_concurrency
                                );
                                // 回一帧 OVERLOADED 再关；回绝名额也满了就直接丢弃
                                if let Ok(reply) = Arc::clone(&overload_limiter).try_acquire_owned()
                                {
                                    let config = config.clone();
                                    tokio::spawn(async move {
                                        let _reply = reply;
                                        if let Ok(Ok(ws)) =
                                            timeout(config.handshake_timeout, accept_async(stream))
                                                .await
                                        {
                                            reject_overloaded(
                                                Box::new(WebSocketTransport::from_tcp_stream(ws)),
                                                &config,
                                            )
                                            .await;
                                        }
                                    });
                                }
                                continue;
                            }
                        };

                        tokio::spawn(async move {
                            let _permit = permit;
                            handle_websocket_connection(
                                stream,
                                manager_clone,
                                config_clone,
                                core_clone,
                            )
                            .await;
                        });
                    }
                    Err(e) => {
                        debug!("[WebSocketServer] 接受连接失败: {}", e);
                    }
                }
            }
            debug!("[WebSocketServer] 停止监听连接");
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
                    "停止服务器失败: {}",
                    e
                ))
            })
    }

    fn is_running(&self) -> bool {
        tokio::task::block_in_place(|| *self.is_running.blocking_lock())
    }
}

/// 处理 WebSocket 连接（内部函数）
async fn handle_websocket_connection(
    stream: TcpStream,
    manager: Arc<ConnectionManager>,
    config: ServerConfig,
    core: Arc<ServerCore>,
) {
    // 建立 WebSocket 连接
    let ws_stream = match timeout(config.handshake_timeout, accept_async(stream)).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => {
            debug!("[WebSocketServer] WebSocket 握手失败: {}", e);
            return;
        }
        Err(_) => {
            debug!(
                "[WebSocketServer] WebSocket 握手超时: {:?}",
                config.handshake_timeout
            );
            return;
        }
    };

    // 创建传输层连接
    let transport = WebSocketTransport::from_tcp_stream(ws_stream);
    let connection: Box<dyn Connection> = Box::new(transport);

    // 使用公共模块设置连接
    if let Err(e) = ServerConnectionHelper::setup_new_connection(
        connection,
        manager.clone(),
        &config,
        core.clone(),
    )
    .await
    {
        debug!("[WebSocketServer] 设置连接失败: {}", e);
    }
}
