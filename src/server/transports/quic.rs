//! QUIC 服务端实现
//!
//! 专注于 QUIC 协议层面的连接处理，连接管理和心跳检测由 ServerCore 统一管理

use crate::common::error::Result;
use crate::common::shutdown::SERVER_SHUTDOWN_REASON;
use crate::server::config::ServerConfig;
use crate::server::connection::ConnectionManager;
use crate::server::transports::Server;
use crate::server::transports::common::{
    AcceptGate, ServerConnectionHelper, overload_reply_limiter, reject_overloaded,
};
use crate::server::transports::server_core::ServerCore;
use crate::transport::connection::Connection;
use crate::transport::quic::QUICTransport;
use async_trait::async_trait;
use quinn::{Endpoint, ServerConfig as QuinnServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;
use tracing::debug;

/// QUIC 服务端
///
/// 专注于 QUIC 协议层面的连接处理
pub struct QUICServer {
    config: ServerConfig,
    core: Arc<ServerCore>,
    endpoint: Option<Endpoint>,
    /// 运行中端点的句柄：停止接受新连接时拒绝新握手，停机最后关闭端点、释放 UDP 端口
    running_endpoint: Option<Endpoint>,
    is_running: Arc<Mutex<bool>>,
    /// 关闭后 accept 循环退出；每次 start 换一个新的
    accept_gate: AcceptGate,
}

impl QUICServer {
    /// 创建新的 QUIC 服务端
    pub fn new(config: ServerConfig) -> Result<Self> {
        Self::with_connection_manager(config, None)
    }

    /// 使用指定的连接管理器创建 QUIC 服务端
    pub fn with_connection_manager(
        config: ServerConfig,
        connection_manager: Option<Arc<ConnectionManager>>,
    ) -> Result<Self> {
        Self::init_rustls();

        let core = Arc::new(ServerCore::new(&config, connection_manager));
        let endpoint = Self::create_quic_endpoint(&config)?;

        Ok(Self {
            config,
            core,
            endpoint: Some(endpoint),
            running_endpoint: None,
            is_running: Arc::new(Mutex::new(false)),
            accept_gate: AcceptGate::new(),
        })
    }

    /// 使用指定的 ServerCore 创建 QUIC 服务端（用于共享 ServerCore）
    pub fn with_shared_core(config: ServerConfig, core: Arc<ServerCore>) -> Result<Self> {
        Self::init_rustls();

        let endpoint = Self::create_quic_endpoint(&config)?;

        Ok(Self {
            config,
            core,
            endpoint: Some(endpoint),
            running_endpoint: None,
            is_running: Arc::new(Mutex::new(false)),
            accept_gate: AcceptGate::new(),
        })
    }

    // ============================================================================
    // 内部辅助方法
    // ============================================================================

    /// 初始化 rustls CryptoProvider（内部辅助方法）
    fn init_rustls() {
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    /// 创建 QUIC 端点（内部辅助方法）
    fn create_quic_endpoint(config: &ServerConfig) -> Result<Endpoint> {
        use crate::common::cert::{get_server_cert_der, get_server_key_der};

        // 加载证书和私钥
        let cert_der = get_server_cert_der().map_err(|e| {
            crate::common::error::FlareError::protocol_error(format!("加载服务器证书失败: {}", e))
        })?;
        let key_der = get_server_key_der().map_err(|e| {
            crate::common::error::FlareError::protocol_error(format!("加载服务器私钥失败: {}", e))
        })?;

        debug!("[QUIC Server] 使用证书: certs/server.crt");

        // 转换证书格式
        let cert = quinn::rustls::pki_types::CertificateDer::from(cert_der);
        let certs = vec![cert];

        // 转换私钥格式
        if key_der.is_empty() {
            return Err(crate::common::error::FlareError::protocol_error(
                "私钥为空".to_string(),
            ));
        }

        let private_key = quinn::rustls::pki_types::PrivateKeyDer::Pkcs8(
            quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
        );

        // 构建服务端配置
        let server_config =
            QuinnServerConfig::with_single_cert(certs, private_key).map_err(|e| {
                crate::common::error::FlareError::protocol_error(format!(
                    "创建 QUIC 服务端配置失败: {}",
                    e
                ))
            })?;

        // 解析地址
        let bind_address =
            config.get_protocol_address(&crate::common::config_types::TransportProtocol::QUIC);
        let addr = bind_address.parse::<SocketAddr>().map_err(|e| {
            crate::common::error::FlareError::protocol_error(format!("无效地址: {}", e))
        })?;

        // 创建端点
        Endpoint::server(server_config, addr).map_err(|e| {
            crate::common::error::FlareError::connection_failed(format!(
                "创建 QUIC 端点失败: {}",
                e
            ))
        })
    }
}

#[async_trait]
impl Server for QUICServer {
    async fn start(&mut self) -> Result<()> {
        *self.is_running.lock().await = true;

        // 启动心跳检测
        self.core.start_heartbeat(&self.config);

        let endpoint = self.endpoint.take().ok_or_else(|| {
            crate::common::error::FlareError::connection_failed("端点未初始化".to_string())
        })?;
        self.running_endpoint = Some(endpoint.clone());
        self.accept_gate = AcceptGate::new();
        let mut accept_stopped = self.accept_gate.watch();

        // 准备共享资源
        let manager = Arc::clone(&self.core.connection_manager);
        let config = self.config.clone();
        let is_running = Arc::clone(&self.is_running);
        let core = Arc::clone(&self.core);
        let handshake_limiter = Arc::new(Semaphore::new(config.max_handshake_concurrency.max(1)));
        let overload_limiter = overload_reply_limiter(&config);

        tokio::spawn(async move {
            debug!("[QUIC Server] 开始监听连接");
            while *is_running.lock().await {
                let incoming = tokio::select! {
                    biased;
                    () = accept_stopped.closed() => break,
                    incoming = endpoint.accept() => incoming,
                };
                if let Some(conn) = incoming {
                    debug!("[QUIC Server] 收到新连接，等待握手");
                    let manager_clone = Arc::clone(&manager);
                    let config_clone = config.clone();
                    let core_clone = Arc::clone(&core);
                    let permit = match Arc::clone(&handshake_limiter).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            debug!(
                                "[QUIC Server] 握手并发已满: {}",
                                config.max_handshake_concurrency
                            );
                            // 回一帧 OVERLOADED 再关；回绝名额也满了就直接丢弃
                            if let Ok(reply) = Arc::clone(&overload_limiter).try_acquire_owned() {
                                let config = config.clone();
                                tokio::spawn(async move {
                                    let _reply = reply;
                                    reject_overloaded_quic(conn, &config).await;
                                });
                            }
                            continue;
                        }
                    };

                    tokio::spawn(async move {
                        let _permit = permit;
                        match timeout(config_clone.handshake_timeout, conn).await {
                            Ok(Ok(connecting)) => {
                                debug!("[QUIC Server] 握手完成，处理连接");
                                handle_quic_connection(
                                    connecting,
                                    manager_clone,
                                    config_clone,
                                    core_clone,
                                )
                                .await;
                            }
                            Ok(Err(e)) => {
                                debug!("[QUIC Server] QUIC 连接握手失败: {}", e);
                            }
                            Err(_) => {
                                debug!(
                                    "[QUIC Server] QUIC 握手超时: {:?}",
                                    config_clone.handshake_timeout
                                );
                            }
                        }
                    });
                } else {
                    debug!("[QUIC Server] 没有更多连接，停止");
                    break;
                }
            }
        });

        Ok(())
    }

    async fn stop_accepting(&mut self) {
        self.accept_gate.close();
        // 端点上的新握手一律拒绝，已建立的连接不受影响
        if let Some(endpoint) = &self.running_endpoint {
            endpoint.set_server_config(None);
        }
    }

    async fn stop(&mut self) -> Result<()> {
        self.stop_accepting().await;
        let result = ServerConnectionHelper::stop_server(&self.core, &self.is_running)
            .await
            .map_err(|e| {
                crate::common::error::FlareError::connection_failed(format!(
                    "停止服务器失败: {}",
                    e
                ))
            });
        // 在线连接都已关闭：关掉端点，释放 UDP 端口
        if let Some(endpoint) = self.running_endpoint.take() {
            endpoint.close(0u32.into(), SERVER_SHUTDOWN_REASON.as_bytes());
        }
        result
    }

    fn is_running(&self) -> bool {
        tokio::task::block_in_place(|| *self.is_running.blocking_lock())
    }
}

/// 握手闸门已满时回绝一条 QUIC 连接：完成握手、等客户端打开双向流，回一帧 OVERLOADED 再关。
/// 客户端的帧要从这条流上读，只关连接它只能看到一个没有原因的失败。
async fn reject_overloaded_quic(incoming: quinn::Incoming, config: &ServerConfig) {
    let Ok(Ok(connection)) = timeout(config.handshake_timeout, incoming).await else {
        return;
    };
    let accepted = timeout(config.handshake_timeout, connection.accept_bi()).await;
    match accepted {
        Ok(Ok((send, recv))) => {
            let transport = QUICTransport::new(send, recv).with_connection(connection);
            reject_overloaded(Box::new(transport), config).await;
        }
        _ => connection.close(0u32.into(), b"OVERLOADED"),
    }
}

/// 处理 QUIC 连接（内部函数）
///
/// 连接数上限在 [`ServerConnectionHelper::setup_new_connection`] 里检查：超限时要在双向流上回一帧
/// OVERLOADED，所以先接受双向流。
async fn handle_quic_connection(
    connection: quinn::Connection,
    manager: Arc<ConnectionManager>,
    config: ServerConfig,
    core: Arc<ServerCore>,
) {
    // 接受双向流
    debug!("[QUIC Server] 等待客户端打开双向流");
    let (send, recv) = match timeout(config.handshake_timeout, connection.accept_bi()).await {
        Ok(Ok(streams)) => {
            debug!("[QUIC Server] 双向流已接受");
            streams
        }
        Ok(Err(e)) => {
            debug!("[QUIC Server] 接受双向流失败: {}", e);
            connection.close(0u32.into(), b"stream error");
            return;
        }
        Err(_) => {
            debug!(
                "[QUIC Server] 等待双向流超时: {:?}",
                config.handshake_timeout
            );
            connection.close(0u32.into(), b"stream timeout");
            return;
        }
    };

    // 创建传输层连接；带上连接句柄，停机时能在连接层面带原因关闭
    let transport = QUICTransport::new(send, recv).with_connection(connection);
    let connection: Box<dyn Connection> = Box::new(transport);

    // 使用公共模块设置连接
    let connection_id = match ServerConnectionHelper::setup_new_connection(
        connection,
        manager.clone(),
        &config,
        core.clone(),
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            debug!("[QUIC Server] 设置连接失败: {}", e);
            return;
        }
    };

    // 注意：on_connect 将在收到 CONNECT 消息并完成协商后调用（在观察者中处理）
    debug!("[QUIC Server] 连接已设置: connection_id={}", connection_id);
}
