//! 客户端传输协议模块
//!
//! 提供多种传输协议的客户端实现：
//! - QUIC：基于 QUIC 协议的客户端（native）
//! - WebSocket：基于 WebSocket 协议的客户端
//! - Hybrid：混合客户端，支持多种协议（native）

use crate::common::error::Result;
use crate::common::protocol::Frame;
use crate::transport::events::ArcObserver;
use async_trait::async_trait;

/// 客户端标准接口
///
/// 连接只建一次、不自己重连：断开后由上层决定何时、向哪里重新建连（新建客户端）。
#[async_trait]
pub trait Client: Send + Sync {
    /// 建连并发 CONNECT（已经 [`Self::establish`] 过的复用那条传输）。不等 CONNECT_ACK。
    async fn connect(&mut self) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    async fn send_frame(&mut self, frame: &Frame) -> Result<()>;
    fn is_connected(&self) -> bool;
    fn add_observer(&mut self, observer: ArcObserver);
    fn remove_observer(&mut self, observer: ArcObserver);
    fn connection_id(&self) -> Option<String> {
        None
    }
    fn set_disconnect_requested(&mut self, _value: bool) {}
    /// 只建立传输（TCP/QUIC 握手、TLS、WebSocket 升级），不发 CONNECT；随后的 [`Self::connect`]
    /// 在这条传输上发 CONNECT。交错竞速用它把「传输建好」与「发 CONNECT」分开，保证同一时刻只有
    /// 一条连接在发 CONNECT。默认不支持。
    async fn establish(&mut self) -> Result<()> {
        Err(crate::common::error::FlareError::operation_not_supported(
            "this client cannot establish a transport without sending CONNECT",
        ))
    }
}

/// 竞速里各协议的客户端以 `Box<dyn Client>` 持有，转发到里面的实现。
#[async_trait]
impl Client for Box<dyn Client> {
    async fn connect(&mut self) -> Result<()> {
        (**self).connect().await
    }

    async fn disconnect(&mut self) -> Result<()> {
        (**self).disconnect().await
    }

    async fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        (**self).send_frame(frame).await
    }

    fn is_connected(&self) -> bool {
        (**self).is_connected()
    }

    fn add_observer(&mut self, observer: ArcObserver) {
        (**self).add_observer(observer);
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        (**self).remove_observer(observer);
    }

    fn connection_id(&self) -> Option<String> {
        (**self).connection_id()
    }

    fn set_disconnect_requested(&mut self, value: bool) {
        (**self).set_disconnect_requested(value);
    }

    async fn establish(&mut self) -> Result<()> {
        (**self).establish().await
    }
}

pub(crate) mod attempt;
pub mod client_core;
mod common;
#[cfg(feature = "websocket")]
pub mod websocket;

#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub mod hybrid;
#[cfg(all(not(target_arch = "wasm32"), feature = "quic"))]
pub mod quic;
#[cfg(all(not(target_arch = "wasm32"), feature = "tcp"))]
pub mod tcp;

pub use client_core::ClientCore;
#[cfg(feature = "websocket")]
pub use websocket::WebSocketClient;

#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "websocket", feature = "quic", feature = "tcp")
))]
pub use hybrid::HybridClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "quic"))]
pub use quic::QUICClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "tcp"))]
pub use tcp::TCPClient;
