//! 交错竞速里一个协议的一次尝试：把传输客户端接到 [`crate::client::race`] 上。

use super::Client;
use super::client_core::{BindingRevoke, ClientCore, LinkBinding};
use crate::client::link::AttemptFailure;
use crate::client::race::RaceAttempt;
use crate::common::error::ErrorCode;
use crate::common::link::ConnectAckInfo;
use crate::common::platform::monotonic_now;
use std::time::Duration;

/// CONNECT 没写出去时，再等多久看服务端是不是已经说明了原因：服务端过载时先回一帧 OVERLOADED
/// 紧接着就关连接，客户端写 CONNECT 可能撞上已关的连接，原因却已经在读缓冲里。
const REJECTION_GRACE: Duration = Duration::from_secs(1);

/// 一个协议的一次尝试：传输客户端、它的 core，以及胜出时要挂到它上面的上层绑定。
pub(crate) struct TransportAttempt<C> {
    client: C,
    core: ClientCore,
    binding: LinkBinding,
}

impl<C: Client + 'static> TransportAttempt<C> {
    pub(crate) fn new(client: C, core: ClientCore, binding: LinkBinding) -> Self {
        Self {
            client,
            core,
            binding,
        }
    }

    /// 胜出后取出客户端与它的 core。
    pub(crate) fn into_parts(self) -> (C, ClientCore) {
        (self.client, self.core)
    }
}

#[async_trait::async_trait]
impl<C: Client + 'static> RaceAttempt for TransportAttempt<C> {
    async fn establish(&mut self) -> Result<(), AttemptFailure> {
        self.client
            .establish()
            .await
            .map_err(|error| AttemptFailure::from_establish_error(&error))
    }

    async fn handshake(&mut self, budget: Duration) -> Result<ConnectAckInfo, AttemptFailure> {
        // 先交出上层绑定再发 CONNECT：CONNECT_ACK 一到，上层观察者就挂上，竞速返回时它们已经在了
        self.core.set_link_binding(self.binding.clone());
        let started = monotonic_now();
        let sent = self.client.connect().await;
        let remaining = budget.saturating_sub(started.elapsed());
        let wait = if sent.is_err() {
            remaining.min(REJECTION_GRACE)
        } else {
            remaining
        };
        let negotiated = self.core.wait_for_negotiation(wait).await;
        let error = match negotiated {
            Ok(()) => return Ok(self.core.connect_ack_info().unwrap_or_default()),
            Err(error) => error,
        };
        // CONNECT_ACK 恰好在时限那一刻到达、上层已经挂上：这条连接成功了
        if self.core.revoke_link_binding() == BindingRevoke::AlreadyAdopted {
            return Ok(self.core.connect_ack_info().unwrap_or_default());
        }
        if let Some(rejection) = self.core.server_rejection() {
            return Err(AttemptFailure::Rejected(rejection));
        }
        if let Err(send_error) = sent {
            return Err(AttemptFailure::Closed {
                detail: send_error.to_string(),
            });
        }
        if matches!(error.code(), Some(ErrorCode::ConnectionTimeout)) {
            Err(AttemptFailure::ConnectAckTimeout)
        } else {
            Err(AttemptFailure::Closed {
                detail: error.to_string(),
            })
        }
    }

    async fn abandon(&mut self) {
        // 收回绑定，并清掉这条连接上的观察者：即使 CONNECT_ACK 在放弃前一刻到了、上层观察者已经
        // 挂上，落败连接关闭时的事件也不会到上层
        let _ = self.core.revoke_link_binding();
        self.core.clear_observers();
        self.client.set_disconnect_requested(true);
        if let Err(error) = self.client.disconnect().await {
            tracing::debug!(%error, "protocol race: closing a losing transport failed");
        }
    }
}
