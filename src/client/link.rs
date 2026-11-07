//! 建连结果与断开原因：交给上层链路监督者的类型化信息。
//!
//! 上层（引擎的链路监督者）据此记熔断、判断「UDP 被封」、决定退避与是否换入口，不必再解析错误文本：
//!
//! - 建连成功：[`ConnectReport`]（胜出协议、握手耗时、CONNECT_ACK 带回的 [`ConnectAckInfo`]、
//!   胜出前失败的协议）；
//! - 建连失败：[`ConnectFailure`]，每个尝试过的协议一条 [`AttemptReport`]，失败类别见
//!   [`AttemptFailure`]；
//! - 连接结束：[`DisconnectReason`]。

use crate::common::config_types::TransportProtocol;
use crate::common::error::{ErrorCode, FlareError};
use std::fmt;
use std::time::Duration;

pub use crate::common::link::{CloseKind, ConnectAckInfo, ServerClose, ServerRejection};

/// 一个协议的一次尝试为什么失败。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttemptFailure {
    /// 传输不可达：连接被拒、连接重置、DNS 解析失败、路由不可达，或握手中途被对端断开。
    Unreachable {
        /// 传输层给出的说明（诊断用）
        detail: String,
    },
    /// TLS 握手失败：证书校验不过、SPKI 固定不符、协议或算法不匹配。
    Tls {
        /// 传输层给出的说明（诊断用）
        detail: String,
    },
    /// 传输建立（TCP/QUIC 握手、TLS、WebSocket 升级）没能在时限内完成。
    Timeout,
    /// 传输已建立、CONNECT 已发出，CONNECT_ACK 没能在时限内到达。
    ConnectAckTimeout,
    /// 传输已建立，CONNECT_ACK 之前连接被关掉且服务端没说原因。
    Closed {
        /// 传输层给出的说明（诊断用）
        detail: String,
    },
    /// 服务端明确拒绝了 CONNECT（过载、需要升级、协议版本不符、令牌问题、租户不可用……）。
    Rejected(ServerRejection),
    /// 本端不支持这个协议（特性没编进来、平台不支持）或地址无效：与入口健康无关。
    Unsupported {
        /// 说明（诊断用）
        detail: String,
    },
}

impl AttemptFailure {
    /// 是否记为这个接入点的一次失败（熔断用）。
    ///
    /// 记：不可达、TLS、传输建立超时、CONNECT_ACK 超时、CONNECT_ACK 前被无故关闭；服务端拒绝按
    /// [`ServerRejection::counts_against_access_point`]。不记：本端不支持。
    pub fn counts_against_access_point(&self) -> bool {
        match self {
            AttemptFailure::Unreachable { .. }
            | AttemptFailure::Tls { .. }
            | AttemptFailure::Timeout
            | AttemptFailure::ConnectAckTimeout
            | AttemptFailure::Closed { .. } => true,
            AttemptFailure::Rejected(rejection) => rejection.counts_against_access_point(),
            AttemptFailure::Unsupported { .. } => false,
        }
    }

    /// 服务端的拒绝（若是）。
    pub fn rejection(&self) -> Option<&ServerRejection> {
        match self {
            AttemptFailure::Rejected(rejection) => Some(rejection),
            _ => None,
        }
    }

    /// 稳定的类别名（日志、指标用）。
    pub fn label(&self) -> &'static str {
        match self {
            AttemptFailure::Unreachable { .. } => "unreachable",
            AttemptFailure::Tls { .. } => "tls",
            AttemptFailure::Timeout => "timeout",
            AttemptFailure::ConnectAckTimeout => "connect_ack_timeout",
            AttemptFailure::Closed { .. } => "closed",
            AttemptFailure::Rejected(_) => "rejected",
            AttemptFailure::Unsupported { .. } => "unsupported",
        }
    }

    /// 把传输建立阶段（不含 CONNECT）的错误归类。
    ///
    /// 各传输把底层错误压成了文本，这里按错误码与文本归类：超时码 → [`Self::Timeout`]；
    /// 证书、TLS、加密握手相关 → [`Self::Tls`]；地址与特性问题 → [`Self::Unsupported`]；
    /// 其余都是不可达。
    pub fn from_establish_error(error: &FlareError) -> Self {
        if matches!(
            error.code(),
            Some(ErrorCode::ConnectionTimeout | ErrorCode::NetworkTimeout)
        ) {
            return AttemptFailure::Timeout;
        }
        let detail = error.to_string();
        let lower = detail.to_ascii_lowercase();
        if matches!(error.code(), Some(ErrorCode::OperationNotSupported))
            || lower.contains("feature is disabled")
            || lower.contains("url error")
            || lower.contains("invalid address")
            || lower.contains("must start with")
        {
            return AttemptFailure::Unsupported { detail };
        }
        const TLS_MARKERS: &[&str] = &[
            "tls",
            "ssl",
            "certificate",
            "cryptographic handshake",
            "crypto",
            "pin mismatch",
            "handshake failure",
            "alert",
        ];
        if TLS_MARKERS.iter().any(|marker| lower.contains(marker)) {
            return AttemptFailure::Tls { detail };
        }
        AttemptFailure::Unreachable { detail }
    }
}

impl fmt::Display for AttemptFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttemptFailure::Unreachable { detail } => write!(f, "unreachable: {detail}"),
            AttemptFailure::Tls { detail } => write!(f, "tls: {detail}"),
            AttemptFailure::Timeout => f.write_str("transport handshake timed out"),
            AttemptFailure::ConnectAckTimeout => f.write_str("CONNECT_ACK timed out"),
            AttemptFailure::Closed { detail } => {
                write!(f, "closed before CONNECT_ACK: {detail}")
            }
            AttemptFailure::Rejected(rejection) => write!(
                f,
                "rejected by server: {} ({})",
                rejection.reason,
                rejection.code.as_str()
            ),
            AttemptFailure::Unsupported { detail } => write!(f, "unsupported: {detail}"),
        }
    }
}

/// 一个协议的一次失败尝试。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttemptReport {
    /// 协议
    pub protocol: TransportProtocol,
    /// 这次尝试花掉的时间：传输建立加 CONNECT 握手，不含排队等 CONNECT 名额的时间
    pub elapsed: Duration,
    /// 失败类别
    pub failure: AttemptFailure,
}

/// 建连成功的结果。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConnectReport {
    /// 胜出的协议
    pub protocol: TransportProtocol,
    /// 胜出协议的握手耗时：传输建立（含 TLS）加 CONNECT → CONNECT_ACK，不含排队等 CONNECT 名额的时间
    pub handshake: Duration,
    /// 整个竞速从开始到胜出的耗时
    pub elapsed: Duration,
    /// CONNECT_ACK 带回的接入信息
    pub ack: ConnectAckInfo,
    /// 胜出前已经失败的协议，按失败先后排列
    pub failed: Vec<AttemptReport>,
    /// 胜出时还没有结果、被取消的协议（不算失败，不该记熔断）
    pub cancelled: Vec<CancelledAttempt>,
}

/// 胜出时被取消的一次尝试。
///
/// 它不是失败，不该记熔断；但「跑了一个交错间隔以上、传输还没建好，而别的协议已经连上」是判断
/// UDP 被封的依据（QUIC 被静默丢弃时，它总是以这种方式被取消，等不到自己的超时）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CancelledAttempt {
    /// 协议
    pub protocol: TransportProtocol,
    /// 从发起到被取消跑了多久
    pub ran_for: Duration,
    /// 被取消时传输是否已经建好（建好了在排队等 CONNECT 名额）
    pub transport_ready: bool,
}

/// 建连失败的结果。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConnectFailure {
    /// 每个尝试过的协议一条，按失败先后排列
    pub attempts: Vec<AttemptReport>,
    /// 服务端明确拒绝时还没开始或被取消的协议：拒绝与传输无关，换协议只会被再拒一次
    pub skipped: Vec<TransportProtocol>,
    /// 整个竞速的耗时
    pub elapsed: Duration,
}

impl ConnectFailure {
    /// 服务端的拒绝（若有）。竞速里任一协议被服务端明确拒绝时立即结束，它就是最后一条尝试。
    pub fn rejection(&self) -> Option<&ServerRejection> {
        self.attempts
            .iter()
            .rev()
            .find_map(|attempt| attempt.failure.rejection())
    }

    /// 某个协议的失败类别。
    pub fn failure_of(&self, protocol: TransportProtocol) -> Option<&AttemptFailure> {
        self.attempts
            .iter()
            .find(|attempt| attempt.protocol == protocol)
            .map(|attempt| &attempt.failure)
    }

    /// 转成本地化错误：有服务端拒绝时用拒绝的类别（附加字段进 `params`）；全部超时为
    /// `CONNECTION_TIMEOUT`；其余为 `CONNECTION_FAILED`。文本列出每个协议的失败。
    pub fn to_error(&self) -> FlareError {
        if let Some(rejection) = self.rejection() {
            return rejection.to_error();
        }
        let all_timed_out = !self.attempts.is_empty()
            && self.attempts.iter().all(|attempt| {
                matches!(
                    attempt.failure,
                    AttemptFailure::Timeout | AttemptFailure::ConnectAckTimeout
                )
            });
        let code = if all_timed_out {
            ErrorCode::ConnectionTimeout
        } else {
            ErrorCode::ConnectionFailed
        };
        FlareError::localized(code, self.to_string())
    }
}

impl fmt::Display for ConnectFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.attempts.is_empty() {
            return f.write_str("nothing was attempted: no protocol to try or no listener set");
        }
        f.write_str("every protocol failed to connect: ")?;
        for (index, attempt) in self.attempts.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(
                f,
                "{:?} after {:?}: {}",
                attempt.protocol, attempt.elapsed, attempt.failure
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for ConnectFailure {}

impl From<ConnectFailure> for FlareError {
    fn from(failure: ConnectFailure) -> Self {
        failure.to_error()
    }
}

/// 一条已协商完成的连接为什么结束。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DisconnectReason {
    /// 我方主动断开（上层调用了 `disconnect`）。
    ClientRequested,
    /// 服务端计划性关闭（排空、停机、再均衡），带建议的重连窗口与优先接入点；不计熔断。
    ServerClose(ServerClose),
    /// 被服务端踢下线。
    Kicked {
        /// KICKED 的类型化原因（如 `DEVICE_CONFLICT`、`TENANT_UNAVAILABLE`）
        reason: String,
        /// 这次会话是否已结束（见 [`crate::common::kick::ends_session`]）：结束时不要自动重连
        ends_session: bool,
    },
    /// 心跳判死：周期心跳或一次验活在时限内没收到 PONG。
    HeartbeatTimeout,
    /// 连接被对端或网络关闭，没有协议层的原因。
    PeerClosed {
        /// 传输层给出的说明（诊断用）
        detail: String,
    },
    /// 读写出错（含心跳发不出去）。
    TransportError {
        /// 传输层给出的说明（诊断用）
        detail: String,
    },
}

impl DisconnectReason {
    /// 是否记为这个接入点的一次失败（熔断用）：心跳判死、对端关闭、传输错误记；主动断开、
    /// 计划性关闭、被踢不记。
    pub fn counts_against_access_point(&self) -> bool {
        matches!(
            self,
            DisconnectReason::HeartbeatTimeout
                | DisconnectReason::PeerClosed { .. }
                | DisconnectReason::TransportError { .. }
        )
    }

    /// 服务端计划性关闭（若是）。
    pub fn server_close(&self) -> Option<&ServerClose> {
        match self {
            DisconnectReason::ServerClose(close) => Some(close),
            _ => None,
        }
    }

    /// 由传输层报的断开原因文本归类：心跳判死、停机等计划性关闭的原因认得出来，其余为对端关闭。
    /// 只在拿不到连接上记下的类型化原因时用。
    pub fn from_detail(detail: &str) -> Self {
        let trimmed = detail.trim();
        if trimmed == crate::client::heartbeat::manager::HEARTBEAT_TIMEOUT_REASON {
            return DisconnectReason::HeartbeatTimeout;
        }
        if trimmed == crate::client::heartbeat::manager::HEARTBEAT_SEND_FAILED_REASON {
            return DisconnectReason::TransportError {
                detail: trimmed.to_string(),
            };
        }
        let token = trimmed.split(':').next().unwrap_or_default();
        if let Some(kind) = CloseKind::parse(token) {
            return DisconnectReason::ServerClose(ServerClose::new(kind));
        }
        DisconnectReason::PeerClosed {
            detail: trimmed.to_string(),
        }
    }
}

/// 文本形式与此前观察者收到的断开原因一致：被踢是类型化原因本身，计划性关闭是
/// `<原因>: server closed the connection`，心跳判死是 [`HEARTBEAT_TIMEOUT_REASON`]，主动断开是
/// `Client disconnected`，其余是传输层的说明。
///
/// [`HEARTBEAT_TIMEOUT_REASON`]: crate::client::heartbeat::manager::HEARTBEAT_TIMEOUT_REASON
impl fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DisconnectReason::ClientRequested => f.write_str(CLIENT_DISCONNECTED_REASON),
            DisconnectReason::ServerClose(close) => {
                write!(f, "{}: server closed the connection", close.kind.as_str())
            }
            DisconnectReason::Kicked { reason, .. } => f.write_str(reason),
            DisconnectReason::HeartbeatTimeout => {
                f.write_str(crate::client::heartbeat::manager::HEARTBEAT_TIMEOUT_REASON)
            }
            DisconnectReason::PeerClosed { detail }
            | DisconnectReason::TransportError { detail } => f.write_str(detail),
        }
    }
}

/// 我方主动断开时报给观察者的原因文本。
pub const CLIENT_DISCONNECTED_REASON: &str = "Client disconnected";

/// 一条连接上记下的断开原因，上层在收到断开事件时读取。
pub type DisconnectReasonCell = std::sync::Arc<std::sync::Mutex<Option<DisconnectReason>>>;

#[cfg(test)]
mod tests {
    use super::*;

    /// 建连阶段的错误按码与文本归类。
    #[test]
    fn establish_errors_are_classified() {
        assert_eq!(
            AttemptFailure::from_establish_error(&FlareError::connection_timeout(
                "Connection timeout"
            )),
            AttemptFailure::Timeout
        );
        assert!(matches!(
            AttemptFailure::from_establish_error(&FlareError::connection_failed(
                "IO error: invalid peer certificate: UnknownIssuer"
            )),
            AttemptFailure::Tls { .. }
        ));
        assert!(matches!(
            AttemptFailure::from_establish_error(&FlareError::connection_failed(
                "the cryptographic handshake failed: error 42"
            )),
            AttemptFailure::Tls { .. }
        ));
        assert!(matches!(
            AttemptFailure::from_establish_error(&FlareError::connection_failed(
                "IO error: Connection refused (os error 61)"
            )),
            AttemptFailure::Unreachable { .. }
        ));
        assert!(matches!(
            AttemptFailure::from_establish_error(&FlareError::operation_not_supported(
                "QUIC transport feature is disabled"
            )),
            AttemptFailure::Unsupported { .. }
        ));
    }

    /// 全部失败时的错误：有拒绝用拒绝的类别，全部超时为连接超时，其余为连接失败。
    #[test]
    fn connect_failure_maps_to_the_most_specific_error() {
        let report = |protocol, failure| AttemptReport {
            protocol,
            elapsed: Duration::from_millis(5),
            failure,
        };
        let timed_out = ConnectFailure {
            attempts: vec![
                report(TransportProtocol::QUIC, AttemptFailure::Timeout),
                report(
                    TransportProtocol::WebSocket,
                    AttemptFailure::ConnectAckTimeout,
                ),
            ],
            skipped: Vec::new(),
            elapsed: Duration::from_secs(8),
        };
        assert_eq!(
            timed_out.to_error().code(),
            Some(ErrorCode::ConnectionTimeout)
        );
        assert_eq!(timed_out.rejection(), None);

        let rejected = ConnectFailure {
            attempts: vec![
                report(
                    TransportProtocol::QUIC,
                    AttemptFailure::Unreachable {
                        detail: "refused".to_string(),
                    },
                ),
                report(
                    TransportProtocol::WebSocket,
                    AttemptFailure::Rejected(ServerRejection::classify("TENANT_UNAVAILABLE", None)),
                ),
            ],
            skipped: Vec::new(),
            elapsed: Duration::from_millis(40),
        };
        let error = FlareError::from(rejected.clone());
        assert_eq!(error.code(), Some(ErrorCode::PermissionDenied));
        assert!(error.to_string().contains("TENANT_UNAVAILABLE"), "{error}");
        assert!(matches!(
            rejected.failure_of(TransportProtocol::QUIC),
            Some(AttemptFailure::Unreachable { .. })
        ));
    }

    /// 断开原因的文本与此前上报的文本一致；按文本归类能认出心跳判死与计划性关闭。
    #[test]
    fn disconnect_reasons_keep_their_wire_text() {
        assert_eq!(
            DisconnectReason::Kicked {
                reason: "DEVICE_CONFLICT".to_string(),
                ends_session: true,
            }
            .to_string(),
            "DEVICE_CONFLICT"
        );
        let shutdown = DisconnectReason::ServerClose(ServerClose::new(CloseKind::ServerShutdown));
        assert_eq!(
            shutdown.to_string(),
            "SERVER_SHUTDOWN: server closed the connection"
        );
        assert_eq!(
            DisconnectReason::from_detail(&shutdown.to_string()),
            shutdown
        );
        assert_eq!(
            DisconnectReason::from_detail(
                crate::client::heartbeat::manager::HEARTBEAT_TIMEOUT_REASON
            ),
            DisconnectReason::HeartbeatTimeout
        );
        assert_eq!(
            DisconnectReason::ClientRequested.to_string(),
            CLIENT_DISCONNECTED_REASON
        );
        assert!(DisconnectReason::HeartbeatTimeout.counts_against_access_point());
        assert!(!shutdown.counts_against_access_point());
    }
}
