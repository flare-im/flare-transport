//! 客户端链路与服务端之间的连接级约定。
//!
//! - [`ConnectAckInfo`]：CONNECT_ACK 带回的接入信息（服务端时间、清单 serial、心跳建议、网关与接入点）；
//! - [`ServerClose`]：服务端计划性关闭连接（`DRAIN` / `SERVER_SHUTDOWN` / `REBALANCE`）时的原因、
//!   重连窗口与优先接入点；
//! - [`ServerRejection`]：服务端拒绝 CONNECT 的类型化原因，含过载（`OVERLOADED`）与需要升级
//!   （`UPGRADE_REQUIRED`）。
//!
//! 字段都放在 `SystemCommand.metadata` 里（键见 [`metadata_keys`]），数字一律是十进制 ASCII：
//! 不改帧结构，认不得这些键的一端照常工作。客户端与服务端共用这一份编解码，两边不会各写一套。

use crate::common::constants::metadata_keys;
use crate::common::error::{ErrorCode, FlareError};
use crate::common::kick::TENANT_UNAVAILABLE_REASON;
use crate::common::protocol::SystemCommand;
use crate::common::protocol::builder::{close, error};
use crate::common::protocol::flare::core::commands::system_command::Type as SystemType;
use std::collections::HashMap;
use std::time::Duration;

/// CONNECT_ACK 里服务端给出的接入信息。
///
/// 每个字段都可能缺省：旧服务端不发，服务端没配置时也不发。客户端按「有就用」处理。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConnectAckInfo {
    /// 服务端当前时间（Unix 毫秒）。客户端据此修正本机时钟偏差，用于令牌提前续期与清单过期判断。
    pub server_time_ms: Option<u64>,
    /// 服务端已知的最新接入清单 serial；比客户端的新时，客户端应刷新清单。`0` 表示服务端还没有清单。
    pub access_serial: Option<u64>,
    /// 服务端建议的心跳间隔（秒）。
    pub heartbeat_interval_hint_sec: Option<u32>,
    /// 处理这条连接的网关实例 ID（诊断用）。
    pub gateway_id: Option<String>,
    /// 这条连接所在的接入点 ID（诊断用）。
    pub ap_id: Option<String>,
}

impl ConnectAckInfo {
    /// 从 CONNECT_ACK 的 metadata 读取。格式不对的字段按缺省处理，不让一个坏字段拖垮建连。
    pub fn from_metadata(metadata: &HashMap<String, Vec<u8>>) -> Self {
        Self {
            server_time_ms: metadata_u64(metadata, metadata_keys::SERVER_TIME_MS),
            access_serial: metadata_u64(metadata, metadata_keys::ACCESS_SERIAL),
            heartbeat_interval_hint_sec: metadata_u64(
                metadata,
                metadata_keys::HEARTBEAT_INTERVAL_HINT_SEC,
            )
            .and_then(|secs| u32::try_from(secs).ok()),
            gateway_id: metadata_text(metadata, metadata_keys::GATEWAY_ID),
            ap_id: metadata_text(metadata, metadata_keys::AP_ID),
        }
    }

    /// 写进 CONNECT_ACK 的 metadata；`None` 的字段不写。
    pub fn write_metadata(&self, metadata: &mut HashMap<String, Vec<u8>>) {
        put_u64(metadata, metadata_keys::SERVER_TIME_MS, self.server_time_ms);
        put_u64(metadata, metadata_keys::ACCESS_SERIAL, self.access_serial);
        put_u64(
            metadata,
            metadata_keys::HEARTBEAT_INTERVAL_HINT_SEC,
            self.heartbeat_interval_hint_sec.map(u64::from),
        );
        put_text(
            metadata,
            metadata_keys::GATEWAY_ID,
            self.gateway_id.as_deref(),
        );
        put_text(metadata, metadata_keys::AP_ID, self.ap_id.as_deref());
    }
}

/// 服务端计划性关闭连接的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CloseKind {
    /// 计划下线（排空）：这个网关或接入点即将停止服务。
    Drain,
    /// 服务端停机。
    ServerShutdown,
    /// 负载再均衡：服务端请客户端换到别处，本身没有故障。
    Rebalance,
}

impl CloseKind {
    /// 线上的类型化原因（CLOSE 的 `message` 与 metadata `reason`）。
    pub const fn as_str(self) -> &'static str {
        match self {
            CloseKind::Drain => "DRAIN",
            CloseKind::ServerShutdown => crate::common::shutdown::SERVER_SHUTDOWN_REASON,
            CloseKind::Rebalance => "REBALANCE",
        }
    }

    /// CLOSE 的 metadata `code`：排空与停机是 `SERVICE_UNAVAILABLE`，再均衡是 `OK`（不是故障）。
    pub const fn code(self) -> &'static str {
        match self {
            CloseKind::Drain | CloseKind::ServerShutdown => "SERVICE_UNAVAILABLE",
            CloseKind::Rebalance => "OK",
        }
    }

    /// 按类型化原因识别（去掉首尾空白，大小写敏感）。
    pub fn parse(reason: &str) -> Option<Self> {
        match reason.trim() {
            "DRAIN" => Some(CloseKind::Drain),
            crate::common::shutdown::SERVER_SHUTDOWN_REASON => Some(CloseKind::ServerShutdown),
            "REBALANCE" => Some(CloseKind::Rebalance),
            _ => None,
        }
    }
}

/// 服务端计划性关闭连接：原因、建议的重连窗口与优先接入点。
///
/// 客户端在窗口内随机挑一个时刻重连（有 `prefer_ap` 时优先其中的接入点），不计入熔断。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServerClose {
    /// 关闭原因。
    pub kind: CloseKind,
    /// 建议重连窗口的下界（毫秒）；服务端没给时为 `None`。
    pub retry_after_min_ms: Option<u64>,
    /// 建议重连窗口的上界（毫秒）；服务端没给时为 `None`。
    pub retry_after_max_ms: Option<u64>,
    /// 重连时优先的接入点 ID，按服务端给出的顺序；没给时为空。
    pub prefer_ap: Vec<String>,
}

impl ServerClose {
    /// 只带原因、没有窗口与优先接入点的关闭。
    pub fn new(kind: CloseKind) -> Self {
        Self {
            kind,
            retry_after_min_ms: None,
            retry_after_max_ms: None,
            prefer_ap: Vec::new(),
        }
    }

    /// 带上建议的重连窗口。
    #[must_use]
    pub fn with_retry_window(mut self, min: Duration, max: Duration) -> Self {
        self.retry_after_min_ms = Some(duration_ms(min));
        self.retry_after_max_ms = Some(duration_ms(max));
        self
    }

    /// 带上优先接入点。
    #[must_use]
    pub fn with_prefer_ap<I, S>(mut self, access_points: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.prefer_ap = access_points.into_iter().map(Into::into).collect();
        self
    }

    /// 建议的重连窗口 `(min, max)`，保证 `min <= max`。只给了一端时两端相同；都没给时为 `None`。
    pub fn retry_window(&self) -> Option<(Duration, Duration)> {
        let (min, max) = match (self.retry_after_min_ms, self.retry_after_max_ms) {
            (Some(min), Some(max)) => (min.min(max), min.max(max)),
            (Some(one), None) | (None, Some(one)) => (one, one),
            (None, None) => return None,
        };
        Some((Duration::from_millis(min), Duration::from_millis(max)))
    }

    /// 从 CLOSE 系统命令读取。原因取 `message`，没有时取 metadata `reason`；不是认得的计划性原因
    /// （或不是 CLOSE）时返回 `None`。
    pub fn from_command(command: &SystemCommand) -> Option<Self> {
        if command.r#type != SystemType::Close as i32 {
            return None;
        }
        let kind = CloseKind::parse(&command.message).or_else(|| {
            metadata_text(&command.metadata, metadata_keys::REASON)
                .as_deref()
                .and_then(CloseKind::parse)
        })?;
        Some(Self {
            kind,
            retry_after_min_ms: metadata_u64(&command.metadata, metadata_keys::RETRY_AFTER_MIN_MS),
            retry_after_max_ms: metadata_u64(&command.metadata, metadata_keys::RETRY_AFTER_MAX_MS),
            prefer_ap: metadata_text(&command.metadata, metadata_keys::PREFER_AP)
                .map(|list| {
                    list.split(',')
                        .map(str::trim)
                        .filter(|ap| !ap.is_empty())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
        })
    }

    /// 编成 CLOSE 系统命令：`message` 与 metadata `reason` 都是类型化原因，metadata `code` 见
    /// [`CloseKind::code`]，窗口与优先接入点有值才写。
    pub fn to_command(&self) -> SystemCommand {
        let mut metadata = HashMap::from([
            (
                metadata_keys::REASON.to_string(),
                self.kind.as_str().as_bytes().to_vec(),
            ),
            (
                metadata_keys::CODE.to_string(),
                self.kind.code().as_bytes().to_vec(),
            ),
        ]);
        put_u64(
            &mut metadata,
            metadata_keys::RETRY_AFTER_MIN_MS,
            self.retry_after_min_ms,
        );
        put_u64(
            &mut metadata,
            metadata_keys::RETRY_AFTER_MAX_MS,
            self.retry_after_max_ms,
        );
        if !self.prefer_ap.is_empty() {
            put_text(
                &mut metadata,
                metadata_keys::PREFER_AP,
                Some(&self.prefer_ap.join(",")),
            );
        }
        close(Some(self.kind.as_str().to_string()), Some(metadata))
    }
}

/// 服务端拒绝 CONNECT 的类型化原因（CONNECT_ACK 之前收到的 ERROR，或 CLOSE）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServerRejection {
    /// 拒绝类别。
    ///
    /// - `OVERLOADED`：过载，等 [`Self::retry_after_ms`] 后重试同一入口；
    /// - `UPGRADE_REQUIRED`：需要升级（带 [`Self::min_build`]、[`Self::release_hint`]），终止；
    /// - `PROTOCOL_VERSION_MISMATCH`：协议版本不被支持，终止；
    /// - `PERMISSION_DENIED`：租户不可用（原因 `TENANT_UNAVAILABLE`），终止；
    /// - `AUTHENTICATION_FAILED` / `TOKEN_*`：令牌问题，续期后可重连；
    /// - `SERVICE_UNAVAILABLE`：服务端暂时放不了行（含握手期间收到的停机、排空 CLOSE），退避重试；
    /// - 认不出时为 `CONNECTION_REFUSED`。
    pub code: ErrorCode,
    /// 服务端给出的类型化原因（如 `TENANT_UNAVAILABLE`、`DRAIN`）；只给了类别时与类别相同。
    pub reason: String,
    /// `OVERLOADED` 时建议等多久（毫秒）。
    pub retry_after_ms: Option<u64>,
    /// `UPGRADE_REQUIRED` 时服务端要求的最低构建号。
    pub min_build: Option<u64>,
    /// `UPGRADE_REQUIRED` 时给用户看的升级说明或下载入口。
    pub release_hint: Option<String>,
}

impl ServerRejection {
    /// 从拒绝 CONNECT 的 ERROR / CLOSE 读取。
    ///
    /// 类别按顺序取：原因是 `TENANT_UNAVAILABLE` 时为 `PERMISSION_DENIED`；原因本身是错误码时用它；
    /// 否则用 metadata `code`；都认不出为 `CONNECTION_REFUSED`。
    pub fn from_command(command: &SystemCommand) -> Self {
        let message = command.message.trim();
        let metadata_reason = metadata_text(&command.metadata, metadata_keys::REASON);
        let reason = if message.is_empty() {
            metadata_reason.as_deref().unwrap_or("").trim()
        } else {
            message
        };
        let code_text = metadata_text(&command.metadata, metadata_keys::CODE);
        Self::classify(reason, code_text.as_deref()).with_details_from(&command.metadata)
    }

    /// 按原因与错误码文本归类（见 [`Self::from_command`]）。
    pub fn classify(reason: &str, code: Option<&str>) -> Self {
        let reason = reason.trim();
        let code = code.map(str::trim).filter(|code| !code.is_empty());
        let error_code = if reason == TENANT_UNAVAILABLE_REASON {
            ErrorCode::PermissionDenied
        } else {
            parse_error_code(reason)
                .or_else(|| code.and_then(parse_error_code))
                .unwrap_or(ErrorCode::ConnectionRefused)
        };
        let reason = if reason.is_empty() {
            code.unwrap_or(error_code.as_str()).to_string()
        } else {
            reason.to_string()
        };
        Self {
            code: error_code,
            reason,
            retry_after_ms: None,
            min_build: None,
            release_hint: None,
        }
    }

    fn with_details_from(mut self, metadata: &HashMap<String, Vec<u8>>) -> Self {
        self.retry_after_ms = metadata_u64(metadata, metadata_keys::RETRY_AFTER_MS);
        self.min_build = metadata_u64(metadata, metadata_keys::MIN_BUILD);
        self.release_hint = metadata_text(metadata, metadata_keys::RELEASE_HINT);
        self
    }

    /// 服务端过载：同一入口等 [`Self::retry_after_ms`] 后重试，不换入口、不计熔断。
    pub fn is_overloaded(&self) -> bool {
        self.code == ErrorCode::Overloaded
    }

    /// 只能由用户动作恢复（升级、联系管理员）：自动重连只会被再拒一次。
    pub fn is_terminal(&self) -> bool {
        self.code.is_terminal_connect_rejection()
    }

    /// 是否该记为这个接入点的一次失败（熔断用）。
    ///
    /// 不记：过载（服务端明确让客户端原地等）、令牌问题（与入口无关）、终止类拒绝、握手期间收到的
    /// 计划性关闭（排空、停机、再均衡）。服务端暂时放不了行（`SERVICE_UNAVAILABLE`，如这台网关读不到
    /// 租户状态）与认不出的拒绝记为失败：换个入口可能就好了。
    pub fn counts_against_access_point(&self) -> bool {
        if self.is_overloaded() || self.is_terminal() || CloseKind::parse(&self.reason).is_some() {
            return false;
        }
        !matches!(
            self.code,
            ErrorCode::AuthenticationFailed
                | ErrorCode::AuthenticationExpired
                | ErrorCode::AuthenticationInvalid
                | ErrorCode::AuthenticationRequired
                | ErrorCode::TokenInvalid
                | ErrorCode::TokenExpired
        )
    }

    /// 转成本地化错误：文本以类型化原因开头（`<原因>: server rejected CONNECT`），附加字段进 `params`。
    pub fn to_error(&self) -> FlareError {
        let mut params = HashMap::new();
        if let Some(retry_after_ms) = self.retry_after_ms {
            params.insert(
                metadata_keys::RETRY_AFTER_MS.to_string(),
                retry_after_ms.to_string(),
            );
        }
        if let Some(min_build) = self.min_build {
            params.insert(metadata_keys::MIN_BUILD.to_string(), min_build.to_string());
        }
        if let Some(hint) = &self.release_hint {
            params.insert(metadata_keys::RELEASE_HINT.to_string(), hint.clone());
        }
        FlareError::Localized {
            code: self.code,
            reason: format!("{}: server rejected CONNECT", self.reason),
            details: None,
            params: (!params.is_empty()).then_some(params),
            timestamp: chrono::Utc::now(),
        }
    }
}

/// 拒绝 CONNECT 的 ERROR 系统命令：`message` 是类型化原因，metadata `code` 是类别，`extra` 附加字段。
pub fn rejection_command(
    code: ErrorCode,
    reason: &str,
    extra: HashMap<String, Vec<u8>>,
) -> SystemCommand {
    let mut metadata = extra;
    metadata.insert(
        metadata_keys::CODE.to_string(),
        code.as_str().as_bytes().to_vec(),
    );
    error(reason.to_string(), Some(metadata))
}

/// 过载拒绝：`OVERLOADED`，带 `retry_after_ms`。
pub fn overloaded_command(retry_after: Duration) -> SystemCommand {
    let mut extra = HashMap::new();
    put_u64(
        &mut extra,
        metadata_keys::RETRY_AFTER_MS,
        Some(duration_ms(retry_after)),
    );
    rejection_command(ErrorCode::Overloaded, ErrorCode::Overloaded.as_str(), extra)
}

/// 需要升级：`UPGRADE_REQUIRED`，带 `min_build` 与可选的 `release_hint`。
pub fn upgrade_required_command(min_build: u64, release_hint: Option<&str>) -> SystemCommand {
    let mut extra = HashMap::new();
    put_u64(&mut extra, metadata_keys::MIN_BUILD, Some(min_build));
    put_text(&mut extra, metadata_keys::RELEASE_HINT, release_hint);
    rejection_command(
        ErrorCode::UpgradeRequired,
        ErrorCode::UpgradeRequired.as_str(),
        extra,
    )
}

/// 协议版本不被支持：`PROTOCOL_VERSION_MISMATCH`（3001）。
pub fn protocol_version_mismatch_command() -> SystemCommand {
    rejection_command(
        ErrorCode::ProtocolVersionMismatch,
        ErrorCode::ProtocolVersionMismatch.as_str(),
        HashMap::new(),
    )
}

/// 过载拒绝里给客户端的等待：`base` 上加 0–50% 的随机抖动（按毫秒取值），
/// 同一刻被拒的一批客户端不会在同一时刻再撞回来。
pub fn overload_retry_after(base: Duration) -> Duration {
    use rand::Rng;
    let base_ms = duration_ms(base);
    let jitter_max = base_ms / 2;
    let jitter = if jitter_max == 0 {
        0
    } else {
        rand::thread_rng().gen_range(0..=jitter_max)
    };
    Duration::from_millis(base_ms.saturating_add(jitter))
}

fn parse_error_code(value: &str) -> Option<ErrorCode> {
    serde_json::from_value(serde_json::Value::String(value.to_string())).ok()
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn metadata_text(metadata: &HashMap<String, Vec<u8>>, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| std::str::from_utf8(value).ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn metadata_u64(metadata: &HashMap<String, Vec<u8>>, key: &str) -> Option<u64> {
    metadata_text(metadata, key).and_then(|value| value.parse().ok())
}

fn put_u64(metadata: &mut HashMap<String, Vec<u8>>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        metadata.insert(key.to_string(), value.to_string().into_bytes());
    }
}

fn put_text(metadata: &mut HashMap<String, Vec<u8>>, key: &str, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        metadata.insert(key.to_string(), value.as_bytes().to_vec());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::builder::{connect_ack, kicked};
    use crate::common::protocol::{Reliability, SerializationFormat, frame_with_system_command};

    /// CONNECT_ACK 的接入信息经 metadata 往返不丢字段；没给的字段不写也读不出来。
    #[test]
    fn connect_ack_info_round_trips_through_the_ack_frame() {
        let info = ConnectAckInfo {
            server_time_ms: Some(1_762_000_000_123),
            access_serial: Some(42),
            heartbeat_interval_hint_sec: Some(30),
            gateway_id: Some("gw-bj-1".to_string()),
            ap_id: Some("ap-bj-a".to_string()),
        };
        let mut metadata = HashMap::new();
        info.write_metadata(&mut metadata);
        let ack = connect_ack(SerializationFormat::Json, None, None, metadata);
        let frame = frame_with_system_command(ack, Reliability::AtLeastOnce);
        let parser = &*crate::common::message::parser::PRE_NEGOTIATION_PARSER;
        let decoded = parser
            .parse(&parser.serialize(&frame).expect("serialize"))
            .expect("parse");
        let Some(crate::common::protocol::command::Type::System(command)) =
            decoded.command.and_then(|c| c.r#type)
        else {
            panic!("system command expected");
        };
        assert_eq!(ConnectAckInfo::from_metadata(&command.metadata), info);

        let mut empty = HashMap::new();
        ConnectAckInfo::default().write_metadata(&mut empty);
        assert!(empty.is_empty());
        assert_eq!(
            ConnectAckInfo::from_metadata(&empty),
            ConnectAckInfo::default()
        );
    }

    /// 格式不对的字段按缺省处理。
    #[test]
    fn malformed_ack_fields_read_as_absent() {
        let metadata = HashMap::from([
            (
                metadata_keys::SERVER_TIME_MS.to_string(),
                b"not-a-number".to_vec(),
            ),
            (
                metadata_keys::HEARTBEAT_INTERVAL_HINT_SEC.to_string(),
                u64::MAX.to_string().into_bytes(),
            ),
            (metadata_keys::GATEWAY_ID.to_string(), b"  ".to_vec()),
            (metadata_keys::ACCESS_SERIAL.to_string(), b" 7 ".to_vec()),
        ]);
        let info = ConnectAckInfo::from_metadata(&metadata);
        assert_eq!(info.server_time_ms, None);
        assert_eq!(info.heartbeat_interval_hint_sec, None);
        assert_eq!(info.gateway_id, None);
        assert_eq!(info.access_serial, Some(7));
    }

    /// 三种计划性关闭都能往返；窗口与优先接入点随之带回。
    #[test]
    fn server_close_round_trips_for_every_kind() {
        for kind in [
            CloseKind::Drain,
            CloseKind::ServerShutdown,
            CloseKind::Rebalance,
        ] {
            let close = ServerClose::new(kind)
                .with_retry_window(Duration::from_secs(1), Duration::from_secs(5))
                .with_prefer_ap(["ap-2", "ap-3"]);
            let command = close.to_command();
            assert_eq!(command.message, kind.as_str());
            assert_eq!(
                command.metadata.get(metadata_keys::CODE).map(Vec::as_slice),
                Some(kind.code().as_bytes())
            );
            assert_eq!(ServerClose::from_command(&command), Some(close.clone()));
            assert_eq!(
                close.retry_window(),
                Some((Duration::from_secs(1), Duration::from_secs(5)))
            );
        }
    }

    /// 原因只在 metadata 里、优先接入点带空白与空项、窗口两端写反：都按约定读出。
    #[test]
    fn server_close_tolerates_loose_encodings() {
        let command = close(
            None,
            Some(HashMap::from([
                (metadata_keys::REASON.to_string(), b"DRAIN".to_vec()),
                (
                    metadata_keys::PREFER_AP.to_string(),
                    b" ap-1 ,, ap-2 ,".to_vec(),
                ),
                (
                    metadata_keys::RETRY_AFTER_MIN_MS.to_string(),
                    b"9000".to_vec(),
                ),
                (
                    metadata_keys::RETRY_AFTER_MAX_MS.to_string(),
                    b"3000".to_vec(),
                ),
            ])),
        );
        let parsed = ServerClose::from_command(&command).expect("drain close");
        assert_eq!(parsed.kind, CloseKind::Drain);
        assert_eq!(
            parsed.prefer_ap,
            vec!["ap-1".to_string(), "ap-2".to_string()]
        );
        assert_eq!(
            parsed.retry_window(),
            Some((Duration::from_secs(3), Duration::from_secs(9)))
        );

        let only_max = close(
            Some("REBALANCE".to_string()),
            Some(HashMap::from([(
                metadata_keys::RETRY_AFTER_MAX_MS.to_string(),
                b"2500".to_vec(),
            )])),
        );
        assert_eq!(
            ServerClose::from_command(&only_max).and_then(|c| c.retry_window()),
            Some((Duration::from_millis(2500), Duration::from_millis(2500)))
        );
        assert_eq!(
            ServerClose::from_command(&close(Some("REBALANCE".to_string()), None))
                .and_then(|c| c.retry_window()),
            None
        );
    }

    /// 认不得的原因、不是 CLOSE 的命令都不算计划性关闭。
    #[test]
    fn unknown_reasons_and_other_commands_are_not_planned_closes() {
        assert_eq!(
            ServerClose::from_command(&close(Some("MAINTENANCE".to_string()), None)),
            None
        );
        assert_eq!(ServerClose::from_command(&close(None, None)), None);
        assert_eq!(ServerClose::from_command(&kicked("DRAIN", None)), None);
    }

    /// 过载拒绝带回等待时间；可重试、不终止、不计熔断。
    #[test]
    fn overloaded_rejection_carries_the_retry_after() {
        let rejection =
            ServerRejection::from_command(&overloaded_command(Duration::from_millis(2400)));
        assert_eq!(rejection.code, ErrorCode::Overloaded);
        assert_eq!(rejection.reason, "OVERLOADED");
        assert_eq!(rejection.retry_after_ms, Some(2400));
        assert!(rejection.is_overloaded());
        assert!(!rejection.is_terminal());
        assert!(!rejection.counts_against_access_point());
        assert!(ErrorCode::Overloaded.is_retryable());
        let error = rejection.to_error();
        assert_eq!(error.code(), Some(ErrorCode::Overloaded));
        assert!(error.to_string().contains("OVERLOADED"), "{error}");
    }

    /// 需要升级是终止类拒绝，最低构建号与升级说明随之带回。
    #[test]
    fn upgrade_required_is_terminal_and_carries_its_details() {
        let rejection = ServerRejection::from_command(&upgrade_required_command(
            30_201,
            Some("https://example.com/download"),
        ));
        assert_eq!(rejection.code, ErrorCode::UpgradeRequired);
        assert_eq!(rejection.min_build, Some(30_201));
        assert_eq!(
            rejection.release_hint.as_deref(),
            Some("https://example.com/download")
        );
        assert!(rejection.is_terminal());
        assert!(!rejection.counts_against_access_point());
        assert!(!ErrorCode::UpgradeRequired.is_retryable());
        assert_eq!(ErrorCode::from_u32(3006), Some(ErrorCode::UpgradeRequired));
        assert_eq!(ErrorCode::from_u32(6005), Some(ErrorCode::Overloaded));
    }

    /// 协议版本不被支持（3001）是终止类拒绝。
    #[test]
    fn protocol_version_mismatch_is_terminal() {
        let rejection = ServerRejection::from_command(&protocol_version_mismatch_command());
        assert_eq!(rejection.code, ErrorCode::ProtocolVersionMismatch);
        assert_eq!(rejection.code.as_u32(), 3001);
        assert!(rejection.is_terminal());
    }

    /// 既有拒绝的归类不变：租户不可用、令牌问题、服务端暂时放不了行、握手期间的停机 CLOSE。
    #[test]
    fn existing_rejections_keep_their_classes() {
        let tenant = ServerRejection::classify("TENANT_UNAVAILABLE", Some("AUTHENTICATION_FAILED"));
        assert_eq!(tenant.code, ErrorCode::PermissionDenied);
        assert!(tenant.is_terminal());

        let auth = ServerRejection::classify("", Some("AUTHENTICATION_FAILED"));
        assert_eq!(auth.code, ErrorCode::AuthenticationFailed);
        assert_eq!(auth.reason, "AUTHENTICATION_FAILED");
        assert!(!auth.is_terminal());
        assert!(!auth.counts_against_access_point());

        let unavailable = ServerRejection::classify("SERVICE_UNAVAILABLE", None);
        assert_eq!(unavailable.code, ErrorCode::ServiceUnavailable);
        assert!(unavailable.counts_against_access_point());

        let shutdown =
            ServerRejection::from_command(&crate::common::shutdown::server_shutdown_close());
        assert_eq!(shutdown.code, ErrorCode::ServiceUnavailable);
        assert_eq!(shutdown.reason, "SERVER_SHUTDOWN");
        assert!(!shutdown.counts_against_access_point());

        let unknown = ServerRejection::classify("", None);
        assert_eq!(unknown.code, ErrorCode::ConnectionRefused);
    }

    /// 过载等待在 `base` 到 `1.5 × base` 之间，且确实有抖动。
    #[test]
    fn overload_retry_after_adds_up_to_half_the_base() {
        let base = Duration::from_secs(2);
        let mut distinct = std::collections::HashSet::new();
        for _ in 0..200 {
            let wait = overload_retry_after(base);
            assert!((base..=Duration::from_secs(3)).contains(&wait), "{wait:?}");
            distinct.insert(wait);
        }
        assert!(
            distinct.len() > 50,
            "only {} distinct waits",
            distinct.len()
        );
        assert_eq!(overload_retry_after(Duration::ZERO), Duration::ZERO);
    }
}
