//! 共享配置类型
//!
//! 定义客户端和服务端共用的配置类型

use std::path::PathBuf;
use std::time::Duration;

/// 传输协议类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum TransportProtocol {
    /// WebSocket 协议
    WebSocket,
    /// QUIC 协议
    QUIC,
    /// TCP 协议
    TCP,
}

impl TransportProtocol {
    /// 从字符串转换为协议类型
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "websocket" | "ws" => Some(TransportProtocol::WebSocket),
            "quic" => Some(TransportProtocol::QUIC),
            "tcp" => Some(TransportProtocol::TCP),
            _ => None,
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            TransportProtocol::WebSocket => "websocket",
            TransportProtocol::QUIC => "quic",
            TransportProtocol::TCP => "tcp",
        }
    }

    /// 将任意基础端点规范化为客户端连接 URL。
    ///
    /// 多协议竞速时常以 WebSocket URL 作为默认地址；这里负责按目标协议替换 scheme，
    /// 避免 TCP/QUIC 客户端拿到 `ws://...` 后各自解析失败。
    pub fn normalize_client_url(&self, endpoint: &str) -> String {
        let endpoint = endpoint.trim();
        match self {
            TransportProtocol::WebSocket => normalize_websocket_client_url(endpoint),
            TransportProtocol::QUIC => {
                format!("quic://{}", strip_known_scheme(endpoint))
            }
            TransportProtocol::TCP => {
                format!("tcp://{}", strip_known_scheme(endpoint))
            }
        }
    }

    /// 将任意协议端点规范化为服务端监听地址。
    ///
    /// 服务端 listener/endpoint 只需要 `host:port`，不应把客户端 URL scheme 传进
    /// `SocketAddr` parser。
    pub fn normalize_server_bind_address(endpoint: &str) -> String {
        strip_known_scheme(endpoint.trim()).to_string()
    }
}

fn normalize_websocket_client_url(endpoint: &str) -> String {
    if endpoint.starts_with("ws://") || endpoint.starts_with("wss://") {
        return endpoint.to_string();
    }

    let scheme = if endpoint.starts_with("https://") {
        "wss"
    } else {
        "ws"
    };
    format!("{scheme}://{}", strip_known_scheme(endpoint))
}

fn strip_known_scheme(endpoint: &str) -> &str {
    const SCHEMES: [&str; 6] = [
        "ws://", "wss://", "quic://", "tcp://", "http://", "https://",
    ];
    SCHEMES
        .iter()
        .find_map(|scheme| endpoint.strip_prefix(scheme))
        .unwrap_or(endpoint)
}

/// TLS/SSL 证书配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TlsConfig {
    /// 证书文件路径（PEM 或 DER 格式）
    pub cert_path: Option<PathBuf>,
    /// 私钥文件路径（PEM 或 DER 格式）
    pub key_path: Option<PathBuf>,
    /// 证书数据（DER 格式，Base64 编码的字符串或直接字节）
    pub cert_data: Option<Vec<u8>>,
    /// 私钥数据（DER 格式，Base64 编码的字符串或直接字节）
    pub key_data: Option<Vec<u8>>,
    /// 是否验证服务器证书（客户端使用）
    pub verify_cert: bool,
    /// CA 证书文件路径（用于验证服务器证书）
    pub ca_cert_path: Option<PathBuf>,
    /// CA 证书数据
    pub ca_cert_data: Option<Vec<u8>>,
    /// SubjectPublicKeyInfo SHA-256 pin 列表，支持 `spki-sha256/<base64>`、`sha256/<base64>`、裸 base64、hex 或冒号分隔 hex。
    ///
    /// 可同时配置当前 pin 与下一把 pin，用于 App 下发后的双 pin 轮换。
    #[serde(default)]
    pub spki_sha256_pins: Vec<String>,
    /// 证书 SHA-256 pin 列表，支持 `sha256/<base64>`、裸 base64、hex 或冒号分隔 hex。
    ///
    /// 保留用于旧整证书 pin；新客户端应优先使用 `spki_sha256_pins`。
    #[serde(default)]
    pub certificate_sha256_pins: Vec<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_path: None,
            key_path: None,
            cert_data: None,
            key_data: None,
            verify_cert: true,
            ca_cert_path: None,
            ca_cert_data: None,
            spki_sha256_pins: Vec::new(),
            certificate_sha256_pins: Vec::new(),
        }
    }
}

impl TlsConfig {
    /// 创建空配置（不使用 TLS）
    pub fn none() -> Self {
        Self::default()
    }

    /// 从文件路径创建配置
    pub fn from_files(cert_path: PathBuf, key_path: PathBuf) -> Self {
        Self {
            cert_path: Some(cert_path),
            key_path: Some(key_path),
            ..Default::default()
        }
    }

    /// 从内存数据创建配置
    pub fn from_data(cert_data: Vec<u8>, key_data: Vec<u8>) -> Self {
        Self {
            cert_data: Some(cert_data),
            key_data: Some(key_data),
            ..Default::default()
        }
    }

    /// 设置 CA 证书路径（用于客户端验证服务器）
    pub fn with_ca_cert(mut self, ca_cert_path: PathBuf) -> Self {
        self.ca_cert_path = Some(ca_cert_path);
        self
    }

    /// 添加 SPKI SHA-256 pin。
    pub fn with_spki_sha256_pin(mut self, pin: impl Into<String>) -> Self {
        self.spki_sha256_pins.push(pin.into());
        self
    }

    /// 批量设置 SPKI SHA-256 pin。
    pub fn with_spki_sha256_pins(mut self, pins: Vec<String>) -> Self {
        self.spki_sha256_pins = pins;
        self
    }

    /// 添加证书 SHA-256 pin。
    pub fn with_certificate_sha256_pin(mut self, pin: impl Into<String>) -> Self {
        self.certificate_sha256_pins.push(pin.into());
        self
    }

    /// 批量设置证书 SHA-256 pin。
    pub fn with_certificate_sha256_pins(mut self, pins: Vec<String>) -> Self {
        self.certificate_sha256_pins = pins;
        self
    }

    /// 是否启用证书 pinning。
    pub fn has_certificate_pins(&self) -> bool {
        !self.spki_sha256_pins.is_empty() || !self.certificate_sha256_pins.is_empty()
    }

    /// 是否需要 flare-core 提供自定义 rustls 客户端配置。
    pub fn requires_custom_client_tls(&self) -> bool {
        self.has_certificate_pins() || self.ca_cert_path.is_some() || self.ca_cert_data.is_some()
    }

    /// 禁用证书验证（仅用于开发/测试）
    pub fn disable_verification(mut self) -> Self {
        self.verify_cert = false;
        self
    }
}

/// 心跳所处的应用状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum HeartbeatAppState {
    /// 前台活跃，保持更短心跳以降低静默断连概率。
    #[default]
    Foreground,
    /// 后台/锁屏，允许更长心跳以节省电量。
    Background,
}

fn default_heartbeat_adaptive() -> bool {
    true
}

/// 个性化心跳配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct HeartbeatConfig {
    /// 心跳发送间隔
    pub interval: Duration,
    /// 心跳超时时间（如果在此时间内未收到响应，认为连接断开）
    pub timeout: Duration,
    /// 是否启用心跳
    pub enabled: bool,
    /// 是否启用自适应心跳。关闭后只使用 `interval`。
    #[serde(default = "default_heartbeat_adaptive")]
    pub adaptive: bool,
    /// 前台心跳间隔。未设置时回退到 `interval`，兼容旧配置。
    #[serde(default)]
    pub foreground_interval: Option<Duration>,
    /// 后台心跳间隔。未设置时使用移动 IM 常见的 120 秒上限。
    #[serde(default)]
    pub background_interval: Option<Duration>,
    /// 自适应下限。未设置时为 `min(interval, 15s)`，避免旧的短间隔配置被抬高。
    #[serde(default)]
    pub min_interval: Option<Duration>,
    /// 自适应上限。未设置时为 120 秒。
    #[serde(default)]
    pub max_interval: Option<Duration>,
    /// 已探测/配置的 NAT 空闲超时；心跳会收敛到约 70% NAT 超时以内。
    #[serde(default)]
    pub nat_timeout: Option<Duration>,
    /// 当前应用状态。
    #[serde(default)]
    pub app_state: HeartbeatAppState,
    /// 一次即时验活（回前台、网络变化、发送迟迟没有回执）等 PONG 的窗口；实际取 `min(timeout, 它)`。
    /// 窗口内没有 PONG 即判死断开。由上层按接入策略（`clientPolicy.probeWakeTimeoutMs`）设置。
    #[serde(default = "default_probe_timeout")]
    pub probe_timeout: Duration,
}

/// 即时验活窗口的缺省值（与接入策略 `probeWakeTimeoutMs` 的缺省一致）。
pub const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

fn default_probe_timeout() -> Duration {
    DEFAULT_PROBE_TIMEOUT
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(90),
            enabled: true,
            adaptive: true,
            foreground_interval: None,
            background_interval: Some(Duration::from_secs(120)),
            min_interval: Some(Duration::from_secs(15)),
            max_interval: Some(Duration::from_secs(120)),
            nat_timeout: None,
            app_state: HeartbeatAppState::Foreground,
            probe_timeout: DEFAULT_PROBE_TIMEOUT,
        }
    }
}

impl HeartbeatConfig {
    /// 创建默认配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置心跳间隔
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self.foreground_interval = Some(interval);
        if self.min_interval.is_none_or(|min| interval < min) {
            self.min_interval = Some(interval);
        }
        self
    }

    /// 设置超时时间
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// 开关自适应心跳。
    pub fn with_adaptive(mut self, adaptive: bool) -> Self {
        self.adaptive = adaptive;
        self
    }

    /// 设置前台心跳间隔。
    pub fn with_foreground_interval(mut self, interval: Duration) -> Self {
        self.foreground_interval = Some(interval);
        if self.min_interval.is_none_or(|min| interval < min) {
            self.min_interval = Some(interval);
        }
        self
    }

    /// 设置后台心跳间隔。
    pub fn with_background_interval(mut self, interval: Duration) -> Self {
        self.background_interval = Some(interval);
        self
    }

    /// 设置自适应心跳上下限。
    pub fn with_interval_bounds(mut self, min: Duration, max: Duration) -> Self {
        self.min_interval = Some(min);
        self.max_interval = Some(max);
        self
    }

    /// 设置 NAT 空闲超时，用于把心跳收敛到 NAT 超时之前。
    pub fn with_nat_timeout(mut self, timeout: Duration) -> Self {
        self.nat_timeout = Some(timeout);
        self
    }

    /// 清除 NAT 超时探测结果。
    pub fn without_nat_timeout(mut self) -> Self {
        self.nat_timeout = None;
        self
    }

    /// 设置即时验活等 PONG 的窗口。
    pub fn with_probe_timeout(mut self, probe_timeout: Duration) -> Self {
        self.probe_timeout = probe_timeout;
        self
    }

    /// 即时验活实际等待的窗口：不超过心跳超时。
    pub fn effective_probe_timeout(&self) -> Duration {
        self.timeout.min(self.probe_timeout)
    }

    /// 设置应用前后台状态。
    pub fn with_app_state(mut self, app_state: HeartbeatAppState) -> Self {
        self.app_state = app_state;
        self
    }

    /// 标记应用在前台。
    pub fn foreground(mut self) -> Self {
        self.app_state = HeartbeatAppState::Foreground;
        self
    }

    /// 标记应用在后台。
    pub fn background(mut self) -> Self {
        self.app_state = HeartbeatAppState::Background;
        self
    }

    /// 当前配置下实际使用的心跳间隔。
    pub fn effective_interval(&self) -> Duration {
        if !self.adaptive {
            return positive_duration(self.interval);
        }

        let base = match self.app_state {
            HeartbeatAppState::Foreground => self.foreground_interval.unwrap_or(self.interval),
            HeartbeatAppState::Background => self
                .background_interval
                .unwrap_or_else(|| Duration::from_secs(120)),
        };

        let nat_limited = self
            .nat_timeout
            .map(nat_safe_interval)
            .unwrap_or_else(|| positive_duration(base));

        let candidate = positive_duration(base).min(nat_limited);
        let min = self
            .min_interval
            .unwrap_or_else(|| self.interval.min(Duration::from_secs(15)));
        let min = positive_duration(min);
        let max = self
            .max_interval
            .unwrap_or_else(|| Duration::from_secs(120))
            .max(min);

        candidate.clamp(min, max)
    }

    /// 禁用心跳
    pub fn disable(mut self) -> Self {
        self.enabled = false;
        self
    }
}

fn positive_duration(duration: Duration) -> Duration {
    if duration.is_zero() {
        Duration::from_millis(1)
    } else {
        duration
    }
}

fn nat_safe_interval(timeout: Duration) -> Duration {
    let millis = timeout.as_millis().saturating_mul(70) / 100;
    let millis = millis.clamp(1, u64::MAX as u128) as u64;
    Duration::from_millis(millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_defaults_keep_foreground_at_legacy_interval() {
        let config = HeartbeatConfig::default();

        assert_eq!(config.effective_interval(), Duration::from_secs(30));
    }

    #[test]
    fn heartbeat_uses_longer_background_interval() {
        let config = HeartbeatConfig::default().background();

        assert_eq!(config.effective_interval(), Duration::from_secs(120));
    }

    #[test]
    fn heartbeat_respects_nat_timeout_before_idle_drop() {
        let config = HeartbeatConfig::default().with_nat_timeout(Duration::from_secs(40));

        assert_eq!(config.effective_interval(), Duration::from_secs(28));
    }

    #[test]
    fn heartbeat_can_disable_adaptive_policy() {
        let config = HeartbeatConfig::default()
            .with_interval(Duration::from_secs(7))
            .with_nat_timeout(Duration::from_secs(10))
            .background()
            .with_adaptive(false);

        assert_eq!(config.effective_interval(), Duration::from_secs(7));
    }

    #[test]
    fn tls_config_tracks_certificate_pins() {
        let tls = TlsConfig::none()
            .with_spki_sha256_pin("spki-sha256/test")
            .with_certificate_sha256_pin("sha256/test");

        assert!(tls.has_certificate_pins());
        assert_eq!(tls.spki_sha256_pins, vec!["spki-sha256/test"]);
        assert_eq!(tls.certificate_sha256_pins, vec!["sha256/test"]);
    }
}
