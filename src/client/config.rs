//! 客户端配置模块

use crate::common::compression::CompressionAlgorithm;
use crate::common::config_types::{HeartbeatConfig, TlsConfig, TransportProtocol};
use crate::common::device::DeviceInfo;
use crate::common::protocol::SerializationFormat;
use std::time::Duration;

/// 客户端配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClientConfig {
    /// 服务器地址（单个协议时使用，多协议时用作默认地址）
    pub server_url: String,
    /// 传输协议（单个）
    pub transport: TransportProtocol,
    /// 交错竞速的协议优先序（设置了就忽略 `transport`）。按顺序依次发起，前一个在
    /// [`Self::race_stagger`] 内没完成握手或已经失败才发起下一个，第一个拿到 CONNECT_ACK 的胜出
    /// （见 [`crate::client::race`]）。上层每次建连传入，例如上次的胜者排在前面。
    pub transports: Option<Vec<TransportProtocol>>,
    /// 每个协议的独立地址配置（协议 -> 地址映射）
    /// 如果设置了此映射，每个协议将使用对应的地址
    pub protocol_urls: Option<std::collections::HashMap<TransportProtocol, String>>,
    /// 交错竞速的间隔：上一个协议这么久还没完成握手（含 CONNECT_ACK），就发起下一个。默认 300 ms。
    #[serde(default = "default_race_stagger")]
    pub race_stagger: Duration,
    /// 序列化格式（首选格式，用于协商）
    pub serialization_format: SerializationFormat,
    /// 压缩算法（首选算法，用于协商）
    pub compression: CompressionAlgorithm,
    /// 强制指定的序列化格式（如果设置，则强制使用此格式，不进行协商）
    /// 适用于某些端不支持 protobuf 等格式的场景
    pub force_serialization_format: Option<SerializationFormat>,
    /// 强制指定的压缩算法（如果设置，则强制使用此算法，不进行协商）
    pub force_compression: Option<CompressionAlgorithm>,
    /// 单个协议一次握手的时限：传输建立（TCP/QUIC、TLS、WebSocket 升级）加 CONNECT → CONNECT_ACK，
    /// 不含竞速里排队等 CONNECT 名额的时间。默认 8 s。
    ///
    /// 客户端不会自己重连：连接断开后由上层（链路监督者）决定何时、向哪里重新建连。
    pub connect_timeout: Duration,
    /// 这次建连的连接代数（CONNECT metadata `connection_epoch`），由上层每次建连给出、单调递增。
    #[serde(default)]
    pub connection_epoch: u64,
    /// 上层当前接入清单的 serial（CONNECT metadata `access_serial`）；还没有清单时为 0。
    #[serde(default)]
    pub access_serial: u64,
    /// 接入点的备用 IP（`ipHints`）：主机名 DNS 失败或结果可疑时才直连它们（规则见
    /// [`crate::client::dial`]），TLS SNI、证书校验和 SPKI 钉扎仍按 URL 的主机名。
    /// WebSocket 与 QUIC 生效；浏览器不能指定 IP，这里不起作用。
    #[serde(default)]
    pub ip_hints: Vec<std::net::IpAddr>,
    /// DoH 备用解析（见 [`crate::client::dial::DohCache`]）：运行时对象，由上层装配，不进配置文件。
    #[cfg(not(target_arch = "wasm32"))]
    #[serde(skip)]
    pub doh: Option<std::sync::Arc<crate::client::dial::DohCache>>,
    /// 心跳配置
    pub heartbeat: HeartbeatConfig,
    /// TLS 配置
    pub tls: TlsConfig,
    /// 自定义连接 ID（如果为 None，则自动生成）
    pub connection_id: Option<String>,
    /// 用户 ID（用于认证）
    pub user_id: Option<String>,
    /// 额外的元数据
    pub metadata: std::collections::HashMap<String, String>,
    /// 是否启用消息路由（默认 false）
    pub enable_router: bool,
    /// 设备信息（用于协商和设备管理）
    pub device_info: Option<DeviceInfo>,
    /// Token（用于认证，如果服务端启用认证，必须提供）
    pub token: Option<String>,
}

fn default_race_stagger() -> Duration {
    crate::client::race::DEFAULT_RACE_STAGGER
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_url: "ws://localhost:8080".to_string(),
            transport: TransportProtocol::WebSocket,
            transports: None,
            protocol_urls: None,
            race_stagger: default_race_stagger(),
            // 默认使用 JSON 序列化，不压缩（客户端可以在协商时指定首选格式）
            serialization_format: SerializationFormat::Json,
            compression: CompressionAlgorithm::None,
            force_serialization_format: None,
            force_compression: None,
            connect_timeout: crate::client::race::DEFAULT_CONNECT_TIMEOUT,
            connection_epoch: 0,
            access_serial: 0,
            ip_hints: Vec::new(),
            #[cfg(not(target_arch = "wasm32"))]
            doh: None,
            heartbeat: HeartbeatConfig::default(),
            tls: TlsConfig::none(),
            connection_id: None,
            user_id: None,
            metadata: std::collections::HashMap::new(),
            enable_router: false,
            device_info: None,
            token: None,
        }
    }
}

impl ClientConfig {
    /// 创建新的客户端配置
    pub fn new(server_url: String) -> Self {
        Self {
            server_url,
            ..Default::default()
        }
    }

    /// 使用 WebSocket 协议
    pub fn websocket(mut self) -> Self {
        self.transport = TransportProtocol::WebSocket;
        self
    }

    /// 使用 QUIC 协议
    pub fn quic(mut self) -> Self {
        self.transport = TransportProtocol::QUIC;
        self
    }

    /// 使用 TCP 协议
    pub fn tcp(mut self) -> Self {
        self.transport = TransportProtocol::TCP;
        self
    }

    /// 设置序列化格式
    pub fn with_format(mut self, format: SerializationFormat) -> Self {
        self.serialization_format = format;
        self
    }

    /// 设置压缩算法
    pub fn with_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.compression = compression;
        self
    }

    /// 设置用户 ID
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// 设置 Token（用于认证，如果服务端启用认证，必须提供）
    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    /// 设置交错竞速的协议优先序（见 [`Self::transports`]）。
    ///
    /// 例如 UDP 被静默丢弃时，`with_protocol_race(vec![QUIC, WebSocket])` 在交错间隔
    /// （[`Self::with_race_stagger`]）后发起 WebSocket 并经它连上，不等 QUIC 握手超时；QUIC 在
    /// 间隔内连上时根本不发起 WebSocket。
    pub fn with_protocol_race(mut self, protocols: Vec<TransportProtocol>) -> Self {
        self.transports = Some(protocols);
        self
    }

    /// 为特定协议设置服务器地址
    pub fn with_protocol_url(mut self, protocol: TransportProtocol, url: String) -> Self {
        if self.protocol_urls.is_none() {
            self.protocol_urls = Some(std::collections::HashMap::new());
        }
        if let Some(ref mut urls) = self.protocol_urls {
            urls.insert(protocol, url);
        }
        self
    }

    /// 批量设置协议地址映射
    pub fn with_protocol_urls(
        mut self,
        urls: std::collections::HashMap<TransportProtocol, String>,
    ) -> Self {
        self.protocol_urls = Some(urls);
        self
    }

    /// 获取指定协议的地址
    pub fn get_protocol_url(&self, protocol: &TransportProtocol) -> String {
        let endpoint = if let Some(ref urls) = self.protocol_urls
            && let Some(url) = urls.get(protocol)
        {
            url.as_str()
        } else {
            self.server_url.as_str()
        };

        protocol.normalize_client_url(endpoint)
    }

    /// 设置交错竞速的间隔（见 [`Self::race_stagger`]）
    pub fn with_race_stagger(mut self, stagger: Duration) -> Self {
        self.race_stagger = stagger;
        self
    }

    /// 设置这次建连的连接代数（见 [`Self::connection_epoch`]）
    pub fn with_connection_epoch(mut self, epoch: u64) -> Self {
        self.connection_epoch = epoch;
        self
    }

    /// 设置上层当前接入清单的 serial（见 [`Self::access_serial`]）
    pub fn with_access_serial(mut self, serial: u64) -> Self {
        self.access_serial = serial;
        self
    }

    /// 设置接入点的备用 IP（见 [`Self::ip_hints`]）
    pub fn with_ip_hints(mut self, ip_hints: Vec<std::net::IpAddr>) -> Self {
        self.ip_hints = ip_hints;
        self
    }

    /// 设置 DoH 备用解析（见 [`Self::doh`]）
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_doh(mut self, doh: std::sync::Arc<crate::client::dial::DohCache>) -> Self {
        self.doh = Some(doh);
        self
    }

    /// 设置心跳配置
    pub fn with_heartbeat(mut self, heartbeat: HeartbeatConfig) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    /// 设置 TLS 配置
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// 设置单个协议一次握手的时限（见 [`Self::connect_timeout`]）
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// 启用消息路由
    pub fn enable_router(mut self) -> Self {
        self.enable_router = true;
        self
    }

    /// 设置设备信息（用于协商和设备管理）
    pub fn with_device_info(mut self, device_info: DeviceInfo) -> Self {
        self.device_info = Some(device_info);
        self
    }

    /// 强制指定序列化格式（不进行协商，直接使用此格式）
    ///
    /// 适用于某些端不支持 protobuf 等格式的场景
    /// 如果设置了强制格式，客户端将直接使用此格式，服务端必须接受
    pub fn force_format(mut self, format: SerializationFormat) -> Self {
        self.force_serialization_format = Some(format);
        self
    }

    /// 强制指定压缩算法（不进行协商，直接使用此算法）
    ///
    /// 如果设置了强制压缩，客户端将直接使用此算法，服务端必须接受
    pub fn force_compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.force_compression = Some(compression);
        self
    }

    /// 检查是否强制指定了格式（不进行协商）
    pub fn is_force_format(&self) -> bool {
        self.force_serialization_format.is_some() || self.force_compression.is_some()
    }

    /// 获取实际使用的序列化格式（强制格式优先，否则使用首选格式）
    pub fn get_serialization_format(&self) -> SerializationFormat {
        self.force_serialization_format
            .unwrap_or(self.serialization_format)
    }

    /// 获取实际使用的压缩算法（强制算法优先，否则使用首选算法）
    pub fn get_compression(&self) -> CompressionAlgorithm {
        self.force_compression
            .clone()
            .unwrap_or_else(|| self.compression.clone())
    }

    /// 要依次尝试的协议（交错竞速的优先序；没设置时只有 `transport` 一个）
    pub fn get_protocols(&self) -> Vec<TransportProtocol> {
        match &self.transports {
            Some(protocols) if !protocols.is_empty() => protocols.clone(),
            _ => vec![self.transport],
        }
    }

    /// 交错竞速参数
    pub(crate) fn race_params(&self) -> crate::client::race::RaceParams {
        crate::client::race::RaceParams {
            order: self.get_protocols(),
            stagger: self.race_stagger,
            connect_timeout: self.connect_timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_protocol_specific_urls_from_default_websocket_url() {
        let config = ClientConfig::default();

        assert_eq!(
            config.get_protocol_url(&TransportProtocol::WebSocket),
            "ws://localhost:8080"
        );
        assert_eq!(
            config.get_protocol_url(&TransportProtocol::QUIC),
            "quic://localhost:8080"
        );
        assert_eq!(
            config.get_protocol_url(&TransportProtocol::TCP),
            "tcp://localhost:8080"
        );
    }

    #[test]
    fn normalizes_explicit_bare_protocol_url() {
        let config = ClientConfig::default()
            .with_protocol_url(TransportProtocol::TCP, "127.0.0.1:19090".to_string());

        assert_eq!(
            config.get_protocol_url(&TransportProtocol::TCP),
            "tcp://127.0.0.1:19090"
        );
    }
}
