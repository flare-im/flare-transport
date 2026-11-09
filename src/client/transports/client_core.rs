//! 客户端核心功能
//!
//! 提供统一的连接状态管理、心跳管理、消息路由等功能，简化客户端实现

use crate::client::config::ClientConfig;
use crate::client::connection::ConnectionStateManager;
use crate::client::heartbeat::HeartbeatManager;
use crate::client::link::{DisconnectReason, DisconnectReasonCell};
use crate::client::router::MessageRouter;
use crate::common::error::{FlareError, Result};
use crate::common::link::{ConnectAckInfo, ServerClose, ServerRejection};
use crate::common::protocol::Frame;
use crate::common::protocol::flare::core::commands::command::Type;
use crate::common::protocol::flare::core::commands::notification_command::Type as NotificationCommandType;
use crate::common::protocol::flare::core::commands::payload_command::Type as PayloadCommandType;
use crate::common::protocol::flare::core::commands::system_command::Type as SystemCommandType;
use crate::common::{HeartbeatAppState, HeartbeatConfig, MessageParser};
use crate::transport::connection::Connection;
use crate::transport::events::{
    ArcObserver, ConnectionEvent, deliver_to_observers, observer_snapshot,
};
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex as StdMutex, RwLock as StdRwLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, Notify, oneshot};

#[path = "client_core_connect.rs"]
mod client_core_connect;

const NEGOTIATION_TIMEOUT_HINT: &str =
    "Ensure `flare_chat_server` is running, not `simple_server`.";

/// 协商完成后连接断开、而读出的帧还没处理完时，断开事件最多等它们这么久再上报
/// （见 [`ClientCore::wait_inbound_drained`]）。
pub(crate) const INBOUND_DRAIN_BOUND: std::time::Duration = std::time::Duration::from_secs(1);

/// WASM 入站队列上限：防止浏览器回调洪泛占满内存；丢弃最旧帧并记录日志。
#[cfg(target_arch = "wasm32")]
const MAX_WASM_INBOUND_QUEUE: usize = 512;

pub use crate::common::kick::TENANT_UNAVAILABLE_REASON;

/// 把服务端拒绝 CONNECT 时下发的 ERROR 帧（`message` 为类型化原因，`metadata.code` 为错误码）
/// 映射成客户端错误，归类规则见 [`ServerRejection::classify`]。
///
/// 错误文本以类型化原因开头，上层可以据此分辨。
pub fn connect_rejection_error(reason: &str, code: Option<&str>) -> FlareError {
    ServerRejection::classify(reason, code).to_error()
}

/// 连接在 CONNECT_ACK 之前被关掉、服务端又没给原因时的错误：这是连接失败，不能猜成令牌被拒。
fn closed_before_connect_ack_error(detail: &str) -> FlareError {
    FlareError::connection_failed(format!(
        "server closed the connection before CONNECT_ACK without a reason ({detail})"
    ))
}

fn negotiation_timeout_error(timeout: std::time::Duration) -> FlareError {
    FlareError::connection_timeout(format!(
        "Negotiation timeout after {:?} (CONNECT_ACK not received). {}",
        timeout, NEGOTIATION_TIMEOUT_HINT
    ))
}

#[cfg(not(target_arch = "wasm32"))]
async fn wait_for_negotiation_notify(
    flag: Arc<AtomicBool>,
    failure: Arc<StdMutex<Option<FlareError>>>,
    notify: Arc<Notify>,
    timeout: std::time::Duration,
) -> Result<()> {
    let wait = async move {
        loop {
            // 先登记唤醒再检查，避免检查与等待之间的通知丢失。
            let notified = notify.notified();
            if flag.load(Ordering::SeqCst) {
                return Ok(());
            }
            if let Ok(failure) = failure.lock()
                && let Some(error) = failure.as_ref()
            {
                return Err(error.clone());
            }
            notified.await;
        }
    };

    match crate::common::platform::timeout(timeout, wait).await {
        Ok(result) => result,
        Err(_) => Err(negotiation_timeout_error(timeout)),
    }
}

/// 客户端核心功能
///
/// 统一管理连接状态、心跳、消息路由，简化客户端实现
pub struct ClientCore {
    /// 连接状态管理器
    pub state_manager: Arc<ConnectionStateManager>,
    /// 消息解析器（使用 Arc<Mutex<>> 以支持协商后更新）
    pub parser: Arc<tokio::sync::Mutex<MessageParser>>,
    /// 心跳管理器（共享，clone 与 observer 路径均可启动/停止）
    heartbeat_manager: Arc<StdMutex<Option<Arc<tokio::sync::Mutex<HeartbeatManager>>>>>,
    /// 运行期心跳策略，供前后台/NAT 探测动态更新。
    heartbeat_config: Arc<StdRwLock<HeartbeatConfig>>,
    /// 前台/网络恢复时唤醒心跳循环做一次即时验活（不等一个完整周期），
    /// 用于戳穿半开死连（onclose 从未触发的场景）并触发重连自愈。
    heartbeat_probe_wake: Arc<Notify>,
    /// 消息路由器（可选，通过配置开启）
    message_router: Option<MessageRouter>,
    /// 观察者列表
    pub observers: Arc<StdMutex<Vec<ArcObserver>>>,
    /// 客户端配置
    pub config: ClientConfig,
    /// 事件处理器（可选，用于自定义业务逻辑）
    event_handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
    /// 客户端连接（用于断开连接）
    /// 使用 Arc 包装，以便在 clone 时共享同一个连接引用
    #[allow(clippy::type_complexity)]
    client_connection: Arc<std::sync::Mutex<Option<Arc<Mutex<Box<dyn Connection>>>>>>,
    /// 等待响应的请求池（按 message_id 匹配）
    pub(crate) pending_map: Arc<tokio::sync::Mutex<HashMap<String, oneshot::Sender<Frame>>>>,
    /// 协商完成标志
    /// 当收到 CONNECT_ACK 并更新 parser 后，设置为 true
    pub(crate) negotiation_completed: Arc<AtomicBool>,
    /// 用于等待 CONNECT_ACK 完成
    pub(crate) negotiation_notify: Arc<Notify>,
    /// CONNECT_ACK 之前的失败（服务端拒绝、连接被关、CONNECT_ACK 无效）；
    /// `wait_for_negotiation` 立即返回它
    negotiation_failure: Arc<StdMutex<Option<FlareError>>>,
    /// 我方已请求断开（disconnect_internal 中置位）：若随后读循环仍收到 KICK，不向观察者通知「被踢」，避免重复登录/协议竞速时误报
    disconnect_requested: Arc<AtomicBool>,
    /// CONNECT_ACK 带回的接入信息；新连接建立时清除。
    connect_ack: Arc<StdMutex<Option<ConnectAckInfo>>>,
    /// 服务端拒绝 CONNECT 的类型化原因（带过载等待、最低构建号等字段）；新连接建立时清除。
    rejection: Arc<StdMutex<Option<ServerRejection>>>,
    /// 协商完成后收到的计划性 CLOSE（排空、停机、再均衡）：随后的断开以它作为原因上报。
    /// 新连接建立时清除。
    server_close: Arc<StdMutex<Option<ServerClose>>>,
    /// 协商完成后收到的终止类 ERROR（如在线连接被升级门槛拒绝的 `UPGRADE_REQUIRED`）：随后的断开以它
    /// 作为原因上报，与协商前的拒绝同一类型。新连接建立时清除。
    session_rejection: Arc<StdMutex<Option<ServerRejection>>>,
    /// 这条连接最近一次上报给观察者的断开原因（类型化）。
    disconnect_reason: DisconnectReasonCell,
    /// 竞速里等着交给胜出连接的上层绑定（见 [`LinkBinding`]）。
    link_binding: Arc<StdMutex<BindingState>>,
    /// 服务端以结束会话的原因踢线（设备冲突、租户不可用，见 [`crate::common::kick::ends_session`]）：
    /// 记下原因，此后不再自动重连（包括发送前的重连），由应用重新登录后显式建连；新连接建立时清除。
    session_ended_by_server: Arc<StdMutex<Option<String>>>,
    /// 要报给观察者、还没报出去的被踢原因。处理 KICKED 时登记；此后这条连接上第一个断开或错误事件
    /// （关连接带出的同一原因的断开，或读循环里并发到达的对端关闭）把它取走、以它上报；对端已先关掉
    /// 连接、关连接没有带出事件时，由 KICKED 的处理补报。是否上报、以什么原因上报都在这把锁内决定，
    /// `kick_reported` 也只在锁内改（见 [`Self::close_report`]）。
    pending_kick: Arc<StdMutex<Option<String>>>,
    /// 这条连接的被踢已经报给了观察者：之后同一条连接再报的断开、错误（对端关闭、读出错）不再上报，
    /// 观察者只收到一次被踢原因。新连接建立时清除。
    kick_reported: Arc<AtomicBool>,
    /// 已读出、还没处理完的入站帧数。帧在独立任务里处理，连接关闭事件却在读循环里同步到达：
    /// 协商完成后的断开要等这些帧处理完再上报（服务端发完 KICKED 紧接着关连接时，被踢要先于对端关闭）。
    inbound_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    /// 入站帧处理完、`inbound_in_flight` 归零时唤醒。
    inbound_drained: Arc<Notify>,
    /// WASM: browser WebSocket 回调线程同步入队，由 `wait_for_negotiation`/drain 在 LocalSet 内处理；
    /// 每帧带着入队那一刻的协商状态（见 [`Self::handle_received_message`]）
    #[cfg(target_arch = "wasm32")]
    wasm_inbound: Arc<StdMutex<Vec<WasmInboundFrame>>>,
}

/// WASM 入站队列里的一帧：数据、入队那一刻的协商状态、在途守卫。
#[cfg(target_arch = "wasm32")]
type WasmInboundFrame = (Vec<u8>, bool, InboundFrameDone);

/// 一帧已记为在途的入站数据（见 [`ClientCore::inbound_frame_read`]）：drop 时记这帧处理完。
///
/// 处理它的任务 panic、被取消（包括运行时关闭、任务还没跑起来就被丢掉），或这帧被丢弃时，都随 drop
/// 记完成。否则在途数永远不归零，此后这个 core 上每次协商完成后的对端关闭都要白等满
/// [`INBOUND_DRAIN_BOUND`] 才上报。
#[must_use = "dropping the guard marks the inbound frame as handled"]
pub(crate) struct InboundFrameDone {
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    drained: Arc<Notify>,
}

impl Drop for InboundFrameDone {
    #[allow(
        deprecated,
        reason = "fetch_update is renamed try_update on newer toolchains, and try_update is unstable at the declared MSRV"
    )]
    fn drop(&mut self) {
        let previous = self
            .in_flight
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .unwrap_or(0);
        if previous <= 1 {
            self.drained.notify_waiters();
        }
    }
}

/// 竞速里等着交给胜出连接的上层绑定：上层观察者，以及上层读断开原因用的槽。
///
/// 只有正在发 CONNECT 的那条尝试持有它；收到 CONNECT_ACK 时整体挂上（[`ClientCore`] 在通知等待
/// 协商的一方之前完成，竞速返回时观察者已经在了，紧跟 CONNECT_ACK 的帧不会丢）。尝试失败时由竞速
/// 收回，落败连接的事件不会到上层。
#[derive(Clone, Default)]
pub(crate) struct LinkBinding {
    pub(crate) observers: Vec<ArcObserver>,
    pub(crate) reason_slot: Option<Arc<std::sync::OnceLock<DisconnectReasonCell>>>,
}

/// 上层绑定在一条连接上的状态。
#[derive(Default)]
enum BindingState {
    #[default]
    Unbound,
    Pending(LinkBinding),
    Adopted,
}

/// 收回上层绑定的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BindingRevoke {
    /// 收回了：上层没有挂到这条连接上。
    Revoked,
    /// 收回之前 CONNECT_ACK 已经到了，上层已经挂上：这条连接其实成功了。
    AlreadyAdopted,
    /// 这条连接没有绑定。
    Unbound,
}

/// 连接断开或出错时向观察者报什么（见 [`ClientCore::close_report`]）。
enum CloseReport {
    /// 照常上报这次事件。
    AsIs,
    /// 以被踢原因上报一次断开。
    Kick(String),
    /// 这条连接的被踢已经报过，不再上报。
    AlreadyKicked,
}

impl ClientCore {
    /// 创建新的客户端核心
    pub fn new(config: &ClientConfig) -> Self {
        let (format, compression) = Self::determine_initial_format(config);
        let parser = MessageParser::new(
            format,
            compression,
            crate::common::encryption::EncryptionAlgorithm::None,
        );

        let message_router = config.enable_router.then(MessageRouter::new);

        Self {
            state_manager: Arc::new(ConnectionStateManager::new()),
            parser: Arc::new(tokio::sync::Mutex::new(parser)),
            heartbeat_manager: Arc::new(StdMutex::new(None)),
            heartbeat_config: Arc::new(StdRwLock::new(config.heartbeat.clone())),
            heartbeat_probe_wake: Arc::new(Notify::new()),
            message_router,
            observers: Arc::new(StdMutex::new(Vec::new())),
            config: config.clone(),
            event_handler: None,
            client_connection: Arc::new(std::sync::Mutex::new(None)),
            pending_map: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            negotiation_completed: Arc::new(AtomicBool::new(false)),
            negotiation_notify: Arc::new(Notify::new()),
            negotiation_failure: Arc::new(StdMutex::new(None)),
            disconnect_requested: Arc::new(AtomicBool::new(false)),
            connect_ack: Arc::new(StdMutex::new(None)),
            rejection: Arc::new(StdMutex::new(None)),
            server_close: Arc::new(StdMutex::new(None)),
            session_rejection: Arc::new(StdMutex::new(None)),
            disconnect_reason: Arc::new(StdMutex::new(None)),
            link_binding: Arc::new(StdMutex::new(BindingState::Unbound)),
            session_ended_by_server: Arc::new(StdMutex::new(None)),
            pending_kick: Arc::new(StdMutex::new(None)),
            kick_reported: Arc::new(AtomicBool::new(false)),
            inbound_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            inbound_drained: Arc::new(Notify::new()),
            #[cfg(target_arch = "wasm32")]
            wasm_inbound: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    /// WASM: enqueue bytes from the browser `onmessage` callback (sync context).
    #[cfg(target_arch = "wasm32")]
    pub fn push_wasm_inbound(&self, data: Vec<u8>) {
        let negotiated = self.is_negotiation_completed();
        // 记一帧在途；出队处理完、被丢弃（队满挤掉、排空任务被取消）时随守卫记完成
        let done = self.inbound_frame_read();
        if let Ok(mut queue) = self.wasm_inbound.lock() {
            if queue.len() >= MAX_WASM_INBOUND_QUEUE {
                // 挤掉的帧连同守卫一起 drop，记为处理完
                drop(queue.remove(0));
                tracing::warn!(
                    "[ClientCore] wasm inbound queue full (max {}), dropping oldest frame",
                    MAX_WASM_INBOUND_QUEUE
                );
            }
            queue.push((data, negotiated, done));
        }
        self.negotiation_notify.notify_waiters();
    }

    /// WASM: process all queued inbound frames on the current LocalSet task.
    #[cfg(target_arch = "wasm32")]
    pub async fn drain_wasm_inbound(&self) {
        let batch: Vec<WasmInboundFrame> = match self.wasm_inbound.lock() {
            Ok(mut queue) if !queue.is_empty() => queue.drain(..).collect(),
            _ => return,
        };
        // 每帧的守卫在它处理完时 drop；排空中途被取消时，还没处理的帧随 batch 一起 drop
        for (data, negotiated, _done) in batch {
            self.handle_received_message(data, negotiated).await;
        }
    }

    /// 标记「我方已请求断开」（disconnect_internal 调用前设置，收到 KICK 时不向观察者通知被踢）
    pub fn set_disconnect_requested(&self, value: bool) {
        self.disconnect_requested.store(value, Ordering::SeqCst);
    }

    /// 我方是否已主动断开这个客户端。
    pub fn is_disconnect_requested(&self) -> bool {
        self.disconnect_requested.load(Ordering::SeqCst)
    }

    /// CONNECT_ACK 带回的接入信息（协商完成之前为 `None`）。
    pub fn connect_ack_info(&self) -> Option<ConnectAckInfo> {
        self.connect_ack.lock().ok().and_then(|info| info.clone())
    }

    /// 服务端拒绝 CONNECT 的类型化原因（这条连接没被拒时为 `None`）。
    pub fn server_rejection(&self) -> Option<ServerRejection> {
        self.rejection
            .lock()
            .ok()
            .and_then(|rejection| rejection.clone())
    }

    /// 这条连接最近一次上报给观察者的断开原因。
    pub fn last_disconnect_reason(&self) -> Option<DisconnectReason> {
        self.disconnect_reason
            .lock()
            .ok()
            .and_then(|reason| reason.clone())
    }

    /// 断开原因的槽：观察者收到断开、错误事件时，从这里读同一次事件的类型化原因。
    pub fn disconnect_reason_cell(&self) -> DisconnectReasonCell {
        Arc::clone(&self.disconnect_reason)
    }

    /// 服务端已以结束会话的原因（设备冲突、租户不可用，见 [`crate::common::kick::ends_session`]）
    /// 踢掉这个客户端时，返回那个原因。
    ///
    /// 恢复要由应用重新登录后显式建连（设备冲突时连回去会把新设备再顶下线、两台设备来回互踢，
    /// 租户不可用时重连只会被再拒一次）；新连接建立时清除。
    pub fn session_ended_by_server(&self) -> Option<String> {
        self.session_ended_by_server
            .lock()
            .ok()
            .and_then(|reason| reason.clone())
    }

    pub(crate) fn end_session_by_server(&self, reason: &str) {
        if let Ok(mut ended) = self.session_ended_by_server.lock() {
            *ended = Some(reason.to_string());
        }
    }

    fn set_pending_kick(&self, reason: &str) {
        if let Ok(mut pending) = self.pending_kick.lock() {
            *pending = Some(reason.to_string());
        }
    }

    /// 取走还没报出去的被踢原因并记为已上报（调用方随即上报它）。
    fn take_unreported_kick(&self) -> Option<String> {
        let mut pending = self.pending_kick.lock().ok()?;
        let reason = pending.take()?;
        self.kick_reported.store(true, Ordering::SeqCst);
        Some(reason)
    }

    /// 连接断开或出错时向观察者报什么。在被踢登记的锁内一次决定：读循环里的对端关闭与被踢处理里
    /// 关连接带出的断开可能在两个线程上并发到达，只有一方上报。
    ///
    /// - 有还没报出去的被踢：这次断开就是被踢的结果，以被踢原因上报，不管事件本身带的是什么原因。
    ///   被踢处理先把状态置为断开再关连接，这期间到达的对端关闭不等入站帧处理完（见
    ///   [`Self::should_defer_close_until_inbound_drained`]），会先于补报到达；
    /// - 这条连接的被踢已经报过、且不是我方之后主动断开：不再上报；
    /// - 否则照常上报这次事件。
    fn close_report(&self) -> CloseReport {
        let Ok(mut pending) = self.pending_kick.lock() else {
            return CloseReport::AsIs;
        };
        if let Some(reason) = pending.take() {
            self.kick_reported.store(true, Ordering::SeqCst);
            return CloseReport::Kick(reason);
        }
        if self.kick_reported.load(Ordering::SeqCst)
            && !self.disconnect_requested.load(Ordering::SeqCst)
        {
            return CloseReport::AlreadyKicked;
        }
        CloseReport::AsIs
    }

    /// 读循环读出一帧、交给处理任务之前调用：记一帧在途，返回的守卫随这帧处理完（或被丢弃）drop，
    /// drop 时记完成（见 [`InboundFrameDone`]）。
    pub(crate) fn inbound_frame_read(&self) -> InboundFrameDone {
        self.inbound_in_flight.fetch_add(1, Ordering::SeqCst);
        InboundFrameDone {
            in_flight: Arc::clone(&self.inbound_in_flight),
            drained: Arc::clone(&self.inbound_drained),
        }
    }

    /// 还有读出、未处理完的入站帧。
    pub(crate) fn has_inbound_in_flight(&self) -> bool {
        self.inbound_in_flight.load(Ordering::SeqCst) > 0
    }

    /// 等已读出的入站帧处理完，最多等 `bound`；处理完返回 true。
    pub(crate) async fn wait_inbound_drained(&self, bound: std::time::Duration) -> bool {
        let drained = Arc::clone(&self.inbound_drained);
        let in_flight = Arc::clone(&self.inbound_in_flight);
        let wait = async move {
            loop {
                // 先登记唤醒再检查，避免检查与等待之间的通知丢失。
                let notified = drained.notified();
                if in_flight.load(Ordering::SeqCst) == 0 {
                    return;
                }
                notified.await;
            }
        };
        crate::common::platform::timeout(bound, wait).await.is_ok()
    }

    /// 竞速里一个协议尝试用的 core：除心跳策略、验活唤醒与事件处理器之外，状态都是它自己的。
    ///
    /// 心跳策略与验活唤醒要与 `base` 共享：上层在建连前后设置的前后台状态、NAT 探测结果，以及
    /// 包装层缓存的验活句柄，都要落到最终胜出的那条连接上。观察者不共享：上层观察者只在收到
    /// CONNECT_ACK 时挂到胜出的那条上（见 [`LinkBinding`]），落败连接的事件到不了上层。
    pub(crate) fn for_attempt(config: &ClientConfig, base: &ClientCore) -> Self {
        let mut core = Self::new(config);
        core.heartbeat_config = Arc::clone(&base.heartbeat_config);
        core.heartbeat_probe_wake = Arc::clone(&base.heartbeat_probe_wake);
        core.event_handler = base.event_handler.clone();
        core
    }

    /// 把上层绑定交给这条连接：它发 CONNECT 之前调用。收到 CONNECT_ACK 时整体挂上。
    pub(crate) fn set_link_binding(&self, binding: LinkBinding) {
        if let Ok(mut state) = self.link_binding.lock() {
            *state = BindingState::Pending(binding);
        }
    }

    /// 收回上层绑定：这条连接没能在时限内完成 CONNECT 时调用。CONNECT_ACK 恰好在这之前到达、
    /// 上层已经挂上时返回 [`BindingRevoke::AlreadyAdopted`]，调用方应把这条连接当作成功。
    pub(crate) fn revoke_link_binding(&self) -> BindingRevoke {
        let Ok(mut state) = self.link_binding.lock() else {
            return BindingRevoke::Unbound;
        };
        match std::mem::take(&mut *state) {
            BindingState::Pending(_) => BindingRevoke::Revoked,
            BindingState::Adopted => {
                *state = BindingState::Adopted;
                BindingRevoke::AlreadyAdopted
            }
            BindingState::Unbound => BindingRevoke::Unbound,
        }
    }

    /// 收到 CONNECT_ACK：把等着的上层绑定挂上，并只给新挂上的观察者补一个 `Connected`（对它们来说
    /// 连接这时才可用）。
    fn adopt_link_binding(&self) {
        let binding = {
            let Ok(mut state) = self.link_binding.lock() else {
                return;
            };
            match std::mem::take(&mut *state) {
                BindingState::Pending(binding) => {
                    *state = BindingState::Adopted;
                    binding
                }
                other => {
                    *state = other;
                    return;
                }
            }
        };
        if let Some(slot) = &binding.reason_slot {
            let _ = slot.set(Arc::clone(&self.disconnect_reason));
        }
        for observer in &binding.observers {
            self.add_observer(Arc::clone(observer));
        }
        deliver_to_observers(
            &binding.observers,
            &ConnectionEvent::Connected,
            "client core",
        );
    }

    /// 确定初始序列化格式和压缩算法
    fn determine_initial_format(
        config: &ClientConfig,
    ) -> (
        crate::common::protocol::SerializationFormat,
        crate::common::compression::CompressionAlgorithm,
    ) {
        if config.is_force_format() {
            (config.get_serialization_format(), config.get_compression())
        } else {
            // 默认使用 JSON，等待协商后更新
            (
                crate::common::protocol::SerializationFormat::Json,
                crate::common::compression::CompressionAlgorithm::None,
            )
        }
    }

    /// 更新消息解析器（协商完成后调用）
    pub async fn update_parser(
        &self,
        format: crate::common::protocol::SerializationFormat,
        compression: crate::common::compression::CompressionAlgorithm,
        encryption: crate::common::encryption::EncryptionAlgorithm,
    ) {
        let compression_clone = compression.clone();
        let encryption_clone = encryption.clone();
        let mut parser = self.parser.lock().await;
        *parser = MessageParser::new(format, compression, encryption);
        // 标记协商已完成
        self.negotiation_completed.store(true, Ordering::SeqCst);
        self.negotiation_notify.notify_waiters();
        tracing::info!(
            "[ClientCore] 协商完成，解析器已更新: 最终序列化方式={:?}, 最终压缩方式={:?}, 最终加密方式={:?}, negotiation_completed={}",
            format,
            compression_clone,
            encryption_clone,
            self.negotiation_completed.load(Ordering::SeqCst)
        );
        self.try_start_heartbeat().await;
    }

    /// 设置客户端连接（用于断开连接）
    pub fn set_client_connection(&mut self, connection: Arc<Mutex<Box<dyn Connection>>>) {
        if let Ok(mut conn) = self.client_connection.lock() {
            *conn = Some(connection);
        }
    }

    /// 清空当前连接槽，用于断开连接时打断 connection -> observer -> core -> connection 的强引用链。
    pub fn clear_client_connection(&self) {
        if let Ok(mut conn) = self.client_connection.lock() {
            *conn = None;
        }
    }

    /// 取出当前连接槽；调用方仍可用返回值完成 close，但共享槽会立即释放旧连接。
    pub fn take_client_connection(&self) -> Option<Arc<Mutex<Box<dyn Connection>>>> {
        self.client_connection
            .lock()
            .ok()
            .and_then(|mut conn| conn.take())
    }

    /// 设置事件处理器
    pub fn set_event_handler(
        &mut self,
        handler: Option<Arc<dyn crate::client::events::handler::ClientEventHandler>>,
    ) {
        self.event_handler = handler;
    }

    /// 启动心跳（协商完成后调用；协商前调用会被忽略）
    pub async fn start_heartbeat(&self, connection: Arc<Mutex<Box<dyn Connection>>>) {
        if let Ok(mut conn) = self.client_connection.lock() {
            *conn = Some(Arc::clone(&connection));
        }
        self.try_start_heartbeat().await;
    }

    /// 协商完成后启动心跳（幂等）
    async fn try_start_heartbeat(&self) {
        if !self.current_heartbeat_config().enabled {
            return;
        }
        if !self.negotiation_completed.load(Ordering::SeqCst) {
            return;
        }
        let Ok(mut slot) = self.heartbeat_manager.lock() else {
            return;
        };
        if slot.is_some() {
            return;
        }
        let Some(connection) = self
            .client_connection
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
        else {
            tracing::debug!("[ClientCore] heartbeat deferred: no active connection");
            return;
        };

        let mut heartbeat =
            HeartbeatManager::with_shared_config(Arc::clone(&self.heartbeat_config));
        let parser_ref = Arc::clone(&self.parser);
        heartbeat.start(
            connection,
            parser_ref,
            Arc::clone(&self.heartbeat_probe_wake),
        );
        *slot = Some(Arc::new(tokio::sync::Mutex::new(heartbeat)));
        tracing::debug!("[ClientCore] heartbeat started after negotiation");
    }

    /// 停止心跳
    pub fn stop_heartbeat(&self) {
        let taken = self
            .heartbeat_manager
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        if let Some(heartbeat) = taken {
            Self::stop_heartbeat_async(heartbeat);
        }
    }

    /// 异步停止心跳（内部辅助函数）
    fn stop_heartbeat_async(heartbeat: Arc<tokio::sync::Mutex<HeartbeatManager>>) {
        stop_heartbeat_on(heartbeat);
    }

    /// 处理接收到的消息
    ///
    /// 如果启用了路由，使用路由处理；否则直接通知观察者
    pub async fn handle_message(&self, data: Vec<u8>) {
        let negotiated = self.negotiation_completed.load(Ordering::SeqCst);
        self.handle_received_message(data, negotiated).await;
    }

    /// 处理一帧入站数据；`negotiated_at_receipt` 是这帧被读出那一刻的协商状态。
    ///
    /// 入站帧在独立任务里处理，而连接关闭事件在读循环里同步处理：服务端先发 KICKED 再关流时，
    /// EOF 往往先把协商标志复位，KICKED 随后才被解析。若按处理时的标志选解析器，它会被当成
    /// 协商前的 JSON 解析失败而整帧丢掉。解析器按读出时的状态选。
    pub async fn handle_received_message(&self, data: Vec<u8>, negotiated_at_receipt: bool) {
        // 解析消息
        let frame = if !negotiated_at_receipt {
            // 协商未完成：使用 PRE_NEGOTIATION_PARSER（这个阶段只会收到 CONNECT_ACK 或拒绝 CONNECT 的 ERROR）
            use crate::common::message::parser::PRE_NEGOTIATION_PARSER;
            match PRE_NEGOTIATION_PARSER.parse(&data) {
                Ok(frame) => frame,
                // 紧跟在 CONNECT_ACK 之后读出的帧：读出时协商尚未落地，此刻已经落地，按协商结果再解析一次
                Err(_) if self.is_negotiation_completed() => {
                    match self.parse_message(&data).await {
                        Ok(frame) => frame,
                        Err(e) => {
                            tracing::warn!("Failed to parse message (negotiated): {}", e);
                            return;
                        }
                    }
                }
                Err(e) => {
                    #[cfg(target_arch = "wasm32")]
                    web_sys::console::warn_1(
                        &format!("[flare-core] parse failed pre-negotiation: {e}").into(),
                    );
                    tracing::warn!("Failed to parse message (pre-negotiation): {}", e);
                    return;
                }
            }
        } else {
            // 协商已完成：直接使用协商后的 parser 解析
            match self.parse_message(&data).await {
                Ok(frame) => frame,
                Err(e) => {
                    #[cfg(target_arch = "wasm32")]
                    web_sys::console::warn_1(
                        &format!("[flare-core] parse failed negotiated: {e}").into(),
                    );
                    tracing::warn!("Failed to parse message (negotiated): {}", e);
                    return;
                }
            }
        };
        let is_pending_response = {
            tracing::trace!(
                "[ClientCore] 尝试匹配等待的响应: message_id={}",
                frame.message_id
            );

            if let Some(cmd) = &frame.command
                && let Some(Type::Payload(msg_cmd)) = &cmd.r#type
                && msg_cmd.message_id != frame.message_id
            {
                tracing::warn!(
                    "[ClientCore] PayloadCommand.message_id 和 Frame.message_id 不一致: cmd_id={}, frame_id={}",
                    msg_cmd.message_id,
                    frame.message_id
                );
            }

            // 列出所有等待的 message_id（用于调试）
            let pending_ids: Vec<String> = {
                let pending = self.pending_map.lock().await;
                pending.keys().cloned().collect()
            };
            if !pending_ids.is_empty() {
                tracing::debug!(
                    "[ClientCore] handle_message: 当前等待的响应 message_id 列表: {:?}",
                    pending_ids
                );
            }

            let mut pending = self.pending_map.lock().await;
            if let Some(sender) = pending.remove(&frame.message_id) {
                tracing::debug!(
                    "[ClientCore] 匹配到等待的响应: message_id={}",
                    frame.message_id
                );
                if sender.send(frame.clone()).is_err() {
                    tracing::warn!(
                        "[ClientCore] 发送响应到等待通道失败: message_id={} (接收者可能已关闭)",
                        frame.message_id
                    );
                    false // 发送失败，继续处理
                } else {
                    tracing::debug!(
                        "[ClientCore] 响应已发送到等待通道: message_id={}",
                        frame.message_id
                    );
                    true // 发送成功，这是等待的响应，不需要继续处理
                }
            } else {
                tracing::debug!(
                    "[ClientCore] 未找到等待的响应: message_id={}",
                    frame.message_id
                );
                false // 不是等待的响应，继续处理
            }
        };

        // 如果是等待的响应且已成功发送，直接返回，避免被 MessageListener 重复处理
        // 注意：系统命令（如 CONNECT_ACK）仍然需要被处理，因为它们可能不在 pending_map 中
        if is_pending_response {
            // 仍然需要通知 observers，以便 MessagePipeline 可以处理响应
            // 但不继续处理业务命令，避免被 MessageListener 重复处理
            self.notify_observers(&ConnectionEvent::Message(data));
            return;
        }

        // 处理系统命令（CONNECT_ACK, PONG, KICKED）
        let is_system_command = self
            .handle_system_commands(&frame, negotiated_at_receipt)
            .await;

        // 关键修复：即使处理了系统命令，也要通知 observers
        // 这样 MessagePipeline 和 MessageListener 也能收到 CONNECT_ACK 等系统命令
        // 通知所有观察者（包括系统命令，让 MessageListener 也能处理）
        self.notify_observers(&ConnectionEvent::Message(data));

        // 如果是系统命令，处理完并通知 observers 后直接返回
        if is_system_command {
            return;
        }

        // 处理业务命令（Message, Notification）
        self.handle_business_commands(&frame).await;

        // 处理消息路由
        self.handle_message_routing(&frame).await;
    }

    /// 解析消息（内部辅助函数）
    async fn parse_message(&self, data: &[u8]) -> Result<Frame> {
        let parser = self.parser.lock().await;
        parser.parse(data)
    }

    /// 处理系统命令（CONNECT_ACK, PONG, KICKED）
    ///
    /// # 返回
    /// `true` 表示已处理，不需要继续处理；`false` 表示不是系统命令或需要继续处理
    async fn handle_system_commands(&self, frame: &Frame, negotiated_at_receipt: bool) -> bool {
        let Some(cmd) = &frame.command else {
            return false;
        };

        let Some(Type::System(sys_cmd)) = &cmd.r#type else {
            return false;
        };

        let cmd_type = match SystemCommandType::try_from(sys_cmd.r#type) {
            Ok(t) => t,
            Err(_) => return false,
        };

        match cmd_type {
            SystemCommandType::ConnectAck => {
                self.handle_connect_ack_command(frame).await;
                true
            }
            SystemCommandType::Pong => {
                self.handle_pong_command(frame).await;
                true
            }
            SystemCommandType::Kicked => {
                self.handle_kicked_command(frame, sys_cmd, negotiated_at_receipt)
                    .await;
                true
            }
            // CONNECT_ACK 之前的 ERROR 是服务端拒绝 CONNECT 的原因（读循环里通常已同步登记过，这里幂等）
            SystemCommandType::Error if !negotiated_at_receipt => {
                self.record_connect_rejection(sys_cmd);
                true
            }
            // 协商完成后的 ERROR：终止类拒绝（升级门槛等）登记并关连接；其余照旧交给后续处理
            SystemCommandType::Error if negotiated_at_receipt => {
                self.handle_session_error_command(sys_cmd).await;
                false
            }
            SystemCommandType::Close => {
                self.handle_close_command(frame, sys_cmd, negotiated_at_receipt)
                    .await;
                true
            }
            _ => false,
        }
    }

    /// 协商完成后收到的 ERROR：只有终止类拒绝（[`ServerRejection::is_terminal`]，如服务端抬高版本门槛后
    /// 对在线连接下发的 `UPGRADE_REQUIRED`）才登记并关连接，随后的断开以 [`DisconnectReason::Rejected`]
    /// 上报，引擎据此与协商前的拒绝一样终止并提示升级。非终止类 ERROR 不处理，行为不变。
    async fn handle_session_error_command(&self, sys_cmd: &crate::common::protocol::SystemCommand) {
        let rejection = ServerRejection::from_command(sys_cmd);
        if !rejection.is_terminal() {
            return;
        }
        let detail = format!("{}: server rejected the session", rejection.reason);
        match self.session_rejection.lock() {
            Ok(mut stored) => {
                if stored.is_some() {
                    return;
                }
                tracing::warn!("[ClientCore] session rejected by server: {}", detail);
                *stored = Some(rejection);
            }
            Err(_) => return,
        }
        let Some(connection) = self.take_client_connection() else {
            tracing::debug!("[ClientCore] terminal ERROR received without an active connection");
            return;
        };
        let mut conn = connection.lock().await;
        if let Err(e) = conn.close_with_reason(&detail).await {
            tracing::debug!("[ClientCore] closing after terminal ERROR failed: {}", e);
        }
    }

    /// 登记服务端拒绝 CONNECT 的原因（ERROR 系统帧），并唤醒等待协商的一方。
    ///
    /// 必须早于随后的关连接事件生效：[`ClientMessageObserver`](super::common::ClientMessageObserver)
    /// 在读循环里同步调用它，关连接事件据此报出类型化原因，而不是泛泛的「连接被关」。
    pub(crate) fn record_connect_rejection(
        &self,
        sys_cmd: &crate::common::protocol::SystemCommand,
    ) {
        let rejection = ServerRejection::from_command(sys_cmd);
        let error = rejection.to_error();
        if let Ok(mut failure) = self.negotiation_failure.lock() {
            if failure.is_some() {
                return;
            }
            tracing::warn!("[ClientCore] CONNECT rejected by server: {}", error);
            if let Ok(mut stored) = self.rejection.lock() {
                *stored = Some(rejection);
            }
            *failure = Some(error);
        }
        self.negotiation_notify.notify_waiters();
    }

    /// 处理 CONNECT_ACK 命令
    async fn handle_connect_ack_command(&self, frame: &Frame) {
        // 通知事件处理器
        if let Some(ref handler) = self.event_handler {
            let _ = handler
                .handle_system_command(SystemCommandType::ConnectAck, frame)
                .await;
        }

        // 处理 CONNECT_ACK
        match self.handle_connect_ack(frame) {
            Ok((format, compression, encryption)) => {
                if let Some(Type::System(sys_cmd)) =
                    frame.command.as_ref().and_then(|cmd| cmd.r#type.as_ref())
                    && let Ok(mut info) = self.connect_ack.lock()
                {
                    *info = Some(ConnectAckInfo::from_metadata(&sys_cmd.metadata));
                }
                // 先挂上上层观察者，再标记协商完成（唤醒竞速）：竞速返回时上层已经在了，
                // 这帧 CONNECT_ACK 与紧随其后的帧都能到上层。
                self.adopt_link_binding();
                tracing::info!(
                    "[ClientCore] 收到 CONNECT_ACK: 服务端确定的序列化方式={:?}, 压缩方式={:?}, 加密方式={:?}",
                    format,
                    compression,
                    encryption
                );

                // 更新 parser 为协商后的格式（如果不是强制模式）
                if !self.config.is_force_format() {
                    self.update_parser(format, compression.clone(), encryption.clone())
                        .await;
                    tracing::info!(
                        "[ClientCore] 解析器已更新为协商后的格式: {:?}, 压缩: {:?}, 加密: {:?}",
                        format,
                        compression,
                        encryption
                    );
                } else {
                    tracing::info!(
                        "[ClientCore] 强制模式：继续使用客户端强制指定的格式: {:?}, 压缩: {:?}",
                        self.config.get_serialization_format(),
                        self.config.get_compression()
                    );
                    self.negotiation_completed.store(true, Ordering::SeqCst);
                    self.negotiation_notify.notify_waiters();
                }

                // 发送 NEGOTIATION_READY 命令，通知服务端客户端已准备好按协商方式通信
                // 注意：这里使用协商后的 parser（如果已更新）或 JSON parser（如果还在协商前）
                // 但 NEGOTIATION_READY 应该在协商完成后发送，所以应该使用协商后的 parser
                if let Err(e) = self.send_negotiation_ready().await {
                    tracing::warn!("[ClientCore] 发送 NEGOTIATION_READY 失败: {}", e);
                }

                self.try_start_heartbeat().await;
            }
            Err(e) => {
                self.fail_negotiation(e.to_string()).await;
            }
        }
    }

    /// 处理 PONG 命令
    async fn handle_pong_command(&self, frame: &Frame) {
        // 通知事件处理器
        if let Some(ref handler) = self.event_handler {
            let _ = handler
                .handle_system_command(SystemCommandType::Pong, frame)
                .await;
        }

        // 记录 PONG，更新心跳
        self.record_pong();
    }

    /// 处理 KICKED 命令。
    ///
    /// 断开原因是 KICKED 的类型化原因（[`crate::common::kick::kick_reason`]，如 `DEVICE_CONFLICT`），
    /// 观察者恰好收到一次：
    ///
    /// - 连接还开着：以被踢原因关闭连接，关连接带出的断开事件就是那一次上报，不再是泛泛的
    ///   `Closed by client`（上层把那句当成主动断开，既不报被踢也不重连）；
    /// - 连接已被对端关掉（服务端发完 KICKED 紧接着关连接）：关连接事件在读循环里同步到达，读循环
    ///   等已读出的帧处理完再处理它（见 `ClientMessageObserver`），这里补报被踢，随后的对端关闭不再
    ///   上报；等过了 [`INBOUND_DRAIN_BOUND`] 才处理到这里时，观察者先收到对端关闭，再收到被踢。
    ///
    /// 只看这帧读出那一刻的协商状态（`negotiated_at_receipt`）：关连接事件会先把协商标志复位，
    /// 按处理时的标志判断，被踢就永远报不出去。协商未完成（协议竞速落选的连接）或我方已请求断开
    /// （切账号、登出）时收到的 KICKED 不通知。原因结束会话（设备冲突、租户不可用）时进入终态，
    /// 见 [`Self::session_ended_by_server`]。
    async fn handle_kicked_command(
        &self,
        frame: &Frame,
        sys_cmd: &crate::common::protocol::SystemCommand,
        negotiated_at_receipt: bool,
    ) {
        let kick_reason = crate::common::kick::kick_reason(sys_cmd);
        tracing::warn!(reason = %kick_reason, "[ClientCore] kicked by the server");

        // 通知事件处理器
        if let Some(ref handler) = self.event_handler
            && let Err(e) = handler
                .handle_system_command(SystemCommandType::Kicked, frame)
                .await
        {
            tracing::warn!("[ClientCore] 事件处理器处理 KICKED 失败: {}", e);
        }

        let should_notify =
            negotiated_at_receipt && !self.disconnect_requested.load(Ordering::SeqCst);
        if should_notify {
            if crate::common::kick::ends_session(&kick_reason) {
                self.end_session_by_server(&kick_reason);
            }
            self.set_pending_kick(&kick_reason);
        }

        // 更新连接状态为断开（被踢）
        self.state_manager.set_disconnected();

        // 主动断开连接：要通知时以被踢原因关闭
        self.disconnect_on_kicked(should_notify.then_some(kick_reason.as_str()))
            .await;

        // 被踢后立刻取消按 message_id 等待的 RPC，避免 send_frame_and_wait 空等至超时
        self.cancel_all_pending_responses().await;

        if !should_notify {
            tracing::debug!(
                "[ClientCore] 收到 KICKED 但不向观察者通知（协商未完成或我方已请求断开）"
            );
            return;
        }
        // 关连接没有带出断开事件（对端已先关掉了连接）：补报被踢
        if let Some(reason) = self.take_unreported_kick() {
            tracing::debug!(
                reason = %reason,
                "[ClientCore] the peer closed the connection before the KICKED was handled; reporting the kick"
            );
            self.notify_observers(&ConnectionEvent::Disconnected(reason));
        }
        tracing::info!("[ClientCore] 连接已断开（被踢）: {}", kick_reason);
    }

    /// 处理服务端下发的 CLOSE：服务端要关闭这条连接（排空 `DRAIN`、停机 `SERVER_SHUTDOWN`、
    /// 再均衡 `REBALANCE`）。
    ///
    /// 协商完成后收到：记下类型化的关闭（原因、重连窗口、优先接入点），以类型化原因自行断开；观察者
    /// 收到的断开原因形如 `SERVER_SHUTDOWN: server closed the connection`，上层从
    /// [`DisconnectReason::ServerClose`] 读到窗口，在窗口内随机时刻重连；不必等服务端在宽限期满后
    /// 强行关闭。协商完成前收到：按服务端拒绝 CONNECT 登记原因（metadata `code` 为
    /// `SERVICE_UNAVAILABLE`，可重试）。
    async fn handle_close_command(
        &self,
        frame: &Frame,
        sys_cmd: &crate::common::protocol::SystemCommand,
        negotiated_at_receipt: bool,
    ) {
        if let Some(ref handler) = self.event_handler
            && let Err(e) = handler
                .handle_system_command(SystemCommandType::Close, frame)
                .await
        {
            tracing::warn!("[ClientCore] 事件处理器处理 CLOSE 失败: {}", e);
        }

        if !negotiated_at_receipt {
            self.record_connect_rejection(sys_cmd);
            return;
        }
        if let Some(close) = ServerClose::from_command(sys_cmd)
            && let Ok(mut stored) = self.server_close.lock()
        {
            *stored = Some(close);
        }

        let reason = Self::close_reason(sys_cmd);
        tracing::info!(reason = %reason, "[ClientCore] server is closing the connection");
        let Some(connection) = self.take_client_connection() else {
            tracing::debug!("[ClientCore] CLOSE received without an active connection");
            return;
        };
        let detail = format!("{reason}: server closed the connection");
        let mut conn = connection.lock().await;
        if let Err(e) = conn.close_with_reason(&detail).await {
            tracing::debug!("[ClientCore] closing after server CLOSE failed: {}", e);
        }
    }

    /// CLOSE 的原因：`message`，没有就用 metadata `reason`，都没有为 `CLOSE`。
    fn close_reason(sys_cmd: &crate::common::protocol::SystemCommand) -> String {
        let message = sys_cmd.message.trim();
        if !message.is_empty() {
            return message.to_string();
        }
        sys_cmd
            .metadata
            .get("reason")
            .and_then(|reason| std::str::from_utf8(reason).ok())
            .map(str::trim)
            .filter(|reason| !reason.is_empty())
            .unwrap_or("CLOSE")
            .to_string()
    }

    /// 断开连接（被踢时调用）。`reason` 为 `Some` 时以它关闭，观察者收到的断开原因就是它。
    async fn disconnect_on_kicked(&self, reason: Option<&str>) {
        // 尝试从 client_connection 断开
        let client_conn_opt = self.take_client_connection();

        if let Some(client_conn) = client_conn_opt {
            let mut conn = client_conn.lock().await;
            let closed = match reason {
                Some(reason) => conn.close_with_reason(reason).await,
                None => conn.close().await,
            };
            if let Err(e) = closed {
                tracing::error!("[ClientCore] 断开连接失败: {}", e);
            } else {
                tracing::info!("[ClientCore] 已主动断开连接（被踢）");
            }
        } else {
            // 如果 client_connection 未设置，记录警告但不阻塞
            // 连接会在底层传输层自动关闭
            tracing::warn!("[ClientCore] 客户端连接未设置，等待底层传输层关闭连接");
        }
    }

    /// 处理业务命令（Message, Notification）
    async fn handle_business_commands(&self, frame: &Frame) {
        let Some(ref handler) = self.event_handler else {
            return;
        };

        let Some(cmd) = &frame.command else {
            return;
        };

        match &cmd.r#type {
            Some(Type::Payload(msg_cmd)) => {
                if let Ok(cmd_type) = PayloadCommandType::try_from(msg_cmd.r#type) {
                    let _ = handler.handle_message_command(cmd_type, frame).await;
                }
            }
            Some(Type::Notification(notif_cmd)) => {
                if let Ok(cmd_type) = NotificationCommandType::try_from(notif_cmd.r#type) {
                    let _ = handler.handle_notification_command(cmd_type, frame).await;
                }
            }
            _ => {}
        }
    }

    /// 处理消息路由
    async fn handle_message_routing(&self, frame: &Frame) {
        let Some(ref router) = self.message_router else {
            return;
        };

        match router.route(frame).await {
            Ok(replies) => {
                // 发送回复（如果需要）
                // 注意：这里需要连接实例来发送，但 ClientCore 不持有连接
                // 回复应该通过客户端发送，这里只是路由处理
                tracing::debug!("Router generated {} replies", replies.len());
            }
            Err(e) => {
                tracing::warn!("Router error: {}", e);
            }
        }
    }

    /// CONNECT_ACK 无效或协商失败：立即唤醒 `wait_for_negotiation` 并上报连接错误。
    async fn fail_negotiation(&self, reason: String) {
        tracing::warn!("[ClientCore] 协商失败: {}", reason);
        let error = FlareError::protocol_error(reason);
        if let Ok(mut stored) = self.negotiation_failure.lock() {
            *stored = Some(error.clone());
        }
        self.negotiation_notify.notify_waiters();
        self.stop_heartbeat();
        self.cancel_all_pending_responses().await;
        self.handle_connection_event(&ConnectionEvent::Error(error));
    }

    fn negotiation_failure_error(&self) -> Option<FlareError> {
        self.negotiation_failure
            .lock()
            .ok()
            .and_then(|failure| failure.clone())
    }

    fn reset_negotiation_state(&self) {
        self.negotiation_completed.store(false, Ordering::SeqCst);
        if let Ok(mut failure) = self.negotiation_failure.lock() {
            *failure = None;
        }
    }

    /// 连接在 CONNECT_ACK 之前没了（关闭或读出错）：立即结束协商等待。
    ///
    /// 服务端登记过拒绝原因（ERROR 帧）就报那个原因；没有原因就是连接失败——
    /// 以前一律报成「令牌被拒」的鉴权失败，心跳失败、踢线后的断开也被误报成令牌问题。
    fn fail_negotiation_on_close(&self, detail: &str) {
        let error = if let Ok(mut failure) = self.negotiation_failure.lock() {
            let error = failure
                .take()
                .unwrap_or_else(|| closed_before_connect_ack_error(detail));
            *failure = Some(error.clone());
            error
        } else {
            return;
        };
        tracing::warn!("[ClientCore] 协商阶段连接断开: {}", error);
        self.negotiation_notify.notify_waiters();
    }

    /// 连接已建立、CONNECT_ACK 还没到、也不是我方主动断开。
    /// 协商失败的原因只在下一条连接建立（[`ConnectionEvent::Connected`]）时清空。
    fn closed_during_negotiation(&self) -> bool {
        matches!(
            self.state_manager.get_state(),
            crate::client::connection::state::ConnectionState::Connected
        ) && !self.is_negotiation_completed()
            && !self.disconnect_requested.load(Ordering::SeqCst)
    }

    /// 处理连接事件
    pub fn handle_connection_event(&self, event: &ConnectionEvent) {
        // 断开、错误报什么（见 `close_report`）：正在被踢时以被踢原因上报；被踢已经报过时，随后同一条
        // 连接的对端关闭、读出错只做状态清理，不再上报；我方之后主动断开（disconnect）照常上报。
        let mut report = CloseReport::AsIs;

        // 通知事件处理器
        if let Some(ref handler) = self.event_handler {
            let handler_clone = Arc::clone(handler);
            let event_clone = event.clone();
            crate::client::runtime::spawn_client_task(async move {
                let _ = handler_clone.handle_connection_event(&event_clone).await;
            });
        }

        // 更新状态
        match event {
            ConnectionEvent::Connected => {
                // 传输层在挂观察者时已经报过一次 Connected；客户端发完 CONNECT 后再报的那次
                // 不能复位协商状态，否则已经到达的 CONNECT_ACK 或拒绝原因会被抹掉。
                let already_connected = matches!(
                    self.state_manager.get_state(),
                    crate::client::connection::state::ConnectionState::Connected
                );
                self.state_manager.set_connected();
                if !already_connected {
                    self.reset_negotiation_state();
                    self.reset_link_state();
                    // 被踢进入终态后，只有应用显式建连才会有新连接
                    if let Ok(mut ended) = self.session_ended_by_server.lock() {
                        *ended = None;
                    }
                    if let Ok(mut pending) = self.pending_kick.lock() {
                        *pending = None;
                        self.kick_reported.store(false, Ordering::SeqCst);
                    }
                }
            }
            ConnectionEvent::Disconnected(reason) => {
                // 服务端在 CONNECT_ACK 之前就关掉了连接：立即唤醒等协商的一方，别让它干等 10s 超时。
                // 原因以服务端下发的 ERROR 帧为准（租户不可用、鉴权失败……）；没有原因就是连接失败。
                let closed_during_negotiation = self.closed_during_negotiation();
                report = self.close_report();
                self.state_manager.set_disconnected();
                self.negotiation_completed.store(false, Ordering::SeqCst);
                // 心跳绑在这条连接上：留着它，它会在死连接上继续发 PING，失败时再关一次连接并误报，
                // 同时占着槽位，让下一条连接协商完成后起不了自己的心跳。
                self.stop_heartbeat();
                if closed_during_negotiation {
                    self.fail_negotiation_on_close(reason);
                }
                let pending = Arc::clone(&self.pending_map);
                crate::client::runtime::spawn_client_task(async move {
                    let mut map = pending.lock().await;
                    if !map.is_empty() {
                        tracing::debug!(
                            count = map.len(),
                            "[ClientCore] connection disconnected: clearing pending response waiters"
                        );
                        map.clear();
                    }
                });
            }
            ConnectionEvent::Error(error) => {
                report = self.close_report();
                let closed_during_negotiation = self.closed_during_negotiation();
                self.state_manager.set_failed();
                self.negotiation_completed.store(false, Ordering::SeqCst);
                self.stop_heartbeat();
                if closed_during_negotiation {
                    self.fail_negotiation_on_close(&error.to_string());
                }
                let pending = Arc::clone(&self.pending_map);
                crate::client::runtime::spawn_client_task(async move {
                    let mut map = pending.lock().await;
                    if !map.is_empty() {
                        tracing::debug!(
                            count = map.len(),
                            "[ClientCore] connection error: clearing pending response waiters"
                        );
                        map.clear();
                    }
                });
            }
            ConnectionEvent::Message(_) => {
                // 消息处理在 handle_message 中完成
            }
        }

        self.record_disconnect_reason(event, &report);

        match report {
            CloseReport::AsIs => self.notify_observers(event),
            CloseReport::Kick(reason) => {
                self.notify_observers(&ConnectionEvent::Disconnected(reason));
            }
            CloseReport::AlreadyKicked => tracing::debug!(
                ?event,
                "[ClientCore] connection already reported as kicked; not reporting the close again"
            ),
        }
    }

    /// 新连接建立：清掉上一条连接留下的接入信息、拒绝、计划性关闭与断开原因。
    fn reset_link_state(&self) {
        if let Ok(mut info) = self.connect_ack.lock() {
            *info = None;
        }
        if let Ok(mut rejection) = self.rejection.lock() {
            *rejection = None;
        }
        if let Ok(mut close) = self.server_close.lock() {
            *close = None;
        }
        if let Ok(mut rejection) = self.session_rejection.lock() {
            *rejection = None;
        }
        if let Ok(mut reason) = self.disconnect_reason.lock() {
            *reason = None;
        }
    }

    /// 要上报给观察者的断开、错误：在通知之前记下类型化原因，观察者在回调里读到的就是这一次的。
    ///
    /// - 正在被踢：[`DisconnectReason::Kicked`]；
    /// - 我方主动断开：[`DisconnectReason::ClientRequested`]；
    /// - 协商完成后收到过计划性 CLOSE：[`DisconnectReason::ServerClose`]（带窗口与优先接入点）；
    /// - 读写出错：[`DisconnectReason::TransportError`]；
    /// - 其余按断开原因的文本归类（心跳判死、传输层带来的停机原因、对端关闭），见
    ///   [`DisconnectReason::from_detail`]。
    fn record_disconnect_reason(&self, event: &ConnectionEvent, report: &CloseReport) {
        let requested = self.disconnect_requested.load(Ordering::SeqCst);
        let reason = match (report, event) {
            (CloseReport::AlreadyKicked, _) => return,
            (CloseReport::Kick(reason), _) => DisconnectReason::Kicked {
                ends_session: crate::common::kick::ends_session(reason),
                reason: reason.clone(),
            },
            (CloseReport::AsIs, ConnectionEvent::Disconnected(_) | ConnectionEvent::Error(_))
                if requested =>
            {
                DisconnectReason::ClientRequested
            }
            (CloseReport::AsIs, ConnectionEvent::Disconnected(_) | ConnectionEvent::Error(_))
                if self
                    .session_rejection
                    .lock()
                    .ok()
                    .is_some_and(|rejection| rejection.is_some()) =>
            {
                match self
                    .session_rejection
                    .lock()
                    .ok()
                    .and_then(|rejection| rejection.clone())
                {
                    Some(rejection) => DisconnectReason::Rejected(rejection),
                    None => return,
                }
            }
            (CloseReport::AsIs, ConnectionEvent::Disconnected(detail)) => {
                match self
                    .server_close
                    .lock()
                    .ok()
                    .and_then(|close| close.clone())
                {
                    Some(close) => DisconnectReason::ServerClose(close),
                    None => DisconnectReason::from_detail(detail),
                }
            }
            (CloseReport::AsIs, ConnectionEvent::Error(error)) => {
                DisconnectReason::TransportError {
                    detail: error.to_string(),
                }
            }
            (CloseReport::AsIs, _) => return,
        };
        if let Ok(mut slot) = self.disconnect_reason.lock() {
            *slot = Some(reason);
        }
    }

    /// 读循环里到达的断开、错误是否要等已读出的帧处理完再处理：连接协商完成且仍在线、不是我方
    /// 正在断开、还有帧没处理完。典型是服务端发完 KICKED 紧接着关连接——不等的话，观察者先收到
    /// 一次对端关闭，被踢原因晚一步才到。
    ///
    /// 我方自己关连接（被踢、主动断开）时状态已经不是 `Connected` 或已置位主动断开，不等。
    pub(crate) fn should_defer_close_until_inbound_drained(&self) -> bool {
        matches!(
            self.state_manager.get_state(),
            crate::client::connection::state::ConnectionState::Connected
        ) && self.is_negotiation_completed()
            && !self.disconnect_requested.load(Ordering::SeqCst)
            && self.has_inbound_in_flight()
    }

    /// 添加观察者
    pub fn add_observer(&self, observer: ArcObserver) {
        self.observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(observer);
    }

    /// 去掉全部观察者（竞速放弃一条连接时用：它关闭时的事件不再到任何上层）
    pub(crate) fn clear_observers(&self) {
        self.observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// 移除观察者
    pub fn remove_observer(&self, observer: ArcObserver) {
        self.observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|o| !Arc::ptr_eq(o, &observer));
    }

    /// 通知所有观察者。
    ///
    /// 先取列表快照再在锁外回调：观察者在回调里增删观察者不会自锁，一个观察者 panic 也毒化不了
    /// 列表；它的 panic 被隔离，其余观察者照常收到本次与之后的事件（含踢线原因）。
    fn notify_observers(&self, event: &ConnectionEvent) {
        let observers = observer_snapshot(&self.observers);
        deliver_to_observers(&observers, event, "client core");
    }

    /// 获取消息路由器（如果启用）
    pub fn router_mut(&mut self) -> Option<&mut MessageRouter> {
        self.message_router.as_mut()
    }

    /// 获取消息路由器（只读）
    pub fn router(&self) -> Option<&MessageRouter> {
        self.message_router.as_ref()
    }

    /// 获取连接状态
    pub fn state(&self) -> crate::client::connection::ConnectionState {
        self.state_manager.get_state()
    }

    /// 检查是否可以发送消息
    pub fn can_send(&self) -> bool {
        self.state_manager.get_state().can_send()
    }

    /// 检查协商是否已完成
    pub fn is_negotiation_completed(&self) -> bool {
        self.negotiation_completed
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// 等待 CONNECT_ACK 协商完成（WASM/JS 侧应在发送业务消息前调用）
    pub async fn wait_for_negotiation(&self, timeout: std::time::Duration) -> Result<()> {
        if self.is_negotiation_completed() {
            return Ok(());
        }
        if let Some(err) = self.negotiation_failure_error() {
            return Err(err);
        }

        #[cfg(target_arch = "wasm32")]
        {
            use crate::common::platform::monotonic_now;
            let deadline = monotonic_now() + timeout;
            loop {
                self.drain_wasm_inbound().await;
                if self.is_negotiation_completed() {
                    return Ok(());
                }
                if let Some(err) = self.negotiation_failure_error() {
                    return Err(err);
                }
                if monotonic_now() >= deadline {
                    return Err(negotiation_timeout_error(timeout));
                }
                crate::common::platform::yield_to_event_loop().await;
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            wait_for_negotiation_notify(
                Arc::clone(&self.negotiation_completed),
                Arc::clone(&self.negotiation_failure),
                Arc::clone(&self.negotiation_notify),
                timeout,
            )
            .await
        }
    }

    /// 检查是否可以连接
    pub fn can_connect(&self) -> bool {
        self.state_manager.get_state().can_connect()
    }

    /// 返回当前心跳策略快照。
    pub fn current_heartbeat_config(&self) -> HeartbeatConfig {
        self.heartbeat_config
            .read()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| self.config.heartbeat.clone())
    }

    /// 当前实际心跳间隔。
    pub fn heartbeat_effective_interval(&self) -> std::time::Duration {
        self.current_heartbeat_config().effective_interval()
    }

    /// 最近一次心跳往返时长（心跳未启动、还没有应答过的 ping、或心跳锁正被占用时为 `None`）。
    /// 不等锁：只是采样，下一次再取即可。
    pub fn heartbeat_round_trip(&self) -> Option<std::time::Duration> {
        let heartbeat = self
            .heartbeat_manager
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(Arc::clone))?;
        let guard = heartbeat.try_lock().ok()?;
        guard.last_round_trip()
    }

    /// 运行期更新心跳策略。启动前更新会影响后续启动，启动后更新会影响下一轮心跳。
    pub fn update_heartbeat_config(&self, update: impl FnOnce(&mut HeartbeatConfig)) {
        if let Ok(mut config) = self.heartbeat_config.write() {
            update(&mut config);
        }
    }

    /// 更新应用前后台状态。回到前台时额外唤醒心跳做一次即时验活：
    /// 后台期间心跳被浏览器节流甚至冻结，连接可能已被服务端回收成半开死连
    /// （readyState 仍报 OPEN、onclose 从未触发）；不主动戳一下就会一直停在
    /// 「自认为在线、实则发送必失败」的状态，直到用户手动刷新。
    pub fn set_heartbeat_app_state(&self, state: HeartbeatAppState) {
        self.update_heartbeat_config(|config| {
            config.app_state = state;
        });
        if state == HeartbeatAppState::Foreground {
            self.heartbeat_probe_wake.notify_one();
        }
    }

    /// 不改前后台状态，只对当前连接做一次即时验活：立即发一枚 ping，验活窗口内收不到 PONG
    /// 就按心跳超时断开，交给上层的断开 → 重连路径。
    ///
    /// 给「连接看起来还在、但有理由怀疑它已经死了」的时刻用：网络切换（旧 IP 上的连接多半已被
    /// 中间设备丢弃，本端却收不到任何关闭信号）、消息发出后迟迟等不到回执。心跳尚未启动时
    /// 唤醒会留到心跳启动后立即验活一次，无害。
    pub fn probe_heartbeat(&self) {
        self.heartbeat_probe_wake.notify_one();
    }

    /// 验活唤醒句柄，供包装层在不拿客户端锁的情况下请求验活（等价于 [`Self::probe_heartbeat`]）。
    pub(crate) fn heartbeat_probe_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.heartbeat_probe_wake)
    }

    /// 更新 NAT 空闲超时探测结果。
    pub fn set_heartbeat_nat_timeout(&self, timeout: Option<std::time::Duration>) {
        self.update_heartbeat_config(|config| {
            config.nat_timeout = timeout;
        });
    }

    /// 记录收到 PONG（心跳响应）
    ///
    /// 由消息观察者调用，用于更新心跳状态
    ///
    /// 注意：由于观察者是同步的，我们需要异步获取锁
    pub fn record_pong(&self) {
        let heartbeat = match self.heartbeat_manager.lock() {
            Ok(guard) => guard.as_ref().map(Arc::clone),
            Err(_) => None,
        };
        let Some(heartbeat) = heartbeat else {
            return;
        };
        record_pong_on(heartbeat);
    }
}

impl ClientCore {
    /// 注册一个按 message_id 等待的响应通道
    /// 返回 Receiver，调用方可在外部等待
    pub async fn register_pending_response(&self, message_id: &str) -> oneshot::Receiver<Frame> {
        let (tx, rx) = oneshot::channel();
        let mut pending = self.pending_map.lock().await;
        pending.insert(message_id.to_string(), tx);
        rx
    }

    /// 取消等待（超时或主动取消时调用）
    pub async fn cancel_pending_response(&self, message_id: &str) {
        let mut pending = self.pending_map.lock().await;
        pending.remove(message_id);
    }

    /// 连接丢失或被踢时取消全部等待，使 `send_frame_and_wait` 尽快收到通道关闭错误。
    pub async fn cancel_all_pending_responses(&self) {
        let mut pending = self.pending_map.lock().await;
        if !pending.is_empty() {
            tracing::debug!(
                count = pending.len(),
                "[ClientCore] clearing all pending response waiters"
            );
            pending.clear();
        }
    }
}

// 为 ClientCore 实现 Clone（用于共享状态管理器和观察者）
impl Clone for ClientCore {
    fn clone(&self) -> Self {
        Self {
            state_manager: Arc::clone(&self.state_manager),
            parser: Arc::clone(&self.parser),
            heartbeat_manager: Arc::clone(&self.heartbeat_manager),
            heartbeat_config: Arc::clone(&self.heartbeat_config),
            heartbeat_probe_wake: Arc::clone(&self.heartbeat_probe_wake),
            message_router: self.message_router.as_ref().map(|_| MessageRouter::new()), // 路由不克隆，创建新的
            observers: Arc::clone(&self.observers),
            config: self.config.clone(),
            event_handler: self.event_handler.clone(), // 事件处理器可以共享
            client_connection: Arc::clone(&self.client_connection), // 共享连接引用
            pending_map: Arc::clone(&self.pending_map),
            negotiation_completed: Arc::clone(&self.negotiation_completed), // 共享协商完成标志
            negotiation_notify: Arc::clone(&self.negotiation_notify),
            negotiation_failure: Arc::clone(&self.negotiation_failure),
            disconnect_requested: Arc::clone(&self.disconnect_requested),
            connect_ack: Arc::clone(&self.connect_ack),
            rejection: Arc::clone(&self.rejection),
            session_rejection: Arc::clone(&self.session_rejection),
            server_close: Arc::clone(&self.server_close),
            disconnect_reason: Arc::clone(&self.disconnect_reason),
            link_binding: Arc::clone(&self.link_binding),
            session_ended_by_server: Arc::clone(&self.session_ended_by_server),
            pending_kick: Arc::clone(&self.pending_kick),
            kick_reported: Arc::clone(&self.kick_reported),
            inbound_in_flight: Arc::clone(&self.inbound_in_flight),
            inbound_drained: Arc::clone(&self.inbound_drained),
            #[cfg(target_arch = "wasm32")]
            wasm_inbound: Arc::clone(&self.wasm_inbound),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod client_core_tests {
    use super::*;
    use crate::common::compression::CompressionAlgorithm;
    use crate::common::encryption::EncryptionAlgorithm;
    use crate::common::protocol::SerializationFormat;
    use std::time::Duration;

    #[tokio::test]
    async fn update_parser_marks_negotiation_completed() {
        let core = ClientCore::new(&ClientConfig::default());
        assert!(!core.is_negotiation_completed());

        core.update_parser(
            SerializationFormat::Protobuf,
            CompressionAlgorithm::Gzip,
            EncryptionAlgorithm::None,
        )
        .await;

        assert!(core.is_negotiation_completed());
    }

    #[tokio::test]
    async fn wait_for_negotiation_returns_after_flag_set() {
        let core = ClientCore::new(&ClientConfig::default());
        let core = Arc::new(core);

        let waiter = {
            let core = Arc::clone(&core);
            tokio::spawn(async move {
                core.wait_for_negotiation(Duration::from_secs(1))
                    .await
                    .expect("negotiation wait")
            })
        };

        tokio::time::sleep(Duration::from_millis(20)).await;
        core.update_parser(
            SerializationFormat::Json,
            CompressionAlgorithm::None,
            EncryptionAlgorithm::None,
        )
        .await;

        waiter.await.expect("wait task");
    }

    #[tokio::test]
    async fn start_heartbeat_before_negotiation_does_not_panic() {
        let core = ClientCore::new(&ClientConfig::default());
        // No connection attached; should no-op safely before negotiation.
        core.start_heartbeat(Arc::new(Mutex::new(
            Box::new(MockConnection) as Box<dyn Connection>
        )))
        .await;
        assert!(!core.is_negotiation_completed());
    }

    #[test]
    fn heartbeat_runtime_policy_is_shared_across_core_clones() {
        let core = ClientCore::new(&ClientConfig::default());
        let cloned = core.clone();

        assert_eq!(core.heartbeat_effective_interval(), Duration::from_secs(30));
        cloned.set_heartbeat_app_state(HeartbeatAppState::Background);
        assert_eq!(
            core.heartbeat_effective_interval(),
            Duration::from_secs(120)
        );

        core.set_heartbeat_nat_timeout(Some(Duration::from_secs(40)));
        assert_eq!(
            cloned.heartbeat_effective_interval(),
            Duration::from_secs(28)
        );
    }

    /// 协商完成后收到计划性 CLOSE：随后的断开以类型化的关闭上报，窗口与优先接入点都在；
    /// 新连接建立后清除。没收到 CLOSE、只从传输层看到停机原因时也认得出是停机（没有窗口）。
    #[tokio::test]
    async fn a_planned_close_is_reported_with_its_window() {
        use crate::common::link::{CloseKind, ServerClose};
        let mut core = ClientCore::new(&ClientConfig::default());
        core.set_client_connection(Arc::new(Mutex::new(
            Box::new(MockConnection) as Box<dyn Connection>
        )));
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            SerializationFormat::Json,
            CompressionAlgorithm::None,
            EncryptionAlgorithm::None,
        )
        .await;
        core.stop_heartbeat();

        let drain = ServerClose::new(CloseKind::Drain)
            .with_retry_window(Duration::from_secs(2), Duration::from_secs(9))
            .with_prefer_ap(["ap-2"]);
        let frame = crate::common::protocol::frame_with_system_command(
            drain.to_command(),
            crate::common::protocol::Reliability::BestEffort,
        );
        let Some(Type::System(command)) = frame.command.as_ref().and_then(|c| c.r#type.as_ref())
        else {
            panic!("system command");
        };
        core.handle_close_command(&frame, command, true).await;
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "DRAIN: server closed the connection".to_string(),
        ));
        assert_eq!(
            core.last_disconnect_reason(),
            Some(DisconnectReason::ServerClose(drain))
        );

        core.handle_connection_event(&ConnectionEvent::Connected);
        assert_eq!(core.last_disconnect_reason(), None);
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "SERVER_SHUTDOWN".to_string(),
        ));
        assert_eq!(
            core.last_disconnect_reason(),
            Some(DisconnectReason::ServerClose(ServerClose::new(
                CloseKind::ServerShutdown
            )))
        );
    }

    /// 协商完成后的终止类 ERROR（升级门槛）以类型化拒绝上报；非终止类 ERROR 不改变断开原因。
    #[tokio::test]
    async fn a_terminal_error_after_negotiation_is_reported_as_a_rejection() {
        use crate::common::error::ErrorCode;
        use crate::common::link::upgrade_required_command;
        let mut core = ClientCore::new(&ClientConfig::default());
        core.set_client_connection(Arc::new(Mutex::new(
            Box::new(MockConnection) as Box<dyn Connection>
        )));
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            SerializationFormat::Json,
            CompressionAlgorithm::None,
            EncryptionAlgorithm::None,
        )
        .await;
        core.stop_heartbeat();

        let command = upgrade_required_command(30_201, Some("https://example.com/download"));
        core.handle_session_error_command(&command).await;
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "UPGRADE_REQUIRED: server rejected the session".to_string(),
        ));
        let Some(DisconnectReason::Rejected(rejection)) = core.last_disconnect_reason() else {
            panic!("expected a rejection");
        };
        assert_eq!(rejection.code, ErrorCode::UpgradeRequired);
        assert_eq!(rejection.min_build, Some(30_201));
        assert!(rejection.is_terminal());

        // 新连接清除；非终止类 ERROR（过载）不登记，断开仍按文本归类。
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.handle_session_error_command(&crate::common::link::overloaded_command(
            Duration::from_millis(500),
        ))
        .await;
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".to_string(),
        ));
        assert!(matches!(
            core.last_disconnect_reason(),
            Some(DisconnectReason::PeerClosed { .. })
        ));
    }

    /// 心跳判死、对端关闭、读写出错、主动断开各自归类。
    #[test]
    fn disconnects_are_typed() {
        let core = ClientCore::new(&ClientConfig::default());
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            crate::client::heartbeat::manager::HEARTBEAT_TIMEOUT_REASON.to_string(),
        ));
        assert_eq!(
            core.last_disconnect_reason(),
            Some(DisconnectReason::HeartbeatTimeout)
        );

        core.handle_connection_event(&ConnectionEvent::Connected);
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".to_string(),
        ));
        assert_eq!(
            core.last_disconnect_reason(),
            Some(DisconnectReason::PeerClosed {
                detail: "Stream closed by peer".to_string()
            })
        );

        core.handle_connection_event(&ConnectionEvent::Connected);
        core.handle_connection_event(&ConnectionEvent::Error(FlareError::connection_failed(
            "read failed",
        )));
        assert!(matches!(
            core.last_disconnect_reason(),
            Some(DisconnectReason::TransportError { .. })
        ));

        core.handle_connection_event(&ConnectionEvent::Connected);
        core.set_disconnect_requested(true);
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            crate::client::link::CLIENT_DISCONNECTED_REASON.to_string(),
        ));
        assert_eq!(
            core.last_disconnect_reason(),
            Some(DisconnectReason::ClientRequested)
        );
    }

    /// CONNECT_ACK 的接入信息记在连接上；等着的上层绑定在协商完成前挂上，挂上的观察者收到
    /// `Connected` 与这帧 CONNECT_ACK，读得到断开原因的槽。收回发生在挂上之后时报告「已挂上」。
    #[tokio::test]
    async fn the_connect_ack_adopts_the_pending_binding() {
        use crate::common::link::ConnectAckInfo;
        #[derive(Default)]
        struct Seen(std::sync::Mutex<Vec<&'static str>>);
        impl crate::transport::events::ConnectionObserver for Seen {
            fn on_event(&self, event: &ConnectionEvent) {
                let label = match event {
                    ConnectionEvent::Connected => "connected",
                    ConnectionEvent::Message(_) => "message",
                    ConnectionEvent::Disconnected(_) => "disconnected",
                    ConnectionEvent::Error(_) => "error",
                };
                self.0.lock().unwrap().push(label);
            }
        }

        let core = ClientCore::new(&ClientConfig::default());
        core.handle_connection_event(&ConnectionEvent::Connected);
        let seen = Arc::new(Seen::default());
        let slot = Arc::new(std::sync::OnceLock::new());
        core.set_link_binding(LinkBinding {
            observers: vec![seen.clone()],
            reason_slot: Some(Arc::clone(&slot)),
        });

        let info = ConnectAckInfo {
            server_time_ms: Some(1_762_000_000_000),
            access_serial: Some(0),
            heartbeat_interval_hint_sec: Some(30),
            gateway_id: Some("gw-1".to_string()),
            ap_id: None,
        };
        let mut metadata = HashMap::new();
        info.write_metadata(&mut metadata);
        let ack = crate::common::protocol::frame_with_system_command(
            crate::common::protocol::builder::connect_ack(
                SerializationFormat::Json,
                None,
                None,
                metadata,
            ),
            crate::common::protocol::Reliability::AtLeastOnce,
        );
        let data = crate::common::message::parser::PRE_NEGOTIATION_PARSER
            .serialize(&ack)
            .expect("serialize");
        core.handle_received_message(data, false).await;
        core.stop_heartbeat();

        assert!(core.is_negotiation_completed());
        assert_eq!(core.connect_ack_info(), Some(info));
        assert_eq!(*seen.0.lock().unwrap(), vec!["connected", "message"]);
        assert_eq!(core.revoke_link_binding(), BindingRevoke::AlreadyAdopted);

        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".to_string(),
        ));
        let cell = slot.get().expect("the slot is bound on adoption");
        assert!(matches!(
            cell.lock().unwrap().clone(),
            Some(DisconnectReason::PeerClosed { .. })
        ));

        let other = ClientCore::new(&ClientConfig::default());
        other.set_link_binding(LinkBinding::default());
        assert_eq!(other.revoke_link_binding(), BindingRevoke::Revoked);
        assert_eq!(other.revoke_link_binding(), BindingRevoke::Unbound);
    }

    /// CONNECT 被拒时，带字段的类型化拒绝记在连接上。
    #[test]
    fn a_rejection_keeps_its_typed_fields() {
        let core = ClientCore::new(&ClientConfig::default());
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.record_connect_rejection(&crate::common::link::overloaded_command(
            Duration::from_millis(1500),
        ));
        let rejection = core.server_rejection().expect("rejection");
        assert_eq!(rejection.code, crate::common::error::ErrorCode::Overloaded);
        assert_eq!(rejection.retry_after_ms, Some(1500));
    }

    #[test]
    fn client_connection_take_clears_shared_core_slot() {
        let mut core = ClientCore::new(&ClientConfig::default());
        let cloned = core.clone();
        core.set_client_connection(Arc::new(Mutex::new(
            Box::new(MockConnection) as Box<dyn Connection>
        )));

        assert!(cloned.take_client_connection().is_some());
        assert!(core.take_client_connection().is_none());
    }

    struct MockConnection;

    #[async_trait::async_trait]
    impl Connection for MockConnection {
        fn add_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        fn remove_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> crate::common::platform::MonotonicInstant {
            crate::common::platform::monotonic_now()
        }
        fn update_active_time(&mut self) {}
    }
}

/// 拿到心跳管理器并记一次 PONG，**不阻塞、不 panic**。
///
/// 这里曾经用 `run_client_async`（`block_in_place` + `block_on`）去取那把 async 锁，
/// 两个代价：
/// ① current-thread runtime 上 `block_in_place` 直接 panic，而 PONG 是每个心跳周期
///    都要走的路径 —— 任务被打死后心跳记账不再更新，连接会被自己判成超时；
/// ② 多线程 runtime 上不 panic，但每收一个 PONG 就要把本 worker 的任务队列交接给
///    另一个线程，只为了写一个时间戳。
///
/// 而 `HeartbeatManager::record_pong` 只要 `&self`，内部已经是 `std::sync::Mutex`
/// 自带互斥 —— 那把外层 async 锁只是取引用的手段。所以常态走 `try_lock` 内联完成，
/// 只有真撞上锁时才丢给后台任务（顺序无所谓：记的是"最近一次 PONG 的时刻"）。
fn record_pong_on(heartbeat: Arc<tokio::sync::Mutex<HeartbeatManager>>) {
    if let Ok(hb_guard) = heartbeat.try_lock() {
        hb_guard.record_pong();
        return;
    }
    crate::client::runtime::spawn_client_task(async move {
        let hb_guard = heartbeat.lock().await;
        hb_guard.record_pong();
    });
}

/// 停止心跳，同上：常态同步完成，撞锁才异步。
///
/// 停止只是把 stop_tx 发一次信号，重复/延后执行都是幂等的。
fn stop_heartbeat_on(heartbeat: Arc<tokio::sync::Mutex<HeartbeatManager>>) {
    if let Ok(mut hb_guard) = heartbeat.try_lock() {
        hb_guard.stop();
        return;
    }
    crate::client::runtime::spawn_client_task(async move {
        let mut hb_guard = heartbeat.lock().await;
        hb_guard.stop();
    });
}

#[cfg(test)]
mod heartbeat_bridge_tests {
    use super::*;
    use std::time::Duration;

    fn manager() -> Arc<tokio::sync::Mutex<HeartbeatManager>> {
        Arc::new(tokio::sync::Mutex::new(HeartbeatManager::new(
            Duration::from_secs(30),
            Duration::from_secs(90),
        )))
    }

    /// `#[tokio::test]` 默认就是 current-thread —— 正是原实现必 panic 的那个形态。
    /// PONG 每个心跳周期都要走，一旦打死任务，心跳记账不再更新，连接会被自己判成超时。
    #[tokio::test]
    async fn record_pong_works_on_current_thread_runtime() {
        let heartbeat = manager();
        {
            let guard = heartbeat.lock().await;
            guard.mark_ping_for_test();
            assert!(
                guard.is_timeout_after_for_test(Duration::from_secs(0)),
                "先制造一个未응答的 ping，否则断言不出 PONG 有没有记上"
            );
        }

        record_pong_on(Arc::clone(&heartbeat));

        let guard = heartbeat.lock().await;
        assert!(
            !guard.is_timeout_after_for_test(Duration::from_secs(0)),
            "PONG 必须被记上：单线程 runtime 下也不能丢"
        );
    }

    /// 撞锁时不能阻塞、更不能 panic —— 退化成后台补记即可。
    #[tokio::test]
    async fn record_pong_falls_back_to_background_when_lock_is_held() {
        let heartbeat = manager();
        {
            let guard = heartbeat.lock().await;
            guard.mark_ping_for_test();
        }
        let held = heartbeat.clone().lock_owned().await;

        record_pong_on(Arc::clone(&heartbeat));
        drop(held);

        for _ in 0..50 {
            tokio::task::yield_now().await;
            if !heartbeat
                .lock()
                .await
                .is_timeout_after_for_test(Duration::from_secs(0))
            {
                return;
            }
        }
        panic!("撞锁后的 PONG 必须由后台任务补记上");
    }

    #[tokio::test]
    async fn stop_heartbeat_works_on_current_thread_runtime() {
        let heartbeat = manager();
        stop_heartbeat_on(Arc::clone(&heartbeat));
        assert!(
            !heartbeat.lock().await.has_stop_channel_for_test(),
            "停止后 stop 通道必须已被取走"
        );
    }
}

#[cfg(test)]
mod negotiation_rejection_tests {
    use super::*;
    use crate::common::error::ErrorCode;
    use crate::common::protocol::{Reliability, frame_with_system_command};

    fn core() -> ClientCore {
        ClientCore::new(&ClientConfig::default())
    }

    fn rejection(message: &str, code: Option<&str>) -> crate::common::protocol::SystemCommand {
        let metadata =
            code.map(|code| HashMap::from([("code".to_string(), code.as_bytes().to_vec())]));
        crate::common::protocol::builder::error(message.to_string(), metadata)
    }

    /// 服务端在 CONNECT_ACK 之前关连接却没给原因：等协商的一方立刻拿到连接失败，
    /// 不再干等 10s，也不再被猜成令牌被拒（心跳失败、踢线后的断开都走这里）。
    #[test]
    fn close_before_connect_ack_without_a_reason_is_a_connection_failure() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        assert!(core.negotiation_failure_error().is_none());

        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "heartbeat send failed".into(),
        ));

        let err = core
            .negotiation_failure_error()
            .expect("协商阶段被断开必须立刻记为协商失败");
        assert_eq!(err.code(), Some(ErrorCode::ConnectionFailed), "{err}");
        assert!(err.to_string().contains("before CONNECT_ACK"), "{err}");
        assert!(!err.to_string().contains("TOKEN"), "{err}");
    }

    /// 读出错（如 QUIC 连接被关）同样立即结束协商等待。
    #[test]
    fn read_error_before_connect_ack_fails_negotiation_at_once() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.handle_connection_event(&ConnectionEvent::Error(FlareError::io("connection lost")));
        let err = core.negotiation_failure_error().expect("must fail at once");
        assert_eq!(err.code(), Some(ErrorCode::ConnectionFailed), "{err}");
    }

    /// 服务端先下发 ERROR 帧再关连接：关连接事件保留服务端给的类型化原因。
    #[test]
    fn typed_rejection_survives_the_close_that_follows_it() {
        for (message, expected) in [
            ("TENANT_UNAVAILABLE", ErrorCode::PermissionDenied),
            ("AUTHENTICATION_FAILED", ErrorCode::AuthenticationFailed),
        ] {
            let core = core();
            core.handle_connection_event(&ConnectionEvent::Connected);
            core.record_connect_rejection(&rejection(message, Some("AUTHENTICATION_FAILED")));
            core.handle_connection_event(&ConnectionEvent::Disconnected(
                "Stream closed by peer".into(),
            ));

            let err = core
                .negotiation_failure_error()
                .expect("rejection recorded");
            assert_eq!(err.code(), Some(expected), "{err}");
            assert!(err.to_string().contains(message), "{err}");
        }
    }

    #[test]
    fn connect_rejection_error_maps_typed_reasons() {
        let cases = [
            (
                "TENANT_UNAVAILABLE",
                Some("AUTHENTICATION_FAILED"),
                ErrorCode::PermissionDenied,
            ),
            (
                "AUTHENTICATION_FAILED",
                Some("AUTHENTICATION_FAILED"),
                ErrorCode::AuthenticationFailed,
            ),
            ("TOKEN_EXPIRED", None, ErrorCode::TokenExpired),
            // 暂时性拒绝：认证依赖读不到，客户端应退避重连
            (
                "SERVICE_UNAVAILABLE",
                Some("SERVICE_UNAVAILABLE"),
                ErrorCode::ServiceUnavailable,
            ),
            (
                "TENANT_RUNTIME_UNREADABLE",
                Some("SERVICE_UNAVAILABLE"),
                ErrorCode::ServiceUnavailable,
            ),
            (
                "SOME_NEW_REASON",
                Some("AUTHENTICATION_FAILED"),
                ErrorCode::AuthenticationFailed,
            ),
            ("", None, ErrorCode::ConnectionRefused),
        ];
        for (reason, code, expected) in cases {
            let err = connect_rejection_error(reason, code);
            assert_eq!(err.code(), Some(expected), "{reason}: {err}");
            if !reason.is_empty() {
                assert!(err.to_string().contains(reason), "{err}");
            }
        }
    }

    /// 等协商的一方被 ERROR 帧唤醒，拿到的是类型化错误而不是协议错误。
    #[tokio::test]
    async fn wait_for_negotiation_returns_the_typed_rejection() {
        let core = Arc::new(core());
        core.handle_connection_event(&ConnectionEvent::Connected);
        let waiter = {
            let core = Arc::clone(&core);
            tokio::spawn(async move {
                core.wait_for_negotiation(std::time::Duration::from_secs(5))
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        core.record_connect_rejection(&rejection(
            "TENANT_UNAVAILABLE",
            Some("AUTHENTICATION_FAILED"),
        ));

        let err = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("woken at once")
            .expect("join")
            .expect_err("rejected");
        assert_eq!(err.code(), Some(ErrorCode::PermissionDenied), "{err}");
    }

    /// 我方主动断开（切账号/登出）不是被拒，不能伪造协商失败。
    #[test]
    fn client_requested_disconnect_is_not_a_rejection() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.set_disconnect_requested(true);
        core.handle_connection_event(&ConnectionEvent::Disconnected("Closed by client".into()));
        assert!(core.negotiation_failure_error().is_none());
    }

    /// 协商已经完成后的断线是普通掉线，走重连，不是协商失败。
    #[test]
    fn disconnect_after_negotiation_completed_is_not_a_rejection() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.negotiation_completed.store(true, Ordering::SeqCst);
        core.handle_connection_event(&ConnectionEvent::Disconnected("Connection closed".into()));
        assert!(core.negotiation_failure_error().is_none());
    }

    /// 客户端发完 CONNECT 后补报的那次 Connected 不能复位协商：CONNECT_ACK 可能已经到了。
    #[tokio::test]
    async fn repeated_connected_keeps_a_completed_negotiation() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            crate::common::protocol::SerializationFormat::Protobuf,
            crate::common::compression::CompressionAlgorithm::Gzip,
            crate::common::encryption::EncryptionAlgorithm::None,
        )
        .await;
        core.handle_connection_event(&ConnectionEvent::Connected);
        assert!(core.is_negotiation_completed());

        // 新的一条连接（先断开再连上）才复位。
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".into(),
        ));
        core.handle_connection_event(&ConnectionEvent::Connected);
        assert!(!core.is_negotiation_completed());
    }

    /// 连接断开时停掉绑在它上面的心跳，下一条连接协商完成后才能起自己的心跳。
    #[tokio::test]
    async fn disconnect_stops_the_heartbeat_of_that_connection() {
        let mut core = core();
        core.set_client_connection(Arc::new(Mutex::new(
            Box::new(NoopConnection) as Box<dyn Connection>
        )));
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            crate::common::protocol::SerializationFormat::Json,
            crate::common::compression::CompressionAlgorithm::None,
            crate::common::encryption::EncryptionAlgorithm::None,
        )
        .await;
        assert!(
            core.heartbeat_manager.lock().unwrap().is_some(),
            "heartbeat started"
        );

        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".into(),
        ));
        assert!(
            core.heartbeat_manager.lock().unwrap().is_none(),
            "heartbeat stopped"
        );
    }

    /// 服务端先发 KICKED 再关流：关流事件先把协商标志复位，KICKED 的处理晚一步。
    /// 它仍要按读出时的协商结果解析并交给上层，而不是按协商前 JSON 解析失败后整帧丢掉。
    #[tokio::test]
    async fn kicked_frame_read_before_the_close_still_reaches_observers() {
        let core = core();
        core.handle_connection_event(&ConnectionEvent::Connected);
        core.update_parser(
            crate::common::protocol::SerializationFormat::Protobuf,
            crate::common::compression::CompressionAlgorithm::Gzip,
            crate::common::encryption::EncryptionAlgorithm::None,
        )
        .await;
        let seen = Arc::new(StdMutex::new(Vec::new()));
        core.add_observer(Arc::new(RecordingObserver(Arc::clone(&seen))));
        let kicked = frame_with_system_command(
            crate::common::protocol::builder::kicked("TENANT_UNAVAILABLE".to_string(), None),
            Reliability::AtLeastOnce,
        );
        let data = core
            .parser
            .lock()
            .await
            .serialize(&kicked)
            .expect("serialize");

        let negotiated_at_receipt = core.is_negotiation_completed();
        core.handle_connection_event(&ConnectionEvent::Disconnected(
            "Stream closed by peer".into(),
        ));
        core.handle_received_message(data, negotiated_at_receipt)
            .await;

        let messages = seen
            .lock()
            .unwrap()
            .iter()
            .filter(|event| matches!(event, ConnectionEvent::Message(_)))
            .count();
        assert_eq!(messages, 1, "KICKED frame must reach observers");
    }

    struct RecordingObserver(Arc<StdMutex<Vec<ConnectionEvent>>>);

    impl crate::transport::events::ConnectionObserver for RecordingObserver {
        fn on_event(&self, event: &ConnectionEvent) {
            self.0.lock().unwrap().push(event.clone());
        }
    }

    struct NoopConnection;

    #[async_trait::async_trait]
    impl Connection for NoopConnection {
        fn add_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        fn remove_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> crate::common::platform::MonotonicInstant {
            crate::common::platform::monotonic_now()
        }
        fn update_active_time(&mut self) {}
    }
}

/// 被踢：观察者恰好收到一次带类型化原因的断开，结束会话的原因让客户端进入终态（W8-S1 F1）。
#[cfg(all(test, not(target_arch = "wasm32")))]
mod kick_tests {
    use super::*;
    use crate::common::device::DevicePlatform;
    use crate::common::kick::{DEVICE_CONFLICT_REASON, device_conflict_kicked};
    use crate::common::protocol::{Reliability, frame_with_system_command};

    struct RecordingObserver(Arc<StdMutex<Vec<ConnectionEvent>>>);

    impl crate::transport::events::ConnectionObserver for RecordingObserver {
        fn on_event(&self, event: &ConnectionEvent) {
            self.0.lock().unwrap().push(event.clone());
        }
    }

    struct PanickingObserver;

    impl crate::transport::events::ConnectionObserver for PanickingObserver {
        fn on_event(&self, _: &ConnectionEvent) {
            panic!("observer bug");
        }
    }

    /// 像真实传输一样：本端关闭时以给定原因（`close()` 为 `Closed by client`）同步上报一次断开，
    /// 已经上报过（对端先关）就不再上报。
    struct LoopbackConnection {
        core: ClientCore,
        closed: Arc<AtomicBool>,
        close_reasons: Arc<StdMutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl Connection for LoopbackConnection {
        fn add_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        fn remove_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
        async fn send(&mut self, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            self.close_with_reason("Closed by client").await
        }
        async fn close_with_reason(&mut self, reason: &str) -> Result<()> {
            self.close_reasons.lock().unwrap().push(reason.to_string());
            if !self.closed.swap(true, Ordering::SeqCst) {
                self.core
                    .handle_connection_event(&ConnectionEvent::Disconnected(reason.to_string()));
            }
            Ok(())
        }
        fn last_active_time(&self) -> crate::common::platform::MonotonicInstant {
            crate::common::platform::monotonic_now()
        }
        fn update_active_time(&mut self) {}
    }

    struct Harness {
        core: ClientCore,
        seen: Arc<StdMutex<Vec<ConnectionEvent>>>,
        transport_closed: Arc<AtomicBool>,
        close_reasons: Arc<StdMutex<Vec<String>>>,
    }

    impl Harness {
        /// 一条协商完成（Protobuf + Gzip）的连接。
        async fn negotiated() -> Self {
            let mut core = ClientCore::new(&ClientConfig::default());
            let transport_closed = Arc::new(AtomicBool::new(false));
            let close_reasons = Arc::new(StdMutex::new(Vec::new()));
            core.set_client_connection(Arc::new(Mutex::new(Box::new(LoopbackConnection {
                core: core.clone(),
                closed: Arc::clone(&transport_closed),
                close_reasons: Arc::clone(&close_reasons),
            }) as Box<dyn Connection>)));
            core.handle_connection_event(&ConnectionEvent::Connected);
            core.update_parser(
                crate::common::protocol::SerializationFormat::Protobuf,
                crate::common::compression::CompressionAlgorithm::Gzip,
                crate::common::encryption::EncryptionAlgorithm::None,
            )
            .await;
            core.stop_heartbeat();
            let seen = Arc::new(StdMutex::new(Vec::new()));
            core.add_observer(Arc::new(RecordingObserver(Arc::clone(&seen))));
            Self {
                core,
                seen,
                transport_closed,
                close_reasons,
            }
        }

        async fn encode(&self, command: crate::common::protocol::SystemCommand) -> Vec<u8> {
            let frame = frame_with_system_command(command, Reliability::AtLeastOnce);
            self.core
                .parser
                .lock()
                .await
                .serialize(&frame)
                .expect("serialize")
        }

        /// 对端关了连接：传输上报一次断开（已经关过的不再上报）。
        fn peer_closes(&self) {
            if !self.transport_closed.swap(true, Ordering::SeqCst) {
                self.core
                    .handle_connection_event(&ConnectionEvent::Disconnected(
                        "Stream closed by peer".into(),
                    ));
            }
        }

        fn disconnects(&self) -> Vec<String> {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .filter_map(|event| match event {
                    ConnectionEvent::Disconnected(reason) => Some(reason.clone()),
                    _ => None,
                })
                .collect()
        }
    }

    /// KICKED 先被处理：以被踢原因关闭连接，观察者收到的唯一一次断开就是 DEVICE_CONFLICT，
    /// 而不是 `Closed by client`；之后对端关闭不再上报。客户端进入终态。
    #[tokio::test]
    async fn kick_handled_before_the_close_is_reported_once_with_its_reason() {
        let harness = Harness::negotiated().await;
        let data = harness
            .encode(device_conflict_kicked(&DevicePlatform::PC, "new-device"))
            .await;

        harness.core.handle_received_message(data, true).await;
        harness.peer_closes();
        harness
            .core
            .handle_connection_event(&ConnectionEvent::Error(FlareError::io("connection lost")));

        assert_eq!(harness.disconnects(), vec![DEVICE_CONFLICT_REASON]);
        assert!(
            !harness
                .seen
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, ConnectionEvent::Error(_))),
            "a read error after the kick is not reported again"
        );
        assert_eq!(
            *harness.close_reasons.lock().unwrap(),
            vec![DEVICE_CONFLICT_REASON]
        );

        // 应用之后主动断开，照常上报
        harness.core.set_disconnect_requested(true);
        harness
            .core
            .handle_connection_event(&ConnectionEvent::Disconnected("Client disconnected".into()));
        assert_eq!(
            harness.disconnects(),
            vec![DEVICE_CONFLICT_REASON, "Client disconnected"]
        );
        assert_eq!(
            harness.core.session_ended_by_server().as_deref(),
            Some(DEVICE_CONFLICT_REASON)
        );
        assert!(!harness.core.can_send());
    }

    /// 排在前面的观察者 panic 过，被踢的原因照样送到其余观察者，之后加入的观察者也照常收到通知。
    /// 原先持着列表锁回调，panic 毒化锁后 `if let Ok` 把此后的通知全部静默丢掉：宿主既收不到
    /// 被踢，也收不到断开。
    #[tokio::test]
    async fn a_panicking_observer_does_not_silence_the_kick_reason() {
        let harness = Harness::negotiated().await;
        harness
            .core
            .observers
            .lock()
            .unwrap()
            .insert(0, Arc::new(PanickingObserver));

        // 修复前 panic 会穿出通知；在这里接住，只看之后的通知是否还在。
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            harness.core.notify_observers(&ConnectionEvent::Connected)
        }));
        assert!(
            matches!(
                harness.seen.lock().unwrap().last(),
                Some(ConnectionEvent::Connected)
            ),
            "the observer behind the panicking one still gets the same event"
        );

        let data = harness
            .encode(device_conflict_kicked(&DevicePlatform::PC, "new-device"))
            .await;
        harness.core.handle_received_message(data, true).await;
        harness.peer_closes();
        assert_eq!(harness.disconnects(), vec![DEVICE_CONFLICT_REASON]);

        let late = Arc::new(StdMutex::new(Vec::new()));
        harness
            .core
            .add_observer(Arc::new(RecordingObserver(Arc::clone(&late))));
        harness.core.set_disconnect_requested(true);
        harness
            .core
            .handle_connection_event(&ConnectionEvent::Disconnected("Client disconnected".into()));
        assert_eq!(
            late.lock().unwrap().len(),
            1,
            "observer added after the panic"
        );
    }

    /// 读循环里的对端关闭恰好落在被踢处理的中间：被踢处理已经把状态置为断开（对端关闭因此不等
    /// 入站帧处理完），关连接时传输已被对端关掉，不再带出事件。观察者仍只收到一次 DEVICE_CONFLICT，
    /// 没有前面那次对端关闭（QUIC 回环上约 1/40 的概率出现过 `["Stream closed by peer",
    /// "DEVICE_CONFLICT"]`）；之后的读出错也不再上报。
    #[tokio::test]
    async fn a_peer_close_racing_the_kick_is_reported_as_the_kick() {
        /// 关连接时对端已经先关：读循环的对端关闭先到，本端关闭不再带出事件。
        struct PeerClosesFirst {
            core: ClientCore,
            closed: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl Connection for PeerClosesFirst {
            fn add_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
            fn remove_observer(&mut self, _observer: crate::transport::events::ArcObserver) {}
            async fn send(&mut self, _data: &[u8]) -> Result<()> {
                Ok(())
            }
            async fn close(&mut self) -> Result<()> {
                self.close_with_reason("Closed by client").await
            }
            async fn close_with_reason(&mut self, _reason: &str) -> Result<()> {
                if !self.closed.swap(true, Ordering::SeqCst) {
                    self.core
                        .handle_connection_event(&ConnectionEvent::Disconnected(
                            "Stream closed by peer".into(),
                        ));
                }
                Ok(())
            }
            fn last_active_time(&self) -> crate::common::platform::MonotonicInstant {
                crate::common::platform::monotonic_now()
            }
            fn update_active_time(&mut self) {}
        }

        let mut harness = Harness::negotiated().await;
        let core = harness.core.clone();
        harness
            .core
            .set_client_connection(Arc::new(Mutex::new(Box::new(PeerClosesFirst {
                core,
                closed: Arc::clone(&harness.transport_closed),
            }) as Box<dyn Connection>)));
        let data = harness
            .encode(device_conflict_kicked(&DevicePlatform::PC, "new-device"))
            .await;

        harness.core.handle_received_message(data, true).await;
        harness
            .core
            .handle_connection_event(&ConnectionEvent::Error(FlareError::io("connection lost")));

        assert_eq!(harness.disconnects(), vec![DEVICE_CONFLICT_REASON]);
        assert!(
            !harness
                .seen
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, ConnectionEvent::Error(_))),
            "a read error after the kick is not reported again"
        );
        assert_eq!(
            harness.core.session_ended_by_server().as_deref(),
            Some(DEVICE_CONFLICT_REASON)
        );
    }

    /// 被踢处理关连接带出的断开与读循环的对端关闭在两个线程上同时到达：不论先后，观察者只收到
    /// 一次 DEVICE_CONFLICT。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_closes_after_a_kick_are_reported_once() {
        for round in 0..200 {
            let harness = Harness::negotiated().await;
            // 被踢处理关连接之前的状态：登记了被踢原因，状态已置为断开
            harness.core.set_pending_kick(DEVICE_CONFLICT_REASON);
            harness.core.state_manager.set_disconnected();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let ours = {
                let core = harness.core.clone();
                let barrier = Arc::clone(&barrier);
                tokio::task::spawn_blocking(move || {
                    barrier.wait();
                    core.handle_connection_event(&ConnectionEvent::Disconnected(
                        DEVICE_CONFLICT_REASON.into(),
                    ));
                })
            };
            let peer = {
                let core = harness.core.clone();
                let barrier = Arc::clone(&barrier);
                tokio::task::spawn_blocking(move || {
                    barrier.wait();
                    core.handle_connection_event(&ConnectionEvent::Disconnected(
                        "Stream closed by peer".into(),
                    ));
                })
            };
            ours.await.unwrap();
            peer.await.unwrap();
            // 被踢处理随后的补报：已经报过，不再报
            if let Some(reason) = harness.core.take_unreported_kick() {
                harness
                    .core
                    .notify_observers(&ConnectionEvent::Disconnected(reason));
            }

            assert_eq!(
                harness.disconnects(),
                vec![DEVICE_CONFLICT_REASON],
                "round {round}"
            );
        }
    }

    /// 关连接先被处理（服务端发完 KICKED 紧接着关连接，读循环同步处理关闭）：协商标志已被复位，
    /// 被踢仍按读出时的协商状态补报出去，客户端进入终态。
    #[tokio::test]
    async fn kick_handled_after_the_peer_closed_is_still_reported() {
        let harness = Harness::negotiated().await;
        let data = harness
            .encode(device_conflict_kicked(&DevicePlatform::PC, "new-device"))
            .await;

        harness.peer_closes();
        harness.core.handle_received_message(data, true).await;

        assert_eq!(
            harness.disconnects(),
            vec!["Stream closed by peer", DEVICE_CONFLICT_REASON]
        );
        assert_eq!(
            harness.core.session_ended_by_server().as_deref(),
            Some(DEVICE_CONFLICT_REASON)
        );
    }

    /// 旧服务端（中文说明 + metadata reason=device_conflict）的踢线同样按 DEVICE_CONFLICT 上报。
    #[tokio::test]
    async fn legacy_device_conflict_kicks_report_the_typed_reason() {
        let harness = Harness::negotiated().await;
        let legacy = crate::common::protocol::builder::kicked(
            "设备冲突：同一用户 (u1) 的同一平台 (PC) 已有其他设备在线，当前设备将被踢下线",
            Some(HashMap::from([(
                "reason".to_string(),
                b"device_conflict".to_vec(),
            )])),
        );
        let data = harness.encode(legacy).await;

        harness.core.handle_received_message(data, true).await;

        assert_eq!(harness.disconnects(), vec![DEVICE_CONFLICT_REASON]);
        assert!(harness.core.session_ended_by_server().is_some());
    }

    /// 只有结束会话的原因（设备冲突、租户不可用）进入终态；运维踢线照常上报原因，不进终态。
    #[tokio::test]
    async fn only_session_ending_kicks_end_the_session() {
        for (reason, ends) in [
            ("TENANT_UNAVAILABLE", true),
            ("DEVICE_KICKED", false),
            ("SESSION_REPLACED", false),
        ] {
            let harness = Harness::negotiated().await;
            let data = harness
                .encode(crate::common::protocol::builder::kicked(reason, None))
                .await;
            harness.core.handle_received_message(data, true).await;
            assert_eq!(harness.disconnects(), vec![reason], "{reason}");
            assert_eq!(
                harness.core.session_ended_by_server().is_some(),
                ends,
                "{reason}"
            );
        }
    }

    /// 协议竞速落选的连接（协商未完成时读出）与我方已请求断开时收到的 KICKED：不通知、不进终态。
    #[tokio::test]
    async fn kicks_on_unnegotiated_or_abandoned_connections_are_not_reported() {
        let harness = Harness::negotiated().await;
        let data = harness
            .encode(device_conflict_kicked(&DevicePlatform::PC, "new-device"))
            .await;
        harness.core.handle_received_message(data, false).await;
        assert!(
            harness
                .disconnects()
                .iter()
                .all(|reason| reason != DEVICE_CONFLICT_REASON),
            "{:?}",
            harness.disconnects()
        );
        assert!(harness.core.session_ended_by_server().is_none());

        let harness = Harness::negotiated().await;
        let data = harness
            .encode(device_conflict_kicked(&DevicePlatform::PC, "new-device"))
            .await;
        harness.core.set_disconnect_requested(true);
        harness.core.handle_received_message(data, true).await;
        assert_eq!(harness.disconnects(), vec!["Closed by client"]);
        assert!(harness.core.session_ended_by_server().is_none());
    }

    /// 终态只由应用显式建连解除：新连接建立时清除。
    #[tokio::test]
    async fn a_new_connection_clears_the_ended_session() {
        let harness = Harness::negotiated().await;
        let data = harness
            .encode(device_conflict_kicked(&DevicePlatform::PC, "new-device"))
            .await;
        harness.core.handle_received_message(data, true).await;
        assert!(harness.core.session_ended_by_server().is_some());

        harness
            .core
            .handle_connection_event(&ConnectionEvent::Connected);
        assert!(harness.core.session_ended_by_server().is_none());
    }
}
