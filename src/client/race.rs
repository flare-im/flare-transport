//! 交错竞速（Happy Eyeballs 风格）：按上层给的协议优先序依次发起，第一个完成 TLS + CONNECT_ACK 的胜出。
//!
//! 1. 先发起 `order[0]`；它在 `stagger` 内没完成握手（含 CONNECT_ACK），或者在此之前就失败了，
//!    再发起下一个协议；
//! 2. 每个协议的一次尝试分两段：建立传输（TCP/QUIC、TLS、WebSocket 升级），再发 CONNECT 等
//!    CONNECT_ACK。两段合计受 `connect_timeout` 约束，排队等 CONNECT 名额的时间不算；
//! 3. **同一时刻只有一条连接在发 CONNECT**：传输先建好的拿到名额，其余建好的排队，前一个失败才
//!    轮到下一个。同一设备的两条 CONNECT 会让服务端按「同设备替换」互踢——后到的 CONNECT 会把已经
//!    胜出的连接顶掉；
//! 4. 第一个拿到 CONNECT_ACK 的胜出，其余立即取消：还在建传输的直接丢弃，已经建好、在排队的正常
//!    关闭（它们没发过 CONNECT，服务端没有会话要清理）；
//! 5. 某个协议被服务端明确拒绝（过载、需要升级、令牌问题……）时立即结束：拒绝与传输无关，换协议
//!    只会被再拒一次，过载时还会把握手负载翻倍。
//!
//! 大多数重连只产生一次握手：服务端在重连风暴时的握手负载不会因为客户端有两个协议而翻倍。

use crate::client::link::{
    AttemptFailure, AttemptReport, CancelledAttempt, ConnectFailure, ConnectReport,
};
use crate::common::config_types::TransportProtocol;
use crate::common::link::ConnectAckInfo;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// 竞速的计时用的时钟：native 用 tokio 的（测试里能被暂停、快进），wasm 用浏览器单调时钟。
#[cfg(not(target_arch = "wasm32"))]
type Instant = tokio::time::Instant;
#[cfg(target_arch = "wasm32")]
type Instant = crate::common::platform::MonotonicInstant;

/// 默认的交错间隔：上一个协议这么久还没完成握手，就发起下一个。
pub const DEFAULT_RACE_STAGGER: Duration = Duration::from_millis(300);

/// 默认的单次握手时限：传输建立（含 TLS）加 CONNECT → CONNECT_ACK。
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(8);

/// 竞速参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaceParams {
    /// 协议优先序（上层每次传入，例如上次的胜者在前）。重复的协议只取第一次出现。
    pub order: Vec<TransportProtocol>,
    /// 交错间隔
    pub stagger: Duration,
    /// 单次握手时限（传输建立 + CONNECT_ACK）
    pub connect_timeout: Duration,
}

/// 竞速里的一次协议尝试。
///
/// 与本 crate 其余异步 trait 一样两端都要求 `Send`（wasm 上的浏览器句柄由传输层自行包装），
/// 竞速的 future 在 native 上因此能交给 tokio 任务。
#[async_trait::async_trait]
pub(crate) trait RaceAttempt: Send + 'static {
    /// 建立传输（TCP/QUIC、TLS、WebSocket 升级），不发 CONNECT。
    async fn establish(&mut self) -> Result<(), AttemptFailure>;

    /// 在已建立的传输上发 CONNECT，等 CONNECT_ACK，最多等 `budget`。
    async fn handshake(&mut self, budget: Duration) -> Result<ConnectAckInfo, AttemptFailure>;

    /// 放弃这次尝试：已建立的传输正常关闭。
    async fn abandon(&mut self);
}

type StepFuture<A> = Pin<Box<dyn Future<Output = Step<A>> + Send>>;

/// 一次尝试走完一段之后的结果。
enum Step<A> {
    Established {
        index: usize,
        attempt: A,
        took: Duration,
    },
    EstablishFailed {
        index: usize,
        failure: AttemptFailure,
        took: Duration,
    },
    Handshaken {
        index: usize,
        attempt: A,
        ack: ConnectAckInfo,
        took: Duration,
    },
    HandshakeFailed {
        index: usize,
        attempt: A,
        failure: AttemptFailure,
        took: Duration,
    },
}

/// 一次尝试的建立阶段：受 `timeout` 约束。
fn establish_step<A: RaceAttempt>(index: usize, mut attempt: A, limit: Duration) -> StepFuture<A> {
    Box::pin(async move {
        let started = Instant::now();
        let result = crate::common::platform::timeout(limit, attempt.establish()).await;
        let took = started.elapsed();
        match result {
            Ok(Ok(())) => Step::Established {
                index,
                attempt,
                took,
            },
            Ok(Err(failure)) => Step::EstablishFailed {
                index,
                failure,
                took,
            },
            Err(_) => Step::EstablishFailed {
                index,
                failure: AttemptFailure::Timeout,
                took,
            },
        }
    })
}

/// 一次尝试的 CONNECT 阶段：受这次尝试剩下的时限约束。
fn handshake_step<A: RaceAttempt>(index: usize, mut attempt: A, budget: Duration) -> StepFuture<A> {
    Box::pin(async move {
        let started = Instant::now();
        // 尝试自己按 budget 等 CONNECT_ACK；外面这层兜住 CONNECT 写不出去之类的卡死，多给一点余量，
        // 让尝试自己先报出更准确的原因。
        let result = crate::common::platform::timeout(
            budget + HANDSHAKE_GUARD_SLACK,
            attempt.handshake(budget),
        )
        .await;
        let took = started.elapsed();
        match result {
            Ok(Ok(ack)) => Step::Handshaken {
                index,
                attempt,
                ack,
                took,
            },
            Ok(Err(failure)) => Step::HandshakeFailed {
                index,
                attempt,
                failure,
                took,
            },
            Err(_) => Step::HandshakeFailed {
                index,
                attempt,
                failure: AttemptFailure::ConnectAckTimeout,
                took,
            },
        }
    })
}

/// 外层兜底超时比尝试自己的时限多出的余量。
const HANDSHAKE_GUARD_SLACK: Duration = Duration::from_millis(250);

/// 跑一次交错竞速。`make` 为协议创建一次尝试（创建失败直接记为这个协议的失败）。
///
/// 返回胜出的尝试与 [`ConnectReport`]；全部失败（或被服务端明确拒绝）时返回 [`ConnectFailure`]。
/// 整个 future 被丢弃时，所有尝试随之丢弃（还在握手的连接一并释放）。
pub(crate) async fn staggered_race<A, F>(
    params: &RaceParams,
    mut make: F,
) -> Result<(A, ConnectReport), ConnectFailure>
where
    A: RaceAttempt,
    F: FnMut(TransportProtocol) -> Result<A, AttemptFailure>,
{
    let race_started = Instant::now();
    let mut order: Vec<TransportProtocol> = Vec::with_capacity(params.order.len());
    for protocol in &params.order {
        if !order.contains(protocol) {
            order.push(*protocol);
        }
    }
    let connect_timeout = params.connect_timeout.max(Duration::from_millis(1));

    let mut running: FuturesUnordered<StepFuture<A>> = FuturesUnordered::new();
    // 传输已建好、排队等 CONNECT 名额的尝试：（编号，尝试，建立耗时）
    let mut waiting: VecDeque<(usize, A, Duration)> = VecDeque::new();
    let mut handshaking = false;
    let mut failed: Vec<AttemptReport> = Vec::new();
    let mut next = 0usize;
    let mut last_start = race_started;
    // 每个尝试建立传输用掉的时间，CONNECT 阶段只能用剩下的
    let mut establish_took: Vec<Duration> = vec![Duration::ZERO; order.len()];
    let mut finished: Vec<bool> = vec![false; order.len()];
    // 每个尝试发起的时刻（被取消时报告它跑了多久）
    let mut started_at: Vec<Option<Instant>> = vec![None; order.len()];

    // 发起下一个协议；创建尝试失败时记下并继续发起后面的，直到真的有一个在跑或没有可发起的了。
    let mut start_next = |next: &mut usize,
                          last_start: &mut Instant,
                          running: &mut FuturesUnordered<StepFuture<A>>,
                          failed: &mut Vec<AttemptReport>,
                          finished: &mut Vec<bool>,
                          started_at: &mut Vec<Option<Instant>>| {
        while *next < order.len() {
            let index = *next;
            *next += 1;
            *last_start = Instant::now();
            started_at[index] = Some(*last_start);
            let protocol = order[index];
            tracing::debug!(?protocol, index, "protocol race: starting attempt");
            match make(protocol) {
                Ok(attempt) => {
                    running.push(establish_step(index, attempt, connect_timeout));
                    return;
                }
                Err(failure) => {
                    tracing::debug!(?protocol, %failure, "protocol race: attempt could not start");
                    finished[index] = true;
                    failed.push(AttemptReport {
                        protocol,
                        elapsed: Duration::ZERO,
                        failure,
                    });
                }
            }
        }
    };

    start_next(
        &mut next,
        &mut last_start,
        &mut running,
        &mut failed,
        &mut finished,
        &mut started_at,
    );

    loop {
        if running.is_empty() && waiting.is_empty() {
            if next >= order.len() {
                tracing::debug!(
                    attempts = failed.len(),
                    "protocol race: every protocol failed"
                );
                return Err(ConnectFailure {
                    attempts: failed,
                    skipped: Vec::new(),
                    elapsed: race_started.elapsed(),
                });
            }
            // 跑着的都失败了：不等交错间隔，立即发起下一个
            start_next(
                &mut next,
                &mut last_start,
                &mut running,
                &mut failed,
                &mut finished,
                &mut started_at,
            );
            continue;
        }

        let stagger_left = (last_start + params.stagger).saturating_duration_since(Instant::now());
        let stagger = crate::common::platform::sleep(stagger_left);
        let can_start_more = next < order.len();

        let step = tokio::select! {
            biased;
            Some(step) = running.next(), if !running.is_empty() => step,
            () = stagger, if can_start_more => {
                start_next(&mut next, &mut last_start, &mut running, &mut failed, &mut finished, &mut started_at);
                continue;
            }
            else => {
                // 只剩排队的尝试却没有在握手的（不会发生：名额空出来时立即交给排队的）
                if let Some((index, attempt, took)) = waiting.pop_front() {
                    handshaking = true;
                    running.push(handshake_step(index, attempt, connect_timeout.saturating_sub(took)));
                }
                continue;
            }
        };

        match step {
            Step::Established {
                index,
                attempt,
                took,
            } => {
                establish_took[index] = took;
                if handshaking {
                    tracing::debug!(protocol = ?order[index], ?took, "protocol race: transport ready, waiting for the CONNECT slot");
                    waiting.push_back((index, attempt, took));
                } else {
                    tracing::debug!(protocol = ?order[index], ?took, "protocol race: transport ready, sending CONNECT");
                    handshaking = true;
                    running.push(handshake_step(
                        index,
                        attempt,
                        connect_timeout.saturating_sub(took),
                    ));
                }
            }
            Step::EstablishFailed {
                index,
                failure,
                took,
            } => {
                tracing::debug!(protocol = ?order[index], ?took, %failure, "protocol race: transport failed");
                finished[index] = true;
                failed.push(AttemptReport {
                    protocol: order[index],
                    elapsed: took,
                    failure,
                });
                if next < order.len() {
                    start_next(
                        &mut next,
                        &mut last_start,
                        &mut running,
                        &mut failed,
                        &mut finished,
                        &mut started_at,
                    );
                }
            }
            Step::Handshaken {
                index,
                attempt,
                ack,
                took,
            } => {
                finished[index] = true;
                let protocol = order[index];
                let handshake = establish_took[index] + took;
                let now = Instant::now();
                let ran_for = |index: usize| {
                    started_at[index]
                        .map(|started| now.saturating_duration_since(started))
                        .unwrap_or_default()
                };
                let mut cancelled: Vec<CancelledAttempt> = Vec::new();
                // 还在建传输的：丢弃 future 即中止，半建好的连接随之释放
                drop(running);
                for (index, done) in finished.iter().enumerate().take(next) {
                    if !done && !waiting.iter().any(|(queued, _, _)| *queued == index) {
                        cancelled.push(CancelledAttempt {
                            protocol: order[index],
                            ran_for: ran_for(index),
                            transport_ready: false,
                        });
                    }
                }
                // 已建好、排队的：没发过 CONNECT，在后台正常关闭，不占胜出的时间
                let losers: Vec<A> = waiting
                    .into_iter()
                    .map(|(index, attempt, _)| {
                        cancelled.push(CancelledAttempt {
                            protocol: order[index],
                            ran_for: ran_for(index),
                            transport_ready: true,
                        });
                        attempt
                    })
                    .collect();
                abandon_in_background(losers);
                tracing::info!(
                    ?protocol,
                    ?handshake,
                    elapsed = ?race_started.elapsed(),
                    failed = failed.len(),
                    cancelled = ?cancelled,
                    "protocol race: connected"
                );
                return Ok((
                    attempt,
                    ConnectReport {
                        protocol,
                        handshake,
                        elapsed: race_started.elapsed(),
                        ack,
                        failed,
                        cancelled,
                    },
                ));
            }
            Step::HandshakeFailed {
                index,
                attempt,
                failure,
                took,
            } => {
                finished[index] = true;
                handshaking = false;
                let protocol = order[index];
                tracing::debug!(?protocol, ?took, %failure, "protocol race: CONNECT failed");
                // 失败的连接在后台关：关闭握手可能要等对端，不能拖住排在后面的协议
                abandon_in_background(vec![attempt]);
                let rejected = failure.rejection().is_some();
                failed.push(AttemptReport {
                    protocol,
                    elapsed: establish_took[index] + took,
                    failure,
                });
                if rejected {
                    // 服务端明确拒绝：与传输无关，不再试别的协议
                    drop(running);
                    let mut skipped: Vec<TransportProtocol> = Vec::new();
                    for (index, done) in finished.iter().enumerate() {
                        if !done && !waiting.iter().any(|(queued, _, _)| *queued == index) {
                            skipped.push(order[index]);
                        }
                    }
                    let losers: Vec<A> = waiting
                        .into_iter()
                        .map(|(index, attempt, _)| {
                            skipped.push(order[index]);
                            attempt
                        })
                        .collect();
                    abandon_in_background(losers);
                    return Err(ConnectFailure {
                        attempts: failed,
                        skipped,
                        elapsed: race_started.elapsed(),
                    });
                }
                if let Some((index, attempt, took)) = waiting.pop_front() {
                    handshaking = true;
                    running.push(handshake_step(
                        index,
                        attempt,
                        connect_timeout.saturating_sub(took),
                    ));
                } else if next < order.len() {
                    start_next(
                        &mut next,
                        &mut last_start,
                        &mut running,
                        &mut failed,
                        &mut finished,
                        &mut started_at,
                    );
                }
            }
        }
    }
}

/// 在后台关闭落败、已建好传输的尝试。
fn abandon_in_background<A: RaceAttempt>(losers: Vec<A>) {
    if losers.is_empty() {
        return;
    }
    crate::client::runtime::spawn_client_task(async move {
        for mut loser in losers {
            if crate::common::platform::timeout(ABANDON_TIMEOUT, loser.abandon())
                .await
                .is_err()
            {
                tracing::debug!("protocol race: closing a losing transport timed out");
            }
        }
    });
}

/// 关一条落败连接最多等这么久：对端不配合关闭握手也不拖住后台任务。
const ABANDON_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::common::error::ErrorCode;
    use crate::common::link::ServerRejection;
    use std::sync::{Arc, Mutex};

    /// 测试用的一次尝试：建立耗时与结果、握手耗时与结果都事先给定，经过的每一步记进共享日志。
    #[derive(Clone)]
    struct Script {
        establish_after: Duration,
        establish: Result<(), AttemptFailure>,
        handshake_after: Duration,
        handshake: Result<ConnectAckInfo, AttemptFailure>,
    }

    impl Script {
        fn ok(establish_ms: u64, handshake_ms: u64) -> Self {
            Self {
                establish_after: Duration::from_millis(establish_ms),
                establish: Ok(()),
                handshake_after: Duration::from_millis(handshake_ms),
                handshake: Ok(ConnectAckInfo {
                    gateway_id: Some("gw-1".to_string()),
                    ..ConnectAckInfo::default()
                }),
            }
        }

        fn establish_fails(after_ms: u64, failure: AttemptFailure) -> Self {
            Self {
                establish_after: Duration::from_millis(after_ms),
                establish: Err(failure),
                handshake_after: Duration::ZERO,
                handshake: Err(AttemptFailure::ConnectAckTimeout),
            }
        }

        fn handshake_fails(establish_ms: u64, after_ms: u64, failure: AttemptFailure) -> Self {
            Self {
                establish_after: Duration::from_millis(establish_ms),
                establish: Ok(()),
                handshake_after: Duration::from_millis(after_ms),
                handshake: Err(failure),
            }
        }

        fn never_establishes() -> Self {
            Self::establish_fails(3_600_000, AttemptFailure::Timeout)
        }
    }

    type Log = Arc<Mutex<Vec<(u64, TransportProtocol, &'static str)>>>;

    struct Scripted {
        protocol: TransportProtocol,
        script: Script,
        log: Log,
        origin: Instant,
    }

    impl Scripted {
        fn record(&self, what: &'static str) {
            let at = self.origin.elapsed().as_millis() as u64;
            self.log.lock().unwrap().push((at, self.protocol, what));
        }
    }

    #[async_trait::async_trait]
    impl RaceAttempt for Scripted {
        async fn establish(&mut self) -> Result<(), AttemptFailure> {
            self.record("establish");
            tokio::time::sleep(self.script.establish_after).await;
            self.script.establish.clone()
        }

        async fn handshake(&mut self, budget: Duration) -> Result<ConnectAckInfo, AttemptFailure> {
            self.record("connect");
            if self.script.handshake_after > budget {
                tokio::time::sleep(budget).await;
                return Err(AttemptFailure::ConnectAckTimeout);
            }
            tokio::time::sleep(self.script.handshake_after).await;
            self.script.handshake.clone()
        }

        async fn abandon(&mut self) {
            self.record("abandon");
        }
    }

    fn params(order: &[TransportProtocol]) -> RaceParams {
        RaceParams {
            order: order.to_vec(),
            stagger: DEFAULT_RACE_STAGGER,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        }
    }

    async fn run(
        order: &[TransportProtocol],
        scripts: Vec<(TransportProtocol, Script)>,
    ) -> (
        Result<(Scripted, ConnectReport), ConnectFailure>,
        Vec<(u64, TransportProtocol, &'static str)>,
    ) {
        let log: Log = Arc::default();
        let origin = Instant::now();
        let result = staggered_race(&params(order), |protocol| {
            let script = scripts
                .iter()
                .find(|(p, _)| *p == protocol)
                .map(|(_, s)| s.clone())
                .ok_or_else(|| AttemptFailure::Unsupported {
                    detail: format!("{protocol:?} not scripted"),
                })?;
            Ok(Scripted {
                protocol,
                script,
                log: Arc::clone(&log),
                origin,
            })
        })
        .await;
        // 后台关闭落败者的任务跑完
        tokio::time::sleep(Duration::from_millis(10)).await;
        let entries = log.lock().unwrap().clone();
        (result, entries)
    }

    const QUIC: TransportProtocol = TransportProtocol::QUIC;
    const WS: TransportProtocol = TransportProtocol::WebSocket;

    /// 首选在交错间隔内就完成握手：只建一条连接，第二个协议根本不发起。
    #[tokio::test(start_paused = true)]
    async fn a_fast_first_choice_is_the_only_connection() {
        let (result, log) = run(
            &[QUIC, WS],
            vec![(QUIC, Script::ok(40, 30)), (WS, Script::ok(10, 10))],
        )
        .await;
        let (winner, report) = result.expect("QUIC wins");
        assert_eq!(winner.protocol, QUIC);
        assert_eq!(report.protocol, QUIC);
        assert_eq!(report.handshake, Duration::from_millis(70));
        assert!(report.failed.is_empty());
        assert!(report.cancelled.is_empty());
        assert_eq!(report.ack.gateway_id.as_deref(), Some("gw-1"));
        assert!(
            log.iter().all(|(_, protocol, _)| *protocol == QUIC),
            "WebSocket must not be started: {log:?}"
        );
    }

    /// 首选不通（UDP 被静默丢弃，握手一直挂着）：交错间隔一到就发起第二个，第二个胜出，首选被取消。
    #[tokio::test(start_paused = true)]
    async fn a_silent_first_choice_starts_the_second_after_the_stagger() {
        let (result, log) = run(
            &[QUIC, WS],
            vec![
                (QUIC, Script::never_establishes()),
                (WS, Script::ok(20, 20)),
            ],
        )
        .await;
        let (winner, report) = result.expect("WebSocket wins");
        assert_eq!(winner.protocol, WS);
        assert_eq!(report.protocol, WS);
        assert_eq!(report.handshake, Duration::from_millis(40));
        assert_eq!(report.elapsed, Duration::from_millis(340));
        assert!(report.failed.is_empty());
        assert_eq!(
            report.cancelled,
            vec![CancelledAttempt {
                protocol: QUIC,
                ran_for: Duration::from_millis(340),
                transport_ready: false,
            }]
        );
        assert_eq!(
            log,
            vec![
                (0, QUIC, "establish"),
                (300, WS, "establish"),
                (320, WS, "connect")
            ]
        );
    }

    /// 首选在交错间隔之前就失败：立即发起第二个，不等间隔走完；失败原因随结果带回。
    #[tokio::test(start_paused = true)]
    async fn a_failing_first_choice_starts_the_second_at_once() {
        let refused = AttemptFailure::Unreachable {
            detail: "connection refused".to_string(),
        };
        let (result, log) = run(
            &[QUIC, WS],
            vec![
                (QUIC, Script::establish_fails(15, refused.clone())),
                (WS, Script::ok(5, 5)),
            ],
        )
        .await;
        let (_, report) = result.expect("WebSocket wins");
        assert_eq!(report.protocol, WS);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].protocol, QUIC);
        assert_eq!(report.failed[0].failure, refused);
        assert_eq!(report.failed[0].elapsed, Duration::from_millis(15));
        assert!(log.contains(&(15, WS, "establish")), "{log:?}");
    }

    /// 两个协议都失败：每个协议的失败类别都带回，按失败先后排列。
    #[tokio::test(start_paused = true)]
    async fn both_failing_report_every_classification() {
        let tls = AttemptFailure::Tls {
            detail: "invalid peer certificate".to_string(),
        };
        let (result, _) = run(
            &[QUIC, WS],
            vec![
                (QUIC, Script::never_establishes()),
                (WS, Script::establish_fails(50, tls.clone())),
            ],
        )
        .await;
        let failure = result.err().expect("both fail");
        assert_eq!(failure.attempts.len(), 2);
        assert_eq!(failure.attempts[0].protocol, WS);
        assert_eq!(failure.attempts[0].failure, tls);
        assert_eq!(failure.attempts[1].protocol, QUIC);
        assert_eq!(failure.attempts[1].failure, AttemptFailure::Timeout);
        assert_eq!(failure.attempts[1].elapsed, DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(failure.rejection(), None);
        assert_eq!(failure.to_error().code(), Some(ErrorCode::ConnectionFailed));
    }

    /// 传输建好、CONNECT_ACK 迟迟不到：记为 CONNECT_ACK 超时，时限扣掉建立传输用掉的时间。
    #[tokio::test(start_paused = true)]
    async fn a_missing_connect_ack_is_its_own_classification() {
        let (result, _) = run(
            &[WS],
            vec![(
                WS,
                Script::handshake_fails(1_000, 3_600_000, AttemptFailure::ConnectAckTimeout),
            )],
        )
        .await;
        let failure = result.err().expect("ACK never arrives");
        assert_eq!(failure.attempts.len(), 1);
        assert_eq!(
            failure.attempts[0].failure,
            AttemptFailure::ConnectAckTimeout
        );
        assert_eq!(failure.attempts[0].elapsed, DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(
            failure.to_error().code(),
            Some(ErrorCode::ConnectionTimeout)
        );
    }

    /// 第二个协议先建好传输时，首选还在等 CONNECT_ACK：第二个排队，不发 CONNECT；首选拿到
    /// CONNECT_ACK 后，排队的那个被关闭。全程只有一条 CONNECT。
    #[tokio::test(start_paused = true)]
    async fn only_one_connect_is_in_flight_and_queued_losers_are_closed() {
        let (result, log) = run(
            &[QUIC, WS],
            vec![(QUIC, Script::ok(50, 400)), (WS, Script::ok(10, 10))],
        )
        .await;
        let (winner, report) = result.expect("QUIC wins");
        assert_eq!(winner.protocol, QUIC);
        assert_eq!(
            report.cancelled,
            vec![CancelledAttempt {
                protocol: WS,
                ran_for: Duration::from_millis(150),
                transport_ready: true,
            }]
        );
        let connects: Vec<_> = log
            .iter()
            .filter(|(_, _, what)| *what == "connect")
            .collect();
        assert_eq!(connects, vec![&(50, QUIC, "connect")], "{log:?}");
        assert!(log.contains(&(300, WS, "establish")), "{log:?}");
        assert!(log.contains(&(450, WS, "abandon")), "{log:?}");
        assert!(
            log.iter()
                .any(|(_, p, what)| *p == WS && *what == "abandon"),
            "{log:?}"
        );
    }

    /// 排在前面的 CONNECT 失败（非拒绝）：排队的那个立即接着发 CONNECT 并胜出。
    #[tokio::test(start_paused = true)]
    async fn a_queued_transport_takes_over_when_the_connect_fails() {
        let (result, log) = run(
            &[QUIC, WS],
            vec![
                (
                    QUIC,
                    Script::handshake_fails(
                        50,
                        500,
                        AttemptFailure::Closed {
                            detail: "reset".to_string(),
                        },
                    ),
                ),
                (WS, Script::ok(10, 10)),
            ],
        )
        .await;
        let (_, report) = result.expect("WebSocket wins");
        assert_eq!(report.protocol, WS);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].protocol, QUIC);
        assert!(log.contains(&(550, WS, "connect")), "{log:?}");
        assert!(log.contains(&(550, QUIC, "abandon")), "{log:?}");
    }

    /// 服务端明确拒绝（过载）：立即结束，不再试别的协议；排队的被关闭并记为跳过。
    #[tokio::test(start_paused = true)]
    async fn a_server_rejection_ends_the_race() {
        let overloaded = ServerRejection::from_command(&crate::common::link::overloaded_command(
            Duration::from_millis(2_000),
        ));
        let (result, log) = run(
            &[QUIC, WS],
            vec![
                (
                    QUIC,
                    Script::handshake_fails(20, 400, AttemptFailure::Rejected(overloaded.clone())),
                ),
                (WS, Script::ok(10, 10)),
            ],
        )
        .await;
        let failure = result.err().expect("rejected");
        assert_eq!(failure.rejection(), Some(&overloaded));
        assert_eq!(failure.attempts.len(), 1);
        assert_eq!(failure.skipped, vec![WS]);
        assert_eq!(failure.to_error().code(), Some(ErrorCode::Overloaded));
        assert!(
            !log.iter()
                .any(|(_, p, what)| *p == WS && *what == "connect"),
            "{log:?}"
        );
    }

    /// 本端不支持的协议直接记为失败，接着发起下一个，不等交错间隔。
    #[tokio::test(start_paused = true)]
    async fn an_unsupported_protocol_is_skipped_at_once() {
        let (result, log) = run(&[QUIC, WS], vec![(WS, Script::ok(5, 5))]).await;
        let (_, report) = result.expect("WebSocket wins");
        assert_eq!(report.protocol, WS);
        assert!(matches!(
            report.failed[0].failure,
            AttemptFailure::Unsupported { .. }
        ));
        assert_eq!(log[0], (0, WS, "establish"));
    }

    /// 重复的协议只试一次；空的优先序直接失败。
    #[tokio::test(start_paused = true)]
    async fn duplicate_protocols_are_tried_once_and_empty_orders_fail() {
        let (result, log) = run(
            &[WS, WS],
            vec![(WS, Script::establish_fails(5, AttemptFailure::Timeout))],
        )
        .await;
        assert_eq!(result.err().expect("fails").attempts.len(), 1);
        assert_eq!(log.len(), 1);

        let (result, _) = run(&[], vec![]).await;
        let failure = result.err().expect("nothing to try");
        assert!(failure.attempts.is_empty());
    }
}
