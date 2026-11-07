//! DoH 备用解析：DNS 失败、结果可疑、TLS 失败或拨号迟迟连不上时在后台查一次主机，答复留给下一次拨号。
//!
//! 拨号路径只读缓存，从不等查询，当次失败走上层原有的退避。地址排序在 [`super::choose_addresses`]。
//! TLS 校验与 SPKI 钉扎仍按主机名，错的答复最多连不上。
//!
//! 这里管缓存、并发和退避。查询本身（解析器、HTTP、报文）在上层，经 [`DohLookup`] 注入：
//! 传输层没有 HTTP 客户端。IM 拨号、REST 和引导拉取共用同一个 [`DohCache`]。

use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use super::is_public;

/// 答复至少缓存这么久：TTL 写 0 也不能变成每次拨号查一次。
pub const MIN_DOH_TTL: Duration = Duration::from_secs(30);
/// 答复最多缓存这么久：地址迁移后，旧答复排在最前的时间有上限。
pub const MAX_DOH_TTL: Duration = Duration::from_secs(3_600);
/// 查询失败后的退避：第一次等这么久，之后翻倍。
const RETRY_BASE: Duration = Duration::from_secs(30);
const RETRY_MAX: Duration = Duration::from_secs(300);
/// 同时在查的主机数：全部接入点一起失败时，解析器不会收到一阵突发。
const MAX_CONCURRENT_LOOKUPS: usize = 2;
/// 记录的主机数上限（清单最多 256 个接入点加十几个镜像，清单更新后旧主机会留在这里）。
const MAX_TRACKED_HOSTS: usize = 512;

/// 一个主机的解析结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DohAnswer {
    pub addresses: Vec<IpAddr>,
    /// 响应里的 TTL。
    pub ttl: Duration,
}

/// 实际的 DoH 查询，由上层实现。
#[async_trait]
pub trait DohLookup: Send + Sync {
    /// 这个主机能不能查：有解析器，且名字可以发给解析器。
    fn covers(&self, host: &str) -> bool;

    /// 查一次 A、AAAA。`None`：没有解析器给出地址。
    async fn lookup(&self, host: &str) -> Option<DohAnswer>;
}

#[derive(Default)]
struct HostState {
    /// 公网地址，到 `expires_at` 为止。
    addresses: Vec<IpAddr>,
    expires_at: Option<Instant>,
    /// 这份答复已经交给拨号用过。
    dialed: bool,
    querying: bool,
    failures: u32,
    retry_at: Option<Instant>,
}

impl HostState {
    fn is_live(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|at| at > now)
    }

    fn may_query(&self, now: Instant) -> bool {
        !self.querying && !self.is_live(now) && self.retry_at.is_none_or(|at| at <= now)
    }

    /// 还有留着的价值：丢掉它会多查一次或丢一份答复。
    fn is_worth_keeping(&self, now: Instant) -> bool {
        self.querying || self.is_live(now) || self.retry_at.is_some_and(|at| at > now)
    }

    fn store(&mut self, addresses: Vec<IpAddr>, ttl: Duration, now: Instant) {
        self.addresses = addresses;
        self.expires_at = Some(now + ttl.clamp(MIN_DOH_TTL, MAX_DOH_TTL));
        self.dialed = false;
        self.failures = 0;
        self.retry_at = None;
    }

    fn back_off(&mut self, now: Instant) {
        self.addresses.clear();
        self.expires_at = None;
        self.dialed = false;
        self.retry_at = Some(now + retry_delay(self.failures));
        self.failures = self.failures.saturating_add(1);
    }
}

fn retry_delay(failures: u32) -> Duration {
    RETRY_BASE
        .saturating_mul(1 << failures.min(8))
        .min(RETRY_MAX)
}

/// 按主机缓存 DoH 答复。
pub struct DohCache {
    lookup: Arc<dyn DohLookup>,
    hosts: Mutex<HashMap<String, HostState>>,
    slots: Semaphore,
}

impl fmt::Debug for DohCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DohCache").finish_non_exhaustive()
    }
}

impl DohCache {
    pub fn new(lookup: Arc<dyn DohLookup>) -> Arc<Self> {
        Arc::new(Self {
            lookup,
            hosts: Mutex::default(),
            slots: Semaphore::new(MAX_CONCURRENT_LOOKUPS),
        })
    }

    fn hosts(&self) -> MutexGuard<'_, HashMap<String, HostState>> {
        self.hosts.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 这次拨号可用的答复，未过期且全是公网地址。
    pub fn addresses(&self, host: &str) -> Vec<IpAddr> {
        let now = Instant::now();
        let mut hosts = self.hosts();
        match hosts.get_mut(&host.to_ascii_lowercase()) {
            Some(state) if state.is_live(now) => {
                state.dialed = true;
                state.addresses.clone()
            }
            _ => Vec::new(),
        }
    }

    /// 后台查一次。主机不能查、已经在查、答复还有效、退避没到时什么也不做。IP 字面量没有可查的。
    pub fn request(self: &Arc<Self>, host: &str) {
        if host.parse::<IpAddr>().is_ok() || !self.lookup.covers(host) {
            return;
        }
        let Ok(runtime) = Handle::try_current() else {
            return;
        };
        let host = host.to_ascii_lowercase();
        if !self.claim(&host) {
            return;
        }
        let cache = self.clone();
        runtime.spawn(async move {
            let answer = match cache.slots.acquire().await {
                Ok(_slot) => cache.lookup.lookup(&host).await,
                Err(_) => None,
            };
            cache.finish(&host, answer);
        });
    }

    /// 拨这个主机时 TLS 失败。拨的就是现有答复：它没有帮上忙，丢掉，下一次回到原来的顺序，否则错的
    /// 答复会一直排在最前面。其余情况和 DNS 失败一样查一次。
    pub fn tls_failed(self: &Arc<Self>, host: &str) {
        let now = Instant::now();
        if let Some(state) = self.hosts().get_mut(&host.to_ascii_lowercase())
            && state.is_live(now)
            && state.dialed
        {
            tracing::warn!(host, "TLS failed on the DoH addresses, dropping them");
            state.back_off(now);
            return;
        }
        self.request(host);
    }

    /// 网络变了：答复和退避都只对原来的网络成立。在查的留着，同一主机同一时间只查一次。
    pub fn clear(&self) {
        self.hosts().retain(|_, state| state.querying);
    }

    /// 占下这个主机的查询名额。
    fn claim(&self, host: &str) -> bool {
        let now = Instant::now();
        let mut hosts = self.hosts();
        if hosts.len() >= MAX_TRACKED_HOSTS && !hosts.contains_key(host) {
            hosts.retain(|_, state| state.is_worth_keeping(now));
            if hosts.len() >= MAX_TRACKED_HOSTS {
                return false;
            }
        }
        let state = hosts.entry(host.to_owned()).or_default();
        if !state.may_query(now) {
            return false;
        }
        state.querying = true;
        true
    }

    fn finish(&self, host: &str, answer: Option<DohAnswer>) {
        let now = Instant::now();
        let mut hosts = self.hosts();
        let Some(state) = hosts.get_mut(host) else {
            return;
        };
        state.querying = false;
        let answer = answer.map(|answer| {
            let addresses: Vec<IpAddr> = answer
                .addresses
                .into_iter()
                .filter(|ip| is_public(*ip))
                .collect();
            (addresses, answer.ttl)
        });
        match answer {
            Some((addresses, ttl)) if !addresses.is_empty() => {
                tracing::info!(host, ?addresses, "DoH answered");
                state.store(addresses, ttl, now);
            }
            _ => {
                tracing::debug!(host, "DoH gave no public address");
                state.back_off(now);
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use tokio::sync::Notify;

    use super::*;

    const HOST: &str = "ap-1.example.com";
    const HOUR: Duration = Duration::from_secs(3_600);

    /// 按顺序给出答复，用完之后都是失败。
    #[derive(Default)]
    pub(in crate::client::dial) struct FakeLookup {
        answers: Mutex<VecDeque<Option<DohAnswer>>>,
        calls: AtomicUsize,
        running: AtomicUsize,
        peak: AtomicUsize,
        /// 设上时查询停在这里，直到 `release`。
        hold: AtomicBool,
        gate: Notify,
        refuses: AtomicBool,
    }

    impl FakeLookup {
        pub(in crate::client::dial) fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn release(&self) {
            self.hold.store(false, Ordering::SeqCst);
            self.gate.notify_waiters();
        }
    }

    #[async_trait]
    impl DohLookup for FakeLookup {
        fn covers(&self, _host: &str) -> bool {
            !self.refuses.load(Ordering::SeqCst)
        }

        async fn lookup(&self, _host: &str) -> Option<DohAnswer> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(running, Ordering::SeqCst);
            while self.hold.load(Ordering::SeqCst) {
                self.gate.notified().await;
            }
            self.running.fetch_sub(1, Ordering::SeqCst);
            self.answers.lock().unwrap().pop_front().flatten()
        }
    }

    pub(in crate::client::dial) fn answer(addresses: &[&str], ttl: Duration) -> DohAnswer {
        DohAnswer {
            addresses: addresses.iter().map(|ip| ip.parse().unwrap()).collect(),
            ttl,
        }
    }

    pub(in crate::client::dial) fn cache(
        answers: Vec<Option<DohAnswer>>,
    ) -> (Arc<DohCache>, Arc<FakeLookup>) {
        let lookup = Arc::new(FakeLookup {
            answers: Mutex::new(answers.into()),
            ..FakeLookup::default()
        });
        (DohCache::new(lookup.clone()), lookup)
    }

    /// 后台任务都在同一个线程上：让出几次就跑完了。
    pub(in crate::client::dial) async fn settle() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_lasts_for_its_ttl() {
        let ttl = Duration::from_secs(120);
        let (cache, lookup) = cache(vec![Some(answer(&["198.51.100.7"], ttl))]);
        assert!(cache.addresses(HOST).is_empty());

        cache.request(HOST);
        assert!(cache.addresses(HOST).is_empty(), "查询在后台，当次拿不到");
        settle().await;
        assert_eq!(cache.addresses(HOST), [ip("198.51.100.7")]);
        assert_eq!(
            cache.addresses(&HOST.to_ascii_uppercase()),
            [ip("198.51.100.7")]
        );

        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 1, "答复还有效，不再查");

        tokio::time::advance(ttl).await;
        assert!(cache.addresses(HOST).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn the_ttl_is_bounded_on_both_ends() {
        let (cache, _) = cache(vec![Some(answer(&["198.51.100.7"], Duration::ZERO))]);
        cache.request(HOST);
        settle().await;
        tokio::time::advance(MIN_DOH_TTL - Duration::from_secs(1)).await;
        assert!(!cache.addresses(HOST).is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(cache.addresses(HOST).is_empty());

        let (cache, _) = self::cache(vec![Some(answer(&["198.51.100.7"], 24 * HOUR))]);
        cache.request(HOST);
        settle().await;
        tokio::time::advance(MAX_DOH_TTL - Duration::from_secs(1)).await;
        assert!(!cache.addresses(HOST).is_empty());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(cache.addresses(HOST).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn one_host_is_asked_once_at_a_time() {
        let (cache, lookup) = cache(vec![Some(answer(&["198.51.100.7"], HOUR))]);
        lookup.hold.store(true, Ordering::SeqCst);
        cache.request(HOST);
        settle().await;
        cache.request(HOST);
        cache.tls_failed(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 1);

        lookup.release();
        settle().await;
        assert_eq!(cache.addresses(HOST), [ip("198.51.100.7")]);
    }

    #[tokio::test(start_paused = true)]
    async fn only_a_few_hosts_are_asked_at_once() {
        let (cache, lookup) = cache(vec![]);
        lookup.hold.store(true, Ordering::SeqCst);
        for index in 0..8 {
            cache.request(&format!("ap-{index}.example.com"));
        }
        settle().await;
        assert_eq!(lookup.calls(), MAX_CONCURRENT_LOOKUPS);

        for _ in 0..8 {
            lookup.release();
            settle().await;
        }
        assert_eq!(lookup.calls(), 8);
        assert_eq!(lookup.peak.load(Ordering::SeqCst), MAX_CONCURRENT_LOOKUPS);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_back_off_and_a_success_resets_them() {
        let (cache, lookup) = cache(vec![
            None,
            None,
            Some(answer(&["198.51.100.7"], MIN_DOH_TTL)),
            None,
        ]);
        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 1);

        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 1, "退避没到");

        tokio::time::advance(RETRY_BASE).await;
        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 2);

        tokio::time::advance(RETRY_BASE).await;
        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 2, "第二次失败等得更久");

        tokio::time::advance(RETRY_BASE).await;
        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 3);
        assert!(!cache.addresses(HOST).is_empty());

        tokio::time::advance(MIN_DOH_TTL).await;
        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 4);

        tokio::time::advance(RETRY_BASE).await;
        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 5, "成功之后退避从头算");
    }

    #[test]
    fn the_backoff_is_capped() {
        assert_eq!(retry_delay(0), RETRY_BASE);
        assert_eq!(retry_delay(1), RETRY_BASE * 2);
        assert_eq!(retry_delay(3), RETRY_BASE * 8);
        assert_eq!(retry_delay(4), RETRY_MAX);
        assert_eq!(retry_delay(u32::MAX), RETRY_MAX);
    }

    #[tokio::test(start_paused = true)]
    async fn only_public_addresses_are_kept() {
        let (cache, lookup) = cache(vec![Some(answer(
            &["10.0.0.9", "198.51.100.7", "127.0.0.1", "fd00::9"],
            HOUR,
        ))]);
        cache.request(HOST);
        settle().await;
        assert_eq!(cache.addresses(HOST), [ip("198.51.100.7")]);
        assert_eq!(lookup.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_with_no_public_address_is_a_failure() {
        let (cache, lookup) = cache(vec![Some(answer(&["10.0.0.9"], HOUR))]);
        cache.request(HOST);
        settle().await;
        assert!(cache.addresses(HOST).is_empty());

        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 1, "和查询失败一样退避");
    }

    #[tokio::test(start_paused = true)]
    async fn hosts_outside_the_lookup_are_left_alone() {
        let (cache, lookup) = cache(vec![]);
        lookup.refuses.store(true, Ordering::SeqCst);
        cache.request(HOST);
        cache.tls_failed(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 0);
        assert!(cache.hosts().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn an_ip_literal_is_never_asked() {
        let (cache, lookup) = cache(vec![]);
        cache.request("203.0.113.10");
        cache.tls_failed("2001:db8::10");
        settle().await;
        assert_eq!(lookup.calls(), 0);
        assert!(cache.hosts().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_tls_failure_starts_a_lookup() {
        let (cache, lookup) = cache(vec![Some(answer(&["198.51.100.7"], HOUR))]);
        cache.tls_failed(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        assert_eq!(cache.addresses(HOST), [ip("198.51.100.7")]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_tls_failure_on_the_answer_drops_it_and_backs_off() {
        let (cache, lookup) = cache(vec![Some(answer(&["198.51.100.7"], HOUR))]);
        cache.request(HOST);
        settle().await;
        assert!(!cache.addresses(HOST).is_empty());

        cache.tls_failed(HOST);
        settle().await;
        assert!(cache.addresses(HOST).is_empty());
        assert_eq!(lookup.calls(), 1, "丢掉答复不算查询");

        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 1, "退避之后才再查");
    }

    /// 答复到之前发起的拨号这时才报 TLS 失败：它没用过这份答复，不能丢。
    #[tokio::test(start_paused = true)]
    async fn a_late_tls_failure_keeps_an_answer_nobody_dialed() {
        let (cache, lookup) = cache(vec![Some(answer(&["198.51.100.7"], HOUR))]);
        cache.request(HOST);
        settle().await;

        cache.tls_failed(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 1);
        assert_eq!(cache.addresses(HOST), [ip("198.51.100.7")]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_network_change_forgets_answers_and_backoff() {
        let (cache, lookup) = cache(vec![None, Some(answer(&["198.51.100.7"], HOUR))]);
        cache.request(HOST);
        settle().await;
        cache.clear();
        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 2, "换了网络不用等退避");
        assert!(!cache.addresses(HOST).is_empty());

        cache.clear();
        assert!(cache.addresses(HOST).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_network_change_keeps_a_running_lookup() {
        let (cache, lookup) = cache(vec![Some(answer(&["198.51.100.7"], HOUR))]);
        lookup.hold.store(true, Ordering::SeqCst);
        cache.request(HOST);
        settle().await;

        cache.clear();
        cache.request(HOST);
        settle().await;
        assert_eq!(lookup.calls(), 1);

        lookup.release();
        settle().await;
        assert!(!cache.addresses(HOST).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn the_host_table_is_bounded() {
        let (cache, _) = cache(vec![]);
        for index in 0..MAX_TRACKED_HOSTS + 8 {
            cache.request(&format!("ap-{index}.example.com"));
            settle().await;
        }
        assert_eq!(cache.hosts().len(), MAX_TRACKED_HOSTS);

        // 退避都过了，这些记录不再有用，新主机把它们换掉。
        tokio::time::advance(RETRY_MAX).await;
        cache.request("ap-new.example.com");
        settle().await;
        assert_eq!(cache.hosts().len(), 1);
    }

    #[test]
    fn outside_a_runtime_nothing_is_asked() {
        let (cache, lookup) = cache(vec![]);
        cache.request(HOST);
        assert_eq!(lookup.calls(), 0);
        assert!(cache.hosts().is_empty());
    }
}
