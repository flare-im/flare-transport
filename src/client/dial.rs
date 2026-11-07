//! 建连地址的取舍：正常 DNS 优先，上层给的 IP（接入点的 `ipHints`）只在两种情况下用：
//!
//! - DNS 失败：解析报错、超时或结果为空。
//! - 解析结果可疑：全是非公网地址（私有、回环、链路本地），而提示里有公网地址。这是 DNS 被污染的
//!   样子。内网部署的提示也是私有地址，所以不会误判。
//!
//! DNS 失败、结果可疑、TLS 失败或拨号迟迟连不上时，[`DohCache`] 在后台用 DoH 查一次主机。下一次拨
//! 这个主机时查到的公网地址排最前，然后是公网提示、解析结果。当次拨号不等它。
//!
//! 这里只决定「连哪个 IP」。主机名不变，所以 TLS SNI 与证书校验仍按主机名，SPKI 钉扎照常生效，
//! 提示和 DoH 都不增加信任。解析成功但连不上不算 DNS 失败：不换 IP，由上层换接入点。
//!
//! 浏览器不能指定 IP，这个模块只在原生端编译。

use std::collections::HashSet;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "websocket")]
use tokio::net::TcpStream;
use tokio::net::lookup_host;
#[cfg(any(feature = "websocket", feature = "quic"))]
use tokio::time::Instant;
use tokio::time::timeout;

mod doh;

pub use doh::{DohAnswer, DohCache, DohLookup, MAX_DOH_TTL, MIN_DOH_TTL};

/// 改用提示或 DoH 地址时，每一种最多试几个 IP。
pub const MAX_DIRECT_ADDRESSES: usize = 2;

/// 有 IP 可退时，DNS 最多占建连时限的多少。DNS 不应答时系统解析器往往要等到建连时限之后才报错，
/// 不设限就永远轮不到 IP。
pub fn dns_limit(connect_timeout: Duration) -> Duration {
    connect_timeout / 2
}

/// 这次要拨的地址。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DialAddresses {
    /// 至少一个
    pub addresses: Vec<SocketAddr>,
    /// 地址来自 IP 提示，不是 DNS
    pub direct: bool,
}

/// 公网地址。私有、回环、链路本地和未指定地址都不算；IPv4 映射的 IPv6 地址按 IPv4 判断。
fn is_public(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => {
            !(ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified())
        }
        IpAddr::V6(ip) => {
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local())
        }
    }
}

fn public(ips: &[IpAddr]) -> impl Iterator<Item = IpAddr> + '_ {
    ips.iter().copied().filter(|ip| is_public(*ip))
}

fn direct_to(ip_hints: impl Iterator<Item = IpAddr>, port: u16) -> DialAddresses {
    DialAddresses {
        addresses: ip_hints
            .take(MAX_DIRECT_ADDRESSES)
            .map(|ip| SocketAddr::new(ip, port))
            .collect(),
        direct: true,
    }
}

/// 解析结果里有没有公网地址。没有就是 DNS 失败或结果可疑。
fn has_public_address(dns: &io::Result<Vec<SocketAddr>>) -> bool {
    dns.as_ref()
        .is_ok_and(|addresses| addresses.iter().any(|address| is_public(address.ip())))
}

/// 按 DNS 结果取地址（纯函数）。`doh` 里有公网地址时，它们排最前（前 [`MAX_DIRECT_ADDRESSES`] 个），
/// 然后是公网提示、解析结果，同一地址只拨一次。没有时：解析结果里有公网地址就用它；解析失败或结果
/// 为空时用 `ip_hints` 的前 [`MAX_DIRECT_ADDRESSES`] 个，端口取 `port`，没有提示时原样报失败；解析
/// 结果全是非公网地址时公网提示排在前面、解析结果接在后面，私有提示不比解析结果更可信，没有公网提示
/// 就用解析结果。
pub fn choose_addresses(
    dns: io::Result<Vec<SocketAddr>>,
    ip_hints: &[IpAddr],
    doh: &[IpAddr],
    port: u16,
) -> io::Result<DialAddresses> {
    let mut doh = public(doh).peekable();
    if doh.peek().is_some() {
        let mut chosen = direct_to(doh, port);
        chosen
            .addresses
            .extend(direct_to(public(ip_hints), port).addresses);
        chosen.addresses.extend(dns.unwrap_or_default());
        let mut seen = HashSet::new();
        chosen.addresses.retain(|address| seen.insert(*address));
        return Ok(chosen);
    }
    let failure = match dns {
        Ok(addresses) if !addresses.is_empty() => {
            let suspicious = !addresses.iter().any(|address| is_public(address.ip()));
            let mut public_hints = public(ip_hints).peekable();
            if suspicious && public_hints.peek().is_some() {
                // 解析结果接在提示后面：分离解析的内网里提示可能不通，TLS 校验保证连不到冒充者。
                let mut chosen = direct_to(public_hints, port);
                chosen.addresses.extend(addresses);
                return Ok(chosen);
            }
            return Ok(DialAddresses {
                addresses,
                direct: false,
            });
        }
        Ok(_) => io::Error::new(io::ErrorKind::NotFound, "DNS returned no addresses"),
        Err(error) => error,
    };
    if ip_hints.is_empty() {
        return Err(failure);
    }
    Ok(direct_to(ip_hints.iter().copied(), port))
}

/// 解析 `host`，取舍见 [`choose_addresses`]。有提示或 DoH 地址时 DNS 受 [`dns_limit`] 约束，超时也算
/// 失败；都没有时不另设时限，由调用方的建连时限兜底。DNS 失败或结果可疑时让 `doh` 在后台查一次，
/// 结果留给下一次拨号。IP 字面量没有解析这一步，按写的地址拨。
pub async fn resolve(
    host: &str,
    port: u16,
    ip_hints: &[IpAddr],
    doh: Option<&Arc<DohCache>>,
    connect_timeout: Duration,
) -> io::Result<DialAddresses> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(DialAddresses {
            addresses: vec![SocketAddr::new(ip, port)],
            direct: false,
        });
    }
    let known = doh.map(|doh| doh.addresses(host)).unwrap_or_default();
    let mut answer = pin!(lookup(host, port));
    let dns = match timeout(dns_limit(connect_timeout), &mut answer).await {
        Ok(dns) => dns,
        Err(_) if !ip_hints.is_empty() || !known.is_empty() => Err(io::ErrorKind::TimedOut.into()),
        Err(_) => {
            // 没有别的地址可退：继续等系统解析器，但调用方的时限可能先到，DoH 现在就开始查。
            if let Some(doh) = doh {
                doh.request(host);
            }
            answer.await
        }
    };
    if let Some(doh) = doh
        && !has_public_address(&dns)
    {
        doh.request(host);
    }
    let chosen = choose_addresses(dns, ip_hints, &known, port)?;
    if chosen.direct {
        tracing::warn!(
            host,
            "DNS failed or looks hijacked, dialing other addresses first"
        );
    }
    Ok(chosen)
}

async fn lookup(host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
    Ok(lookup_host((host, port)).await?.collect())
}

/// 到 `deadline` 还剩的时间平分给剩下的 `attempts` 次尝试：一个黑洞 IP 不能吃掉后面的机会。
#[cfg(any(feature = "websocket", feature = "quic"))]
pub(crate) fn share_of(deadline: Instant, attempts: usize) -> Duration {
    deadline.saturating_duration_since(Instant::now()) / attempts.max(1) as u32
}

/// 等 `dial` 完成。用掉 `limit` 的一半还没完成，就让 `doh` 在后台查一次 `host`，自己接着等。
///
/// 拨号超时是 DNS 污染的常见表现：解析成功了，地址却是黑洞。等真超时才查来不及：竞速的时限和这里一样
/// 长，又开始得更早，一到就取消这个 future，这里什么也看不到。
#[cfg(any(feature = "websocket", feature = "quic"))]
pub(crate) async fn watch_dial<F: Future>(
    host: &str,
    doh: Option<&Arc<DohCache>>,
    limit: Duration,
    dial: F,
) -> F::Output {
    let Some(doh) = doh else {
        return dial.await;
    };
    let mut dial = pin!(dial);
    match timeout(limit / 2, &mut dial).await {
        Ok(output) => output,
        Err(_) => {
            doh.request(host);
            dial.await
        }
    }
}

/// 建一条 TCP 连接。解析结果按顺序逐个试；IP 提示每个分一份时限。
#[cfg(feature = "websocket")]
pub(crate) async fn connect_tcp(
    host: &str,
    port: u16,
    ip_hints: &[IpAddr],
    doh: Option<&Arc<DohCache>>,
    connect_timeout: Duration,
) -> io::Result<TcpStream> {
    let deadline = Instant::now() + connect_timeout;
    let chosen = resolve(host, port, ip_hints, doh, connect_timeout).await?;
    if !chosen.direct {
        return TcpStream::connect(chosen.addresses.as_slice()).await;
    }
    let mut last_error = io::Error::from(io::ErrorKind::TimedOut);
    for (index, address) in chosen.addresses.iter().enumerate() {
        let share = share_of(deadline, chosen.addresses.len() - index);
        match timeout(share, TcpStream::connect(*address)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(error)) => last_error = error,
            Err(_) => last_error = io::ErrorKind::TimedOut.into(),
        }
    }
    Err(last_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: Duration = Duration::from_secs(2);

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    fn address(text: &str) -> SocketAddr {
        text.parse().unwrap()
    }

    /// 没有 DoH 地址时的取舍。
    fn choose(
        dns: io::Result<Vec<SocketAddr>>,
        ip_hints: &[IpAddr],
        port: u16,
    ) -> io::Result<DialAddresses> {
        choose_addresses(dns, ip_hints, &[], port)
    }

    #[test]
    fn a_resolved_host_never_uses_the_hints() {
        let chosen = choose(
            Ok(vec![address("198.51.100.7:443")]),
            &[ip("203.0.113.10")],
            443,
        )
        .unwrap();
        assert_eq!(chosen.addresses, [address("198.51.100.7:443")]);
        assert!(!chosen.direct);
    }

    #[test]
    fn a_dns_failure_uses_the_hints_on_the_url_port() {
        let chosen = choose(
            Err(io::ErrorKind::NotFound.into()),
            &[ip("203.0.113.10"), ip("2001:db8::10")],
            8443,
        )
        .unwrap();
        assert_eq!(
            chosen.addresses,
            [address("203.0.113.10:8443"), address("[2001:db8::10]:8443")]
        );
        assert!(chosen.direct);
    }

    #[test]
    fn an_empty_answer_counts_as_a_failure() {
        let chosen = choose(Ok(vec![]), &[ip("203.0.113.10")], 443).unwrap();
        assert!(chosen.direct);
        let error = choose(Ok(vec![]), &[], 443).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn without_hints_the_failure_stands() {
        let error = choose(Err(io::ErrorKind::TimedOut.into()), &[], 443).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn direct_attempts_are_capped() {
        let hints = [ip("203.0.113.10"), ip("203.0.113.11"), ip("203.0.113.12")];
        let chosen = choose(Err(io::ErrorKind::NotFound.into()), &hints, 443).unwrap();
        assert_eq!(chosen.addresses.len(), MAX_DIRECT_ADDRESSES);
        assert_eq!(chosen.addresses[0], address("203.0.113.10:443"));
    }

    #[test]
    fn an_answer_with_no_public_address_gives_way_to_a_public_hint() {
        for answer in [
            "10.1.2.3:443",
            "172.16.0.1:443",
            "192.168.1.1:443",
            "127.0.0.1:443",
            "169.254.1.1:443",
            "0.0.0.0:443",
            "[::1]:443",
            "[fd00::1]:443",
            "[fe80::1]:443",
            "[::ffff:10.1.2.3]:443",
        ] {
            let chosen = choose(Ok(vec![address(answer)]), &[ip("203.0.113.10")], 8443).unwrap();
            assert_eq!(
                chosen.addresses,
                [address("203.0.113.10:8443"), address(answer)],
                "{answer}"
            );
            assert!(chosen.direct, "{answer}");
        }
    }

    #[test]
    fn a_suspicious_answer_only_gives_way_to_the_public_hints() {
        let hints = [
            ip("10.0.0.9"),
            ip("203.0.113.10"),
            ip("203.0.113.11"),
            ip("203.0.113.12"),
        ];
        let chosen = choose(Ok(vec![address("10.1.2.3:443")]), &hints, 443).unwrap();
        assert_eq!(
            chosen.addresses,
            [
                address("203.0.113.10:443"),
                address("203.0.113.11:443"),
                address("10.1.2.3:443"),
            ]
        );
        assert!(chosen.direct);
    }

    #[test]
    fn a_private_deployment_keeps_its_private_answer() {
        let chosen = choose(
            Ok(vec![address("10.1.2.3:443")]),
            &[ip("10.0.0.9"), ip("fd00::9")],
            443,
        )
        .unwrap();
        assert_eq!(chosen.addresses, [address("10.1.2.3:443")]);
        assert!(!chosen.direct);
    }

    #[test]
    fn one_public_address_clears_the_answer() {
        let answer = vec![address("10.1.2.3:443"), address("198.51.100.7:443")];
        let chosen = choose(Ok(answer.clone()), &[ip("203.0.113.10")], 443).unwrap();
        assert_eq!(chosen.addresses, answer);
        assert!(!chosen.direct);
    }

    #[test]
    fn a_private_answer_with_no_hints_stands() {
        let chosen = choose(Ok(vec![address("10.1.2.3:443")]), &[], 443).unwrap();
        assert_eq!(chosen.addresses, [address("10.1.2.3:443")]);
        assert!(!chosen.direct);
    }

    #[test]
    fn doh_answers_go_first_then_public_hints_then_the_dns_answer() {
        let chosen = choose_addresses(
            Ok(vec![address("198.51.100.7:443")]),
            &[ip("10.0.0.9"), ip("203.0.113.10")],
            &[ip("198.51.100.20")],
            443,
        )
        .unwrap();
        assert_eq!(
            chosen.addresses,
            [
                address("198.51.100.20:443"),
                address("203.0.113.10:443"),
                address("198.51.100.7:443"),
            ]
        );
        assert!(chosen.direct);
    }

    #[test]
    fn doh_answers_rescue_a_failed_lookup_with_no_hints() {
        let chosen = choose_addresses(
            Err(io::ErrorKind::NotFound.into()),
            &[],
            &[ip("198.51.100.20"), ip("2001:db8::20"), ip("198.51.100.21")],
            8443,
        )
        .unwrap();
        assert_eq!(
            chosen.addresses,
            [
                address("198.51.100.20:8443"),
                address("[2001:db8::20]:8443")
            ]
        );
        assert!(chosen.direct);
    }

    #[test]
    fn private_doh_answers_do_not_count() {
        let chosen = choose_addresses(
            Ok(vec![address("198.51.100.7:443")]),
            &[],
            &[ip("10.0.0.9"), ip("127.0.0.1"), ip("fd00::9")],
            443,
        )
        .unwrap();
        assert_eq!(chosen.addresses, [address("198.51.100.7:443")]);
        assert!(!chosen.direct);

        let error = choose_addresses(
            Err(io::ErrorKind::NotFound.into()),
            &[],
            &[ip("10.0.0.9")],
            443,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn an_address_listed_twice_is_dialed_once() {
        let chosen = choose_addresses(
            Ok(vec![
                address("198.51.100.20:443"),
                address("198.51.100.7:443"),
            ]),
            &[ip("198.51.100.20")],
            &[ip("198.51.100.20")],
            443,
        )
        .unwrap();
        assert_eq!(
            chosen.addresses,
            [address("198.51.100.20:443"), address("198.51.100.7:443")]
        );
    }

    #[tokio::test]
    async fn a_dns_failure_asks_doh_for_the_next_dial_without_waiting_for_it() {
        let (cache, lookup) = doh::tests::cache(vec![Some(doh::tests::answer(
            &["198.51.100.20"],
            Duration::from_secs(60),
        ))]);
        assert!(
            resolve("ap-1.invalid", 443, &[], Some(&cache), LIMIT)
                .await
                .is_err(),
            "这一次没有地址可拨，照常失败"
        );
        doh::tests::settle().await;
        assert_eq!(lookup.calls(), 1);

        let chosen = resolve("ap-1.invalid", 443, &[], Some(&cache), LIMIT)
            .await
            .unwrap();
        assert_eq!(chosen.addresses, [address("198.51.100.20:443")]);
        assert!(chosen.direct);
    }

    #[tokio::test]
    async fn a_suspicious_answer_asks_doh_too() {
        let (cache, lookup) = doh::tests::cache(vec![None]);
        let chosen = resolve("localhost", 443, &[], Some(&cache), LIMIT)
            .await
            .unwrap();
        assert!(!chosen.direct, "没有别的地址时还是用解析结果");
        doh::tests::settle().await;
        assert_eq!(lookup.calls(), 1);
    }

    #[tokio::test]
    async fn an_ip_literal_never_reaches_doh() {
        let (cache, lookup) = doh::tests::cache(vec![]);
        resolve("127.0.0.1", 443, &[], Some(&cache), LIMIT)
            .await
            .unwrap();
        doh::tests::settle().await;
        assert_eq!(lookup.calls(), 0);
    }

    #[tokio::test]
    async fn an_unresolvable_name_falls_back_to_the_hints() {
        let chosen = resolve("ap-1.invalid", 443, &[ip("203.0.113.10")], None, LIMIT)
            .await
            .unwrap();
        assert_eq!(chosen.addresses, [address("203.0.113.10:443")]);
        assert!(chosen.direct);

        assert!(
            resolve("ap-1.invalid", 443, &[], None, LIMIT)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn an_ip_literal_is_neither_a_dns_failure_nor_a_suspicious_answer() {
        let chosen = resolve("127.0.0.1", 9000, &[ip("203.0.113.10")], None, LIMIT)
            .await
            .unwrap();
        assert_eq!(chosen.addresses, [address("127.0.0.1:9000")]);
        assert!(!chosen.direct);
    }

    #[tokio::test]
    async fn a_name_that_resolves_to_loopback_moves_to_a_public_hint() {
        let chosen = resolve("localhost", 443, &[ip("203.0.113.10")], None, LIMIT)
            .await
            .unwrap();
        assert_eq!(chosen.addresses[0], address("203.0.113.10:443"));
        assert!(chosen.addresses.len() > 1, "解析结果接在提示后面");
        assert!(chosen.direct);

        let chosen = resolve("localhost", 443, &[ip("10.0.0.9")], None, LIMIT)
            .await
            .unwrap();
        assert!(!chosen.direct, "内网部署的提示不算");
    }

    #[cfg(any(feature = "websocket", feature = "quic"))]
    #[tokio::test(start_paused = true)]
    async fn the_budget_is_split_between_the_remaining_attempts() {
        let deadline = Instant::now() + Duration::from_secs(8);
        assert_eq!(share_of(deadline, 2), Duration::from_secs(4));
        tokio::time::advance(Duration::from_secs(4)).await;
        assert_eq!(share_of(deadline, 1), Duration::from_secs(4));
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(share_of(deadline, 1), Duration::ZERO);
    }

    /// 拨号用时过半就查，拨号本身不受影响：它一直等到自己完成。
    #[cfg(any(feature = "websocket", feature = "quic"))]
    #[tokio::test(start_paused = true)]
    async fn a_slow_dial_asks_doh_and_keeps_going() {
        let (cache, lookup) = doh::tests::cache(vec![None]);
        let dial = tokio::spawn({
            let cache = cache.clone();
            async move {
                watch_dial("ap-1.example.com", Some(&cache), LIMIT, async {
                    tokio::time::sleep(LIMIT).await;
                    "dialed"
                })
                .await
            }
        });

        tokio::time::sleep(LIMIT / 2 - Duration::from_millis(1)).await;
        doh::tests::settle().await;
        assert_eq!(lookup.calls(), 0, "还没过半");

        tokio::time::sleep(Duration::from_millis(2)).await;
        doh::tests::settle().await;
        assert_eq!(lookup.calls(), 1);
        assert!(!dial.is_finished(), "拨号不等查询，也不被它打断");

        assert_eq!(dial.await.unwrap(), "dialed");
    }

    #[cfg(any(feature = "websocket", feature = "quic"))]
    #[tokio::test(start_paused = true)]
    async fn a_quick_dial_leaves_doh_alone() {
        let (cache, lookup) = doh::tests::cache(vec![]);
        let dialed = watch_dial("ap-1.example.com", Some(&cache), LIMIT, async {
            tokio::time::sleep(LIMIT / 4).await;
            "dialed"
        })
        .await;
        assert_eq!(dialed, "dialed");

        tokio::time::sleep(LIMIT).await;
        doh::tests::settle().await;
        assert_eq!(lookup.calls(), 0);
    }

    /// 竞速的时限先到，拨号被取消：查询已经发出去了。
    #[cfg(any(feature = "websocket", feature = "quic"))]
    #[tokio::test(start_paused = true)]
    async fn a_dial_cancelled_at_the_deadline_has_already_asked_doh() {
        let (cache, lookup) = doh::tests::cache(vec![Some(doh::tests::answer(
            &["198.51.100.20"],
            Duration::from_secs(60),
        ))]);
        let stalled = watch_dial(
            "ap-1.example.com",
            Some(&cache),
            LIMIT,
            std::future::pending::<()>(),
        );
        assert!(timeout(LIMIT, stalled).await.is_err());

        doh::tests::settle().await;
        assert_eq!(lookup.calls(), 1);
        assert_eq!(cache.addresses("ap-1.example.com"), [ip("198.51.100.20")]);
    }

    #[cfg(any(feature = "websocket", feature = "quic"))]
    #[tokio::test(start_paused = true)]
    async fn without_doh_a_dial_is_left_as_it_is() {
        let dialed = watch_dial("ap-1.example.com", None, LIMIT, async {
            tokio::time::sleep(LIMIT).await;
            "dialed"
        })
        .await;
        assert_eq!(dialed, "dialed");
    }

    #[test]
    fn dns_gets_half_of_the_connect_timeout() {
        assert_eq!(dns_limit(Duration::from_secs(8)), Duration::from_secs(4));
    }

    #[cfg(feature = "websocket")]
    #[tokio::test]
    async fn connect_tcp_moves_on_from_a_refused_hint() {
        // 同一端口只有 127.0.0.1 在听：第一个提示是 IPv6 回环，连不上就试下一个。
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let hints = [ip("::1"), ip("127.0.0.1")];
        let (stream, accepted) = tokio::join!(
            connect_tcp("ap-1.invalid", port, &hints, None, LIMIT),
            listener.accept()
        );
        assert_eq!(stream.unwrap().peer_addr().unwrap().port(), port);
        accepted.unwrap();
    }

    #[cfg(feature = "websocket")]
    #[tokio::test]
    async fn connect_tcp_reports_the_last_failure_when_every_hint_fails() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let error = connect_tcp("ap-1.invalid", port, &[ip("127.0.0.1")], None, LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionRefused);
    }
}
