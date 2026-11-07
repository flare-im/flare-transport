//! 认证连接负载客户端:读预签 token 文件,用 SDK 建立 N 个**已认证** WS 连接并保持,
//! SDK 自带心跳保活。用于「N 万真在线」压测(裸 TCP/未认证 WS 会被网关回收,必须真认证)。
//!
//! 用法:
//!   TOKENS=/root/tokens.txt WS_URL=ws://127.0.0.1:60051 N=60000 DURATION=7200 \
//!   CONNECT_CONC=2000 cargo run --release --example loadconns
//!
//! tokens.txt 每行:  <userId> <token>
use flare_core::HybridClient;
use flare_core::client::ClientConfig;
use flare_core::common::config_types::HeartbeatConfig;
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let tokens_path = std::env::var("TOKENS").unwrap_or_else(|_| "/root/tokens.txt".into());
    // 多目标 IP 绕单源 IP 的 ~28k 临时端口上限:网关绑 0.0.0.0:60051,连 127.0.0.1..6:60051
    // 都到它,每个 (src,dst) 元组独立 ~28k 端口空间 → 6 个目标 IP≈17万端口容量。
    // HOSTS 逗号分隔(默认单个 127.0.0.1);PORT 默认 60051。按连接索引轮转。
    let hosts: Vec<String> = std::env::var("HOSTS")
        .unwrap_or_else(|_| "127.0.0.1".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60051);
    let ws_url = format!("ws://{}:{} (轮转 {} 个目标)", hosts[0], port, hosts.len());
    let n: usize = std::env::var("N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60000);
    let duration: u64 = std::env::var("DURATION")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7200);
    let concurrency: usize = std::env::var("CONNECT_CONC")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);

    let content = std::fs::read_to_string(&tokens_path).expect("读 tokens 文件失败");
    let toks: Vec<(String, String)> = content
        .lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            match (it.next(), it.next()) {
                (Some(u), Some(t)) => Some((u.to_string(), t.to_string())),
                _ => None,
            }
        })
        .take(n)
        .collect();
    eprintln!(
        "[loadconns] 载入 {} 个 token, 目标 N={}, url={}",
        toks.len(),
        n,
        ws_url
    );

    let connected = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let alive = Arc::new(AtomicU64::new(0));
    let sem = Arc::new(Semaphore::new(concurrency));
    // 保持 client 存活:握手后放进这里,SDK 后台任务(含心跳)随 client 存活而运行
    let holds: Arc<tokio::sync::Mutex<Vec<HybridClient>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(toks.len())));

    // 采样线程
    {
        let connected = connected.clone();
        let failed = failed.clone();
        let alive = alive.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                eprintln!(
                    "[loadconns] t+{}s 已连接={} 存活={} 失败={}",
                    start.elapsed().as_secs(),
                    connected.load(Ordering::Relaxed),
                    alive.load(Ordering::Relaxed),
                    failed.load(Ordering::Relaxed),
                );
            }
        });
    }

    let mut handles = Vec::new();
    for (idx, (uid, tok)) in toks.into_iter().enumerate() {
        let sem = sem.clone();
        // 轮转目标 IP,分散临时端口压力
        let conn_url = format!("ws://{}:{}", hosts[idx % hosts.len()], port);
        let connected = connected.clone();
        let failed = failed.clone();
        let alive = alive.clone();
        let holds = holds.clone();
        let permit = sem.acquire_owned().await.unwrap();
        handles.push(tokio::spawn(async move {
            let mut last_err = String::new();
            let mut ok = false;
            for attempt in 0..3 {
                // 每次重建 cfg(避免 ClientConfig: Clone 依赖)。网关 CONNECT 要求 device_id;
                // 每连接唯一避免设备冲突互踢。显式心跳(20s)保活,否则网关按空闲回收。
                let cfg = ClientConfig::new(conn_url.clone())
                    .websocket()
                    .with_token(tok.clone())
                    .with_device_info(DeviceInfo::new(
                        format!("load-{}", uid),
                        DevicePlatform::Web,
                    ))
                    .with_heartbeat(
                        HeartbeatConfig::default()
                            .with_interval(std::time::Duration::from_secs(20))
                            .with_timeout(std::time::Duration::from_secs(60)),
                    );
                match HybridClient::connect_with_race(cfg).await {
                    Ok(client) => {
                        connected.fetch_add(1, Ordering::Relaxed);
                        alive.fetch_add(1, Ordering::Relaxed);
                        holds.lock().await.push(client);
                        ok = true;
                        break;
                    }
                    Err(e) => {
                        last_err = format!("{}", e);
                        if attempt < 2 {
                            tokio::time::sleep(Duration::from_millis(300)).await;
                        }
                    }
                }
            }
            if !ok {
                let n = failed.fetch_add(1, Ordering::Relaxed);
                if n < 3 {
                    eprintln!("[loadconns] 连接失败样例 uid={} err={}", uid, last_err);
                }
            }
            drop(permit);
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    eprintln!(
        "[loadconns] 建立阶段完成 已连接={} 失败={},保持 {}s",
        connected.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
        duration
    );
    tokio::time::sleep(Duration::from_secs(duration)).await;
    eprintln!(
        "[loadconns] 结束,峰值连接={}",
        connected.load(Ordering::Relaxed)
    );
}
