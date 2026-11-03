# flare-core Performance Baseline

> 本文的目的不是展示漂亮数字，而是让你**自己跑出可比较的数字**。
> 所有数据都附带复现命令与环境；对不能代表生产的部分，本文直接说明。

**最新测量日期**：2026-08-03
**历史基线**：2026-06-05（见文末「历史基线与已落地优化」）

---

## 一、先说这份数据不能证明什么

企业选型时最容易被误导的就是厂商给的性能数字。所以先划清边界：

| 本文**能**回答 | 本文**不能**回答 |
|---|---|
| 单机上编解码、连接管理、事件分发的量级 | 集群吞吐、跨机房延迟 |
| 各环节的相对开销（哪里是瓶颈） | 真实用户在弱网下的体感 |
| 改动是否引入性能回退（可作回归基线） | 你的业务负载下的容量规划 |

**下面所有数字都来自一台 MacBook 的进程内基准测试，不含网络、不含数据库、
不含多机协调。** 拿它做容量规划会严重高估。它的正确用途是：
①判断架构量级是否合理 ②作为性能回归的基线。

要做容量规划，请在与生产同构的环境上跑第五节的端到端压测。

## 二、性能目标

以下为 flare-core 作为长连接与帧传输层的设计目标，供对照参考：

| 指标 | 目标值 | 说明 |
|------|--------|------|
| **消息处理延迟** | P99 < 50ms | 端到端消息处理延迟 |
| **连接建立延迟** | P99 < 100ms | 从连接到协商完成 |
| **内存占用** | < 2GB/10K 连接 | 单实例内存占用 |
| **吞吐量** | 10 万+ TPS/实例 | 单实例消息处理能力 |

## 三、测量环境

| 项 | 值 |
|---|---|
| CPU | Apple M1 Pro（10 核） |
| 内存 | 16 GB |
| 系统 | macOS 26.5.1 |
| Rust | 1.94.1 |
| 构建 | `--release`（cargo bench 默认） |

> 注意：**ARM 笔记本与生产用的 x86 服务器不可直接比较。** 同一份基准在云上
> 通用型实例通常慢 1.5–3 倍（更低的单核频率、共享 CPU、虚拟化开销）。

## 四、传输层（flare-core）

本节覆盖 `flare-core` 传输地基本身：帧序列化/解析、消息管线分发、连接管理器
注册与活跃时间更新、以及经 `ConnectionManagerTrait` 的内存扇出。它**刻意不含**
IM 域语义（`seq`、同步游标、离线存储、Social 权限 hook、推送 worker、读模型）——
那些属于 `flare-im-core`、`flare-server-core`、`flare-social` 的基准。

复现：

```bash
cd flare-core
cargo bench --bench perf_baseline --features "server,compression-gzip"
```

基准实现于 `benches/perf_baseline.rs`，不依赖外部基准框架，输出 JSON 以便 CI
或发布脚本归档比较。

| 指标 | ops/s | ns/op | 说明 |
|---|---:|---:|---|
| `codec.protobuf.round_trip.256b` | 1,488,965 | 672 | Protobuf 编解码往返 |
| `pipeline.process_raw.validate_no_response.256b` | 1,383,957 | 723 | 完整管线处理（校验，不产生响应） |
| `connection_manager.add_update_remove` | 1,104,655 | 905 | 单连接生命周期（分片管理器） |
| `codec.json.round_trip.256b` | 200,549 | 4,986 | JSON 编解码往返 |
| `codec.protobuf_gzip.round_trip.1kb` | 59,800 | 16,722 | Protobuf + gzip（1KB） |
| `connection_manager.broadcast.1000x256b` | 1,521 | 657,320 | 向 1000 个内存连接广播（一次广播为一 op） |
| `connection_manager.broadcast_frame_explicit_parser.1000x256b` | 1,720 | 581,536 | 同上，但帧只序列化一次 |
| `connection_manager.cleanup_timeout_trait.1000` | 1,080 | 926,250 | 快照+关闭+批量移除 1000 个超时连接 |

### 从数据里能读出的三件事

**① Protobuf 比 JSON 快 7.4 倍**（672ns vs 4,986ns）。协商时默认选 Protobuf 是对的，
JSON 通道只应用于调试与协商前握手。

**② gzip 的代价是 25 倍**（16,722ns vs 672ns）。压缩只对大载荷划算 ——
小文本消息开压缩是净亏。当前默认策略按体积阈值开启，与这个数据一致。

**③ 广播时预序列化能省 12%**（581µs vs 657µs，1000 连接）。差距会随连接数放大，
超大群场景应始终走 `broadcast_frame_explicit_parser` 那条路径。

> 广播那三项的 ops/s 看起来很低，是因为**一次 op 等于向 1000 个连接各发一次**。
> 换算成单连接投递约 580–660 ns/连接。这不是网络吞吐，只衡量管理器扇出开销加
> 异步锁在内存 `Connection` 实现上的成本。

## 五、客户端核心（flare-im-core-sdk）

复现：

```bash
cd flare-im-core-sdk
cargo bench --bench perf_baseline
```

该套件用 criterion，输出含置信区间与相对上次运行的变化率 —— 适合做回归检测。
关键量级：

| 指标 | 吞吐 |
|---|---|
| `protocol_codec/decode_data_packet` | ~1.27 GiB/s（66.7 ns） |
| `event_bus_publish_steady_state/0`（无订阅者） | 1.31 M/s |
| `event_bus_publish_steady_state/100`（100 订阅者） | 15.1 K/s |
| `message_send/prepare_text_message` | 1.06 M/s |
| `message_send/memory_store_save_batch_100` | 808 K 条/s |
| `event_json_serialization/sync_messages_1000_payload` | 298 K 条/s |

**事件总线随订阅者数近似线性劣化**（0 → 100 订阅者，吞吐降约 87 倍）。
这符合扇出的本质，但意味着**单进程内挂大量订阅者会成为瓶颈** ——
多设备/多视图场景应复用订阅而非各自订阅。

## 六、端到端压测（做容量规划请用这个）

上面都是进程内基准。真实容量必须连着服务端与数据库测：

```bash
# 1) 起全栈（见 flare-core-server/QUICKSTART.md）
cd flare-im-core && docker compose -f deploy/docker-compose.yml up -d && ./scripts/start_server.sh

# 2) 两用户端到端时延与吞吐
cd flare-im-core-sdk && cargo run --release --example two_user_latency_throughput

# 3) 群场景时延与吞吐
cargo run --release --example group_latency_throughput
```

**报告端到端数字时必须同时给出**：服务端与客户端是否同机、数据库规格、
网络 RTT、并发连接数。缺任何一项，数字都无法被验证或比较。

## 七、优化建议

### 1. 序列化格式选择

- **Protobuf**：推荐用于生产环境，性能比 JSON 快 3–7 倍（见第四节实测）
- **JSON**：适合调试和兼容性要求高的场景

### 2. 压缩算法选择

- **Gzip**：适合文本消息，压缩率高
- **Zstd**：适合二进制数据，压缩速度快
- **None**：适合已压缩的数据或低延迟要求
- 压缩应按**体积阈值**开启：小控制帧/聊天帧保持不压缩，超过阈值再压

### 3. 日志级别配置

- **开发环境**：`RUST_LOG=debug`
- **测试环境**：`RUST_LOG=info`
- **生产环境**：`RUST_LOG=warn`（减少日志开销）

### 4. 连接管理

- 合理设置心跳间隔（建议 30–60 秒）
- 及时清理超时连接
- 使用连接池管理连接

### 5. 消息批处理

- 对于批量消息，考虑使用批处理 API
- 减少网络往返次数
- 超大群扇出始终走预序列化路径（`broadcast_frame_explicit_parser`）

## 八、性能监控

### 关键指标

- 消息处理延迟（P50/P99/P999）
- 连接数和使用率
- 内存使用情况
- CPU 使用率
- 错误率和重试次数

### 监控工具

- 使用 Prometheus 收集指标
- 使用 Grafana 可视化监控数据
- 使用 Jaeger 进行分布式追踪

## 九、架构解读

`flare-core` 被正确定位为长连接与帧层。被测热点路径不引入 IM 消息排序、Social
规则或同步语义，因此这个地基与预期的分层保持兼容：

- `flare-core`：传输帧、协商、心跳、连接生命周期
- `flare-server-core`：运行时、上下文传播、服务发现、事件总线、可观测性
- `flare-im-core`：seq、投递、同步、存储、推送权威
- `flare-social`：通过 hook 与 bridge 投影行使用户/关系/群权威

对齐参照：

- **飞书 / Lark**：传输与业务门禁保持分离；未来的 PreSend 校验属于
  `flare-social-hook`，不属于 `flare-core`。
- **微信**：心跳与连接生命周期现已表现为可靠的长连接底座；发送 FSM 与离线重试
  仍属于 SDK/core 层。
- **Telegram**：编解码与帧路径保持同步游标中立；云同步基准应加在
  `flare-im-core`，而非此处。

## 十、把它当回归基线用

性能回退通常不是一次跳崖，而是每次改动慢 3%、半年后慢一倍。建议：

```bash
# 改动前
cargo bench --bench perf_baseline -- --save-baseline before
# 改动后
cargo bench --bench perf_baseline -- --baseline before
```

criterion 会直接标出显著变化（含 p 值）。**超过 5% 的劣化应当在 PR 里给出解释。**

本文数字未接入 CI 门禁 —— 基准测试在共享 CI 机器上噪声极大，
拿它当红绿灯会制造大量假警报。建议在固定物理机上定期跑并归档趋势。

微基准应在同一主机、相近负载下比较。单次运行差异低于 5–10% 视为噪声，
除非重复运行确认趋势。

## 十一、历史基线与已落地优化（2026-06-05）

首个 2026-06-05 基线在同机（Darwin 25.3.0 / Apple M1 Pro / 16 GiB /
rustc 1.94.1）测得的传输层量级与上表一致（Protobuf 256B 往返 ~1.0M ops/s，
JSON 约慢 7 倍，Protobuf+Gzip 1KiB 约慢 25 倍，单连接生命周期 < 1µs/op）。
围绕该基线已落地的优化如下，作为后续回归对照：

1. `MetricsMiddleware` 此前在创建 `Instant` 后立即读 `elapsed()` 导致近零耗时；
   现改为存绝对墙钟毫秒并上报真实耗时。
2. `MessagePipeline` 改用中间件/处理器列表的写时复制快照；在途中间件不再跨
   `await` 持有注册表锁，一条原始消息用单一 parser 快照完成请求处理与响应序列化。
3. `ConnectionManager` 扇出在异步写前先快照连接句柄；字节级广播避免了旧的
   ID 列表 + 逐发分片查找模式，帧扇出复用可用的每连接缓存 parser。
4. 带显式 parser 的帧扇出现在只序列化一次帧，使用轻量鉴权快照，并在扇出后按
   分片批量更新成功的 `last_active`。
5. 超时清理先快照超时连接句柄，无需重读连接表即关闭，再按分片批量移除连接与
   用户索引项；心跳清理日志改为结构化计数加少量连接 ID 采样，避免超时风暴时刷屏。
6. `ServerCore` 按协商 parser profile（`format + compression + encryption`）复用共享
   `MessagePipeline` 实例；配置了中间件/处理器时，同 profile 的成千连接不再重复分配管线。

### 后续建议优化

1. 在调用方/配置层加压缩阈值：小控制帧与聊天帧默认 Protobuf 不压缩，仅在实测
   载荷阈值以上启用 Gzip。
2. 把网关规模的扇出压测移到 `flare-server-core` 或 `flare-im-core`，须包含有界的
   每连接队列、慢消费者驱逐、在线路由分发。
3. 增加带真实 WebSocket/TCP socket 的端到端传输基准：测连接协商时延、发送/ack
   时延、空闲心跳稳定性、并发客户端重连行为。
4. 保持 `flare-core` 无 IM 语义：seq 分配、消息去重、离线拉取、Social 访问检查应
   留在此 crate 之外。

---

## 附：本报告的诚实声明

- 数字来自**一台 ARM 笔记本的进程内基准**，非生产环境实测
- 广播类指标为**内存连接**，不含真实 socket 与网络栈开销
- 未做长时间稳定性测试（内存增长、连接泄漏需另行验证）
- 端到端压测（第六节）**本报告未执行**，只给出复现方法
