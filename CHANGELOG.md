# 变更记录

本文件只记录**已发布到 crates.io 的版本**。版本号遵循
[语义化版本](https://semver.org/lang/zh-CN/)。

## 1.1.1 — 未发布

安全修复版本。1.1.0 发布于 2026-08-03，此后修掉的公告都在这里。
**建议所有 1.1.0 用户升级**。

### 安全

- 升级传输层依赖，修复 **13 项 RUSTSEC 公告**。
- 升级 `time`，修复 **RUSTSEC-2026-0009**（栈耗尽 DoS，评分 6.8）。
- 迁离 `rustls-pemfile` —— 它已停止维护且**没有修复版本**
  （RUSTSEC-2025-0134）。PEM 解析改用其继任者 `rustls-pki-types`，
  它本就在依赖树里，不引入新依赖。
- 设备冲突改按（租户，用户）判定。此前 `DeviceManager` 只按 user_id 分桶，两个租户里
  user_id 相同、平台相同的用户会在平台互斥策略下互相踢掉设备，策略带来的设备名额也跨租户
  计算。租户取自认证结果（`AuthResult::with_tenant_id`，记在 `user_metadata` 的
  `tenant_id` 下），并原样写进连接元数据；认证器没给租户、或未启用认证且客户端也没报时，
  归入默认租户 `"0"`。
- 启用认证时，设备冲突不再使用客户端在 CONNECT 里自报的 `user_id`，改用认证结果里的
  用户。此前持有效令牌的客户端报上别人的 user_id，就能把对方同平台的设备踢下线。
- 连接管理器的用户索引改按（租户，用户）建立。此前按裸 user_id 建索引：`send_to_user`
  会把帧写到其他租户里 user_id 相同的用户的连接上，`get_user_connections` 也返回其他
  租户的连接，用户数把两个租户的同名用户算成一个。现在按连接元数据里的租户（认证结果写入）
  限定，没有租户的连接归入默认租户 `"0"`；租户在用户之后才写入时，索引随之迁移。
- 启用认证时，认证器返回「通过」却没有给出用户（或给了空用户）就拒绝 CONNECT，客户端收到
  `AUTHENTICATION_FAILED`。此前回落到客户端在 CONNECT 里自报的 `user_id`，持任意有效
  令牌就能把连接绑到别人名下。

### 不兼容变更

- `DeviceManager` 的 `add_device`、`remove_device`、`get_user_devices`、
  `get_user_connections`、`get_device_connection` 以及 `handle_device_conflict`
  改为接收 `DeviceOwner`（租户，用户），不再接收裸 user_id。新增
  `DeviceManager::remove_connection`，按连接 ID 移除设备。
- `NegotiationResult` 新增 `tenant_id` 字段（客户端自报，只在未启用认证时采用）。
  用结构体字面量构造它的代码需要补上这个字段或改用 `..Default::default()`。
- `AuthResult` 新增 `retryable` 字段（见下文「修复」中的暂时性拒绝）。用结构体字面量构造
  它的代码需要补上这个字段，或改用 `success` / `failure` / `retryable_failure` 构造。
- 按用户寻址的接口改为接收 `&DeviceOwner`（租户，用户），不再接收裸 user_id：
  `ConnectionManager::get_user_connections`，`ConnectionManagerTrait` 的
  `get_user_connections`、`send_to_user`、`send_frame_to_user`，`ServerHandle::send_to_user`，
  以及 `ServerCore`、`FlareServer`、`ObserverServer`、`SimpleServer`、`MessageContext`、
  `ServerWrapper` 的 `send_to_user`。单租户部署传 `DeviceOwner::new(None, user_id)`。
  `bind_user` 签名不变，用户绑在连接当前的租户下；`user_count` 与
  `ConnectionStats::total_users` 按（租户，用户）计数。新增 `ConnectionInfo::owner`。
- `DefaultAuthenticator` 改为采用客户端在 CONNECT 元数据 `user_id` 里自报的用户（它本就
  不校验令牌），客户端没报时按认证失败拒绝。此前它返回不带用户的「通过」，由服务端回落到
  自报值，没报 `user_id` 的客户端会以无用户的身份接入。
- 传输对象（WebSocket、TCP、QUIC）没有关闭就被释放（drop）时，它的读任务随之结束：还在等
  第一个观察者的立即退出；已经在读的，读到下一帧（或对端关闭、读出错）时退出，这一帧以及之后的
  断开、错误都不再上报给观察者，底层连接随读任务一起释放。此前读任务与传输对象的生命周期无关，
  会一直读到对端关闭并照常上报。需要在释放前让观察者收到 `Disconnected` 的调用方，应先调用
  `close()` 或 `close_with_reason()`。
- `ConnectionEvent::Error` 明确为终止事件：传输先标记关闭、取走全部观察者再上报，此后不再上报
  任何事件，也不会再补一个 `Disconnected`。内置的三种传输一直是这样做的，此前文档写的是
  「非致命错误」。自定义传输若要上报可恢复的错误，不能用这个事件。
- `ClientConfig` 新增 `reconnect_on_send`，`ServerConfig` 新增 `shutdown_close_grace`（见下文「新增」）。
  用结构体字面量构造它们的代码需要补上字段或改用 `..Default::default()`；反序列化时缺这两个
  字段取默认值（`true`、1 s），行为不变。
- `ClientConnectionManager::start_auto_reconnect` 遵守 `max_reconnect_attempts`：为 `Some(n)` 时连续
  失败 n 次即停止（`ClientConfig` 的默认值是 `Some(5)`）。此前忽略这一项，一直重试。
- 压缩协商：客户端在 CONNECT 里没有指定压缩时，服务端默认压缩只在客户端声明能解它（metadata
  `accept_compression`，见下文「新增」）时才采用，否则不压缩。此前没指定就用服务端默认压缩，
  不带对应解压器的客户端（例如编译时没开 `compression-gzip`）解不开此后的帧，包括停机 CLOSE。
  本版本的客户端会自动声明；旧客户端连到默认压缩为 gzip 的新服务端时改为不压缩。
  `NegotiationResult` 新增 `accepted_compressions` 字段，用结构体字面量构造它的代码需要补上
  这个字段或改用 `..Default::default()`。
- 设备冲突踢线的 KICKED 改为类型化原因：`message` 与 metadata `reason` 都是 `DEVICE_CONFLICT`，
  metadata 仍带 `platform` 与 `new_device_id`，不再带用户 ID。此前 `message` 是含用户 ID 的中文说明、
  metadata `reason` 是小写的 `device_conflict`。客户端上报给观察者的断开原因随之为 `DEVICE_CONFLICT`
  （此前为 `设备冲突：<说明>`）。本版本的客户端照样认得旧服务端的写法；旧客户端读新服务端的帧时以
  `DEVICE_CONFLICT` 为原因，按子串（不分大小写）找 `device_conflict` 的逻辑不受影响。
- 客户端被以结束会话的原因（`DEVICE_CONFLICT`、`TENANT_UNAVAILABLE`，见 `common::kick::ends_session`）
  踢下线后进入终态：不再自动重连，`reconnect_on_send` 为 `true` 时发送前也不重连，发送返回
  `NOT_CONNECTED`，错误文本以踢线原因开头；应用重新登录后显式建连（新连接建立）才解除。此前被同平台
  新设备顶下的客户端在下一次发送时自己连回去，又把新设备顶下线，两台设备来回互踢。其他原因的踢线
  （如运维踢线 `DEVICE_KICKED`）照旧。

### 修复

- 客户端：被服务端踢线时，观察者恰好收到一次断开，原因就是 KICKED 的类型化原因。此前服务端发完
  KICKED 紧接着关连接，同一次踢线随处理顺序有两种结果：KICKED 先被处理时客户端以 `Closed by client`
  关闭连接，上层当成主动断开，既不知道被踢也不重连；关连接先被处理时协商标志已被复位，被踢不再通知，
  上层只看到一次对端关闭。现在被踢时以被踢原因关闭连接；按 KICKED 读出那一刻（而不是处理时）的
  协商状态决定是否通知；协商完成后对端关连接、而读出的帧还没处理完时，断开最多等 1 s 让它们先处理，
  被踢先上报，随后同一条连接的对端关闭与读出错不再重复上报。处理帧的任务 panic 或被取消、帧被丢弃
  时，这一帧同样记为处理完，之后的断开不会因此多等 1 s。被踢处理已经开始关连接时，读循环里并发
  到达的对端关闭或读出错也以被踢原因上报，与关连接带出的断开只报一次（此前 QUIC 上偶尔先报一次
  对端关闭、再报被踢，或被踢之后又报一次对端关闭）。
- 客户端（QUIC）：被服务端踢下线或移除之后，下一次发送不再复用旧连接。此前惰性重连把
  CONNECT 发在服务端已经不读的旧流上，传输层照样报 Connected，客户端却等不到 CONNECT_ACK，
  要等心跳在旧连接上失败（约 24 s）才真正重连。TCP 客户端有同样的问题，一并修复。
- QUIC 传输在对端结束流或读出错后即标记为关闭：此后发送立即失败，新挂的观察者只会收到
  Disconnected。读任务等第一个观察者挂上才开始读，服务端不再因观察者挂得晚而丢掉客户端
  刚发来的 CONNECT。
- 客户端：连接断开时停掉绑在这条连接上的心跳。此前旧心跳留在死连接上继续发 PING，失败时
  再关一次连接并上报断开，还占着槽位，下一条连接协商完成后起不了自己的心跳。
- 客户端：CONNECT_ACK 之前连接被关，按服务端先下发的 ERROR 帧分类：`TENANT_UNAVAILABLE`
  报 `PERMISSION_DENIED`，`AUTHENTICATION_FAILED`、`TOKEN_EXPIRED` 等错误码原样上报；
  服务端没给原因时报 `CONNECTION_FAILED`。此前一律当成令牌被拒，心跳失败、踢线后的断开
  都被误报成鉴权失败。新增 `connect_rejection_error` 与 `TENANT_UNAVAILABLE_REASON`，
  去掉未发布过的 `TOKEN_REJECTED_PREFIX`。
- 客户端：入站帧按读出那一刻的协商状态选解析器。服务端先发 KICKED 再关流时，关流事件常常
  先把协商标志复位，KICKED 随后被当成协商前的 JSON 解析失败而整帧丢掉，上层看不到踢线原因。
- 服务端：客户端断开（登出、关页面、断网后对端关闭）时立即把连接移出连接表。此前只通知
  业务层的 `on_disconnect`，连接要等心跳超时才被清掉，期间仍计入连接数与用户数、仍在用户的
  投递路由里。设备冲突踢线后第二设备登出，计数会一直停在 1。
- WebSocket、TCP 传输与 QUIC 一样，读任务等第一个观察者挂上才开始读。此前服务端握手完成后
  立即开读，观察者还没挂上时到达的 CONNECT 被读出后丢掉，客户端只能干等 CONNECT_ACK；
  这段时间里对端断开也没人收到，连接停在「已连接」。连接关闭（对端关闭、读出错或本端关闭）
  之后发送立即失败，新挂的观察者只收到 Disconnected。
- NEGOTIATION_READY 确认失败不再记 ERROR：连接已被踢掉或断开属于正常竞争，记 debug；
  连接还在但协商未完成是客户端时序问题，记 warn。
- 缺少事件处理器时返回配置错误，不再 panic。服务端在启动配置不完整时
  应当给出可读的错误，而不是崩掉。
- 服务端拒绝 CONNECT（认证失败）时，关连接前先发一个 ERROR 系统帧，编码与
  CONNECT_ACK 相同（协商前 JSON）。认证器给出类型化错误码（如 `TENANT_UNAVAILABLE`）
  时原样作为 message，其余一律为 `AUTHENTICATION_FAILED`；metadata `code` 为
  `AUTHENTICATION_FAILED`。此前连接被直接关掉，客户端分不清令牌失效和租户被停用。
- CONNECT_ACK 发出后 `ServerEventHandler::on_connect` 失败时（例如认证时租户缓存还是
  active，业务层登记会话时租户已被停用），关连接前先发一个 KICKED 系统帧，按本次协商的
  格式编码。错误原因是类型化错误码时原样作为 message 和 metadata `reason`，否则用错误码
  本身（如 `SERVICE_UNAVAILABLE`）；metadata `code` 为错误码。此前客户端刚收到 ACK 就被
  无原因断开，无法与网络故障区分。`PermissionDenied` 的 CONNECT 失败改记 warn。
- 认证器可以给出暂时性拒绝：`AuthResult::retryable_failure`，用于认证依赖暂时读不到
  （例如租户状态库抖动）、并没有对令牌或租户下结论的情况。服务端以 `SERVICE_UNAVAILABLE`
  拒绝 CONNECT，ERROR 帧的 metadata `code` 也是它，客户端拿到的错误码为
  `SERVICE_UNAVAILABLE`（可重试）。此前认证器只能报失败，客户端拿到的不是鉴权失败就是
  `TENANT_UNAVAILABLE`，前者让客户端去换令牌，后者让它终止会话、不再重连，一次数据库
  抖动就会让这段时间里连进来的客户端一直离线。
- 服务端：认证器返回 `Err`（按 `Authenticator` 的约定是系统错误，例如依赖读不到）时，以
  `SERVICE_UNAVAILABLE` 拒绝 CONNECT，与 `AuthResult::retryable_failure` 一致，客户端退避
  重连。此前报成 `AUTHENTICATION_FAILED`，客户端会因此去换一个本来有效的令牌。错误文本只进
  服务端日志，ERROR 帧的 message 与 metadata `code` 都是 `SERVICE_UNAVAILABLE`。
- 服务端：传输层以终止性错误结束连接时（例如 QUIC 对端不结束流就关掉整条连接、读流出错），
  按断开处理：业务层先收到 `on_error`，再收到 `on_disconnect`，连接立即移出连接表，设备也被
  清掉。此前只通知 `on_error`，连接要等心跳超时（默认 90 s）才被清掉，期间仍计入连接数与
  用户数、仍在用户的投递路由里。
- 服务端日志：入站帧排队期间连接已被移出连接表（踢线、断开、CONNECT 被拒）时，「更新连接活跃
  时间失败」改记 debug，这是良性竞争；连接还在却更新失败才记 warn。以 `SERVICE_UNAVAILABLE`
  拒绝的 CONNECT 在观察者里改记 warn，原因已由 `ServerCore` 按应有级别记过，不再重复记一条
  ERROR。单条连接的传输错误（QUIC 对端直接关连接、读流出错）改记 warn：客户端进程被杀、网络
  切换都会走到这里，不是服务端故障。

- 服务端：WebSocket、TCP、QUIC 服务端停止后立即退出 accept 循环。此前循环停在 accept 上，
  停止后还会再接一条连接，WebSocket 与 TCP 的监听端口一直占着，直到下一条连接到来；QUIC 端点
  不再接受新握手，停止的最后关闭端点。
- 服务端：连接移出连接表（踢线、客户端断开、CONNECT 被拒、停机）之后，本连接入站队列里剩下的
  非 CONNECT 帧直接丢弃。此前它们照常出队，按协商前的格式解析后交给业务层：被踢下线、租户被停用
  的连接排在队列里的 MESSAGE 仍会被处理。
- QUIC 传输：对端在连接层面结束连接（应用关闭、不带协议错误的传输层关闭、空闲超时、无状态重置）、
  本端已关闭连接或对端重置流时上报 `Disconnected`，应用关闭带的非空原因原样作为断开原因（例如
  `SERVER_SHUTDOWN`）。对端的传输层关闭按错误码区分：`NO_ERROR`、`CONNECTION_REFUSED`、
  `INTERNAL_ERROR`、`APPLICATION_ERROR`、`AEAD_LIMIT_REACHED`、`NO_VIABLE_PATH` 是断开；协议违例、
  帧编码、流量控制、流状态、传输参数、令牌、密钥更新等错误码与 TLS 告警是 `Error`，与本端检测到的
  协议违例、版本不符、帧长度非法一样。此前一律报 `Error`，客户端把服务端停机、对端重启都当成故障，
  只监听断开的上层不会重连；服务端业务层对断开这几类关闭只收到 `on_disconnect`，不再先收到
  `on_error`。
- 客户端（`FlareClient`）：不再按文本把 WebSocket 协议错误当作关闭协议竞速未选中连接的正常错误吞掉。
  观察者在竞速结束后才挂上，未选中的连接此时已经关闭（见下一条），被吞掉的其实是选中连接自己的终止性
  错误：当前连接以这类错误终止时，监听器既收不到断开也收不到错误，只能等下一次发送时的惰性重连，关掉了
  发送前重连的上层就停在一条死连接上。协议竞速与单协议都是如此。
- 客户端（协议竞速）：未选中的连接关闭之前先与选中连接的共享状态脱钩（观察者、「我方已主动断开」标记、
  协商与等待响应的状态）。此前竞速里不止一条连接建好时（两个协议都可达），关掉未选中连接置位的主动断开
  标记留在了共享标记上，选中的连接一直带着它：被服务端断开后发送前不再重连（报
  `client was disconnected on request`），被踢时不通知观察者，CONNECT 之后没有原因的关闭也不立即结束
  协商等待。
- 客户端（协议竞速）：第一个建好网络连接的协议胜出后立即在它上面发 CONNECT，不再等其余协议；还在建连的
  在后台中止，已经建好的在后台脱钩后关闭。此前竞速要等所有未选中的连接结束：UDP 被静默丢弃时 QUIC 握手
  要等到连接超时（默认 30 s），整个竞速在竞速时限（默认 5 s）处以 `Protocol race timed out` 失败，已经
  连上的 WebSocket 也一起丢掉，原生端与 Tauri 在屏蔽 UDP 的网络里一直连不上。竞速超时时还在建连的任务
  随之中止。文档里「列表顺序即优先级、几乎同时成功时选排在前面的」从未生效（选中的一直是最先建好的），
  改为如实描述：列表顺序是发起顺序，也是日志与全部失败时错误信息里的编号。竞速耗时日志改按字符截断错误
  信息，非 ASCII 的错误文本不再可能在截断处 panic。
- 客户端（协议竞速）：胜出的 WebSocket 直接在竞速建好的连接上发 CONNECT，不再重新拨号（QUIC、TCP 早已
  如此）。此前每次经 WebSocket 胜出都多拨一次，竞速建好的那条被直接丢掉。
- `ClientConnectionManager::start_auto_reconnect` 断开后按退避重连：从 `reconnect_interval` 起每次
  翻倍、封顶 30 s、上下浮动 20%（按毫秒取值），每次尝试前先等，与 IM SDK 的重连循环一致。此前断开
  后立即重连，失败后按固定间隔重试，同一刻断开的一批客户端会同时重连。
- 服务端与客户端日志去掉装饰用的 emoji。
- 服务端停机：宽限期满后由服务端关闭余下连接这一步合计不超过 5 s，到时还没关完的连接不再等
  关闭握手，直接移出连接表（底层连接在后台关闭）；广播停机 CLOSE 最多等 1 s。此前单条关闭
  有 1 s 上限、64 并发，但条数没有上限，大量不读也不关的连接要 N/64 秒，`stop()` 会超过接入
  网关 10 s 的运行时停机上限。按默认宽限，停机总用时约 7.5 s 以内。
- 服务端：发给协商还没完成（CONNECT_ACK 还没发出）的连接的帧，例如停机时广播的 CLOSE，按协商前
  的格式（JSON、不压缩、不加密）编码。此前按连接信息里已写入的协商结果编码，客户端此时只按协商前
  的格式解析，协商出 gzip 时解不开这一帧。协商前编码的帧，已切到协商格式的客户端照样能解。

### 新增

- 系统命令增加 `DEVICE_STATE = 12` 与 `DEVICE_STATE_ACK = 13`：客户端上报设备状态，服务端确认。
  `data` 是上层定义的载荷，传输层不解释。构建函数 `device_state`、`device_state_ack`。
  服务端新增 `ServerEventHandler::handle_device_state`，在连接的读循环里按到达顺序调用，
  返回的帧发回客户端；默认实现不处理也不回复。客户端收到 `DEVICE_STATE_ACK` 后转给
  `ClientEventHandler::handle_system_command`。同一条连接上帧按写出顺序到达，所以客户端
  收到确认时，服务端在确认之前写出的帧都已经到了。
- CONNECT 元数据键 `metadata_keys::DEVICE_STATE`：建连时的设备应用状态，小写文本 `active`、
  `standby` 或 `deferred`，缺省按前台处理。
- 客户端在协商完成后应答服务端发来的 PING。此前只有客户端发 PING、服务端回 PONG；服务端
  主动探活时收不到应答，无法区分「进程还在」和「连接已经没人读」。

- 踢线的类型化原因约定（`common::kick`）：`DEVICE_CONFLICT_REASON`、`TENANT_UNAVAILABLE_REASON`
  （原 `client::transports::client_core::TENANT_UNAVAILABLE_REASON` 保留为它的重导出）、
  `device_conflict_kicked`、`is_device_conflict_kick`、`kick_reason`、`is_device_conflict_reason`、
  `ends_session`；`ClientCore::session_ended_by_server` 返回结束会话的踢线原因。
- 服务端停机的关闭约定（`common::shutdown`）：停止前给每条在线连接下发 CLOSE 系统命令，
  `message` 与 metadata `reason` 为 `SERVER_SHUTDOWN`，metadata `code` 为
  `SERVICE_UNAVAILABLE`；最多等停机宽限（默认 `SERVER_SHUTDOWN_CLOSE_GRACE`，1 s）让客户端
  自行断开，期满还在的连接由服务端关闭：WebSocket 关闭帧为 1001（Going Away）、原因 `SERVER_SHUTDOWN`，
  QUIC 以应用错误码 0 与同一原因关闭连接。此前停机直接关连接，WebSocket 客户端看到的是 1005，
  分不清停机与故障。不认得 CLOSE 的旧客户端把它当作未知系统帧忽略，由服务端在宽限期满后关闭，
  行为与此前相同。新增 `server_shutdown_close`、`server_shutdown_close_frame`、
  `is_server_shutdown_close`、`is_server_shutdown_reason`、`server_shutdown_reconnect_delay`
  及相关常量。
- 客户端收到 CLOSE 后以类型化原因自行断开（断开原因形如
  `SERVER_SHUTDOWN: server closed the connection`）；断开原因是停机时，下一次重连（发送前的
  惰性重连与 `try_reconnect`）先在 1–5 s 之间随机等一段，只用一次，新连接建立后复位，而不是按
  配置的间隔立即重试。新增 `ClientCore::reconnect_delay` 与 `ClientCore::closed_by_server_shutdown`。
- `Connection::close_going_away`（默认回落到 `close_with_reason`）：WebSocket 关闭帧带 1001
  与原因；`QUICTransport::with_connection` 记下流所在的连接后，以应用错误码 0 与原因关闭整条
  连接。
- `ServerConfig::shutdown_close_grace` 与各服务端构建器的 `with_shutdown_close_grace`：按部署
  设置停机宽限（默认 1 s，0 表示下发 CLOSE 后立即关闭）。此前固定为 1 s。
- `ClientConfig::reconnect_on_send`（默认 `true`）与各客户端构建器的 `with_reconnect_on_send`：
  置为 `false` 时，连接断开后发送直接返回 `NOT_CONNECTED`，客户端不再自己重连（发送前的惰性重连与
  传输客户端发送时的 `try_reconnect` 都不做），重连交给上层。上层自带重连循环时（例如 IM SDK）应
  关掉它：两条路径并行时，旧客户端在下一次发送时自己连上、上层又新建一个客户端，同一用户出现两条
  连接，旧客户端还带着建连时的令牌。
- `Client::detach_from_race`（默认什么都不做）：协议竞速关掉未选中的客户端之前调用，让它不再与选中的
  连接共享状态。内置的 WebSocket、QUIC、TCP 客户端实现了它。
- `client::manager::auto_reconnect_delay_bounds`、`auto_reconnect_delay` 与
  `AUTO_RECONNECT_MAX_DELAY`：自动重连的退避区间与取值。
- `Server::stop_accepting`（默认什么都不做）：停止接受新连接、保留已建立的连接。WebSocket、
  TCP、QUIC 服务端实现了它，`stop` 也先停止接受新连接；`HybridServer::stop` 先关所有协议的入口，
  再统一通知在线连接。
- 客户端在 CONNECT 元数据 `accept_compression`（`metadata_keys::ACCEPT_COMPRESSION`，逗号分隔）里
  声明本端注册了解压器的内置压缩（开了 `compression-gzip` 时为 `gzip`）；服务端只在客户端没指定
  压缩时从中选它的默认压缩。

### 其他

- 每个示例在 `Cargo.toml` 里声明所需特性（`required-features`）。关掉默认特性构建示例时，
  缺特性的示例被跳过，不再编译失败；`tcp_client` 的要求补齐为 `client`、`tcp`、
  `compression-gzip` 与 `encryption-aes-gcm`。
- 补齐 crate 的 `license` 声明。此前 `Cargo.toml` 里没有该字段，
  SBOM 与合规扫描会把它报成 "unlicensed"。
- 修正文档中的许可证表述：本 crate 是 **Apache-2.0**，此前部分文档写成 MIT。

## 1.1.0 — 2026-08-03

与契约层 `flare-proto` 2.0.1 对齐的实现层发布。

## 1.0.3 — 2026-07-05

见 git tag `1.0.3`。
