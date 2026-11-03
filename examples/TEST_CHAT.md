# Flare 聊天室测试指南

## 快速开始

### 方法 1: 使用三个终端窗口（推荐）

#### 终端 1: 启动服务器

```bash
cd /Users/hg/workspace/flare/flare-im/flare-transport
RUST_LOG=debug cargo run --example flare_chat_server
```

等待看到：
```
✓ 服务器已启动
   WebSocket: ws://127.0.0.1:8080
   QUIC: quic://127.0.0.1:8081
```

#### 终端 2: 启动第一个客户端（用户 alice）

```bash
cd /Users/hg/workspace/flare/flare-im/flare-transport
RUST_LOG=debug cargo run --example flare_chat_client -- alice
```

等待看到：
```
✓ 连接成功
 使用说明：
   - 输入消息并按回车发送
```

#### 终端 3: 启动第二个客户端（用户 bob）

```bash
cd /Users/hg/workspace/flare/flare-im/flare-transport
RUST_LOG=debug cargo run --example flare_chat_client -- bob
```

### 测试步骤

1. **在 alice 客户端输入消息**：
   ```
   Hello, this is Alice!
   ```

2. **在 bob 客户端应该看到**：
   ```
   [消息 #1] 欢迎 bob 加入聊天室！
   [消息 #2] [alice]: Hello, this is Alice!
   ```

3. **在 bob 客户端输入消息**：
   ```
   Hi Alice, this is Bob!
   ```

4. **在 alice 客户端应该看到**：
   ```
   [消息 #2] [bob]: Hi Alice, this is Bob!
   ```

5. **测试 Echo 功能**（在任意客户端）：
   ```
   echo: Hello World
   ```
   应该收到：`Echo: Hello World`

6. **退出客户端**：
   输入 `quit` 或 `exit`

## 测试场景

### 场景 1: 基本聊天功能

**目标**：验证两个用户能够正常发送和接收消息

**步骤**：
1. 启动服务器
2. 启动客户端 alice
3. 启动客户端 bob
4. alice 发送消息 "Hello Bob"
5. bob 应该收到 "[alice]: Hello Bob"
6. bob 发送消息 "Hi Alice"
7. alice 应该收到 "[bob]: Hi Alice"

**预期结果**：
- ✓ 消息正常发送
- ✓ 消息正常接收（广播）
- ✓ 消息格式正确（包含用户名）

### 场景 2: 设备冲突测试

**目标**：验证同一用户同一平台只能有一个设备在线

**步骤**：
1. 启动服务器
2. 启动第一个客户端：`cargo run --example flare_chat_client -- user1`
3. 等待连接成功
4. 启动第二个客户端（相同用户ID + 相同平台）：`cargo run --example flare_chat_client -- user1`
5. 观察第一个客户端的反应

**预期结果**：
- ✓ 第二个客户端连接成功
- ✓ 第一个客户端显示 "连接被踢下线: 设备冲突"
- ✓ 第一个客户端自动断开

### 场景 3: 多平台共存测试

**目标**：验证同一用户不同平台可以同时在线

**步骤**：
1. 启动服务器
2. 启动第一个客户端：`DEVICE_PLATFORM=pc cargo run --example flare_chat_client -- user1`
3. 等待连接成功
4. 启动第二个客户端（相同用户ID + 不同平台）：`DEVICE_PLATFORM=android cargo run --example flare_chat_client -- user1`
5. 观察两个客户端的连接状态

**预期结果**：
- ✓ 两个客户端都连接成功
- ✓ 两个客户端可以相互发送消息
- ✓ 没有设备冲突提示

### 场景 4: 协议竞速测试

**目标**：验证客户端能够自动选择最快的协议

**步骤**：
1. 启动服务器（同时监听 WebSocket 和 QUIC）
2. 启动客户端（配置协议竞速）
3. 观察客户端日志中的协议选择结果

**预期结果**：
- ✓ 客户端日志显示协议竞速过程
- ✓ 最终选择了一个协议（QUIC 或 WebSocket）
- ✓ 连接成功建立

### 场景 5: 协商机制测试

**目标**：验证序列化格式和压缩方式的协商

**步骤**：
1. 启动服务器（默认 JSON）
2. 启动客户端（不指定格式）
3. 观察协商日志

**预期结果**：
- ✓ 客户端发送 CONNECT 消息
- ✓ 服务器返回 CONNECT_ACK
- ✓ 客户端解析器更新为协商后的格式
- ✓ 后续消息使用协商后的格式

## 验证清单

测试完成后，检查以下功能是否正常：

### 服务器端
- [ ] 服务器正常启动
- [ ] 能够接受多个客户端连接
- [ ] 消息正常广播
- [ ] 设备冲突正常处理
- [ ] 欢迎消息正常发送
- [ ] 中间件正常工作（验证、日志、性能监控）
- [ ] Echo 处理器正常工作

### 客户端端
- [ ] 客户端正常连接
- [ ] 协议竞速正常工作
- [ ] 协商机制正常工作
- [ ] 消息正常发送
- [ ] 消息正常接收
- [ ] 欢迎消息正常显示
- [ ] 设备冲突正常处理（被踢提示）
- [ ] 中间件正常工作（日志、性能监控）

## 常见问题

### Q1: 客户端连接失败

**可能原因**：
- 服务器未启动
- 端口被占用
- 防火墙阻止

**解决方法**：
```bash
# 检查服务器是否运行
ps aux | grep flare_chat_server

# 检查端口占用
lsof -i :8080
lsof -i :8081

# 杀死占用进程
kill -9 <PID>
```

### Q2: 消息未收到

**可能原因**：
- 协商失败
- 消息格式错误
- 中间件拦截

**解决方法**：
- 使用 `RUST_LOG=debug` 查看详细日志
- 检查服务器日志中的消息处理记录
- 验证消息格式是否正确

### Q3: 设备冲突未触发

**可能原因**：
- 使用了不同的用户ID
- 使用了不同的平台

**解决方法**：
- 确保使用相同的用户ID
- 确保使用相同的平台（或使用默认 PC 平台）

## 性能测试

### 多客户端压力测试

启动 10 个客户端同时发送消息：

```bash
# 在服务器运行的情况下，运行以下命令
for i in {1..10}; do
    cargo run --example flare_chat_client -- "user$i" &
done
```

观察服务器是否能正常处理所有消息。

### 延迟测试

使用 `RUST_LOG=debug` 查看消息处理延迟：

```bash
RUST_LOG=debug cargo run --example flare_chat_server
```

查看日志中的时间戳，计算：
- 消息发送到接收的延迟
- 消息广播的延迟
- 中间件处理的延迟

## 日志分析

### 服务器日志关键信息

- `✓ 新连接`: 新客户端连接
- ` 用户ID`: 用户ID和连接ID
- ` [用户名]: 消息内容`: 收到的消息
- `✗ 用户断开`: 客户端断开

### 客户端日志关键信息

- `✓ 连接成功`: 连接建立
- `✓ 收到 CONNECT_ACK`: 协商完成
- `✓ 解析器已更新`: 解析器更新为协商后的格式
- `[消息 #N]`: 收到的消息
- `✗ 连接被踢下线`: 设备冲突被踢

## 下一步

测试完成后，可以：

1. **性能优化**：根据测试结果优化消息处理性能
2. **功能扩展**：添加更多聊天室功能（私聊、群组等）
3. **错误处理**：完善错误处理和重连机制
4. **监控集成**：集成 Prometheus 等监控工具

