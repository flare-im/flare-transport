# Flare Core 客户端构建器使用指南

本文档介绍 Flare Core 提供的三种客户端构建器，帮助您根据需求选择合适的构建器。

## 目录

- [三种构建器对比](#三种构建器对比)
- [ClientBuilder（毛坯房）](#clientbuilder毛坯房)
- [ObserverClientBuilder（基本装修）](#observerclientbuilder基本装修)
- [FlareClientBuilder（精装修）](#flareclientbuilder精装修)
- [功能对比表](#功能对比表)
- [迁移指南](#迁移指南)

---

## 三种构建器对比

Flare Core 提供三种不同抽象级别的客户端构建器，从简单到复杂：

| 构建器 | 抽象级别 | 适用场景 | 特点 |
|--------|---------|---------|------|
| **ClientBuilder** | 最低 | 快速原型、小型应用 | 最小实现，零配置，轻量级 |
| **ObserverClientBuilder** | 中等 | 自定义处理、事件驱动 | 自定义观察器，事件处理，消息路由 |
| **FlareClientBuilder** | 最高 | 生产环境、企业应用 | 完整功能，消息管道，中间件支持 |

### 架构层次

```
┌─────────────────────────────────────────────────────────┐
│         FlareClientBuilder (精装修)                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │  MessagePipeline + Middleware + Processor        │  │
│  │  序列化协商 + 压缩 + 加密 + 心跳 + 自动重连        │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                        ▲
                        │ 基于
┌─────────────────────────────────────────────────────────┐
│      ObserverClientBuilder (基本装修)                    │
│  ┌──────────────────────────────────────────────────┐  │
│  │  ConnectionObserver + EventHandler + Router      │  │
│  │  自定义观察器 + 事件处理 + 消息路由               │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                        ▲
                        │ 基于
┌─────────────────────────────────────────────────────────┐
│         ClientBuilder (毛坯房)                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │  闭包处理 + 基本配置                                │  │
│  │  最小实现，零依赖                                   │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                        ▲
                        │ 基于
┌─────────────────────────────────────────────────────────┐
│              HybridClient (核心实现)                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │  多协议支持 + 连接管理 + 心跳检测                   │  │
│  │  WebSocket + QUIC + ClientCore                    │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

---

## ClientBuilder（毛坯房）

### 概述

最简单的构建器，提供最小实现，没有任何"装修"。适合快速原型开发和小型应用。

### 特点

- ✓ **最小依赖**：只提供基本的消息处理（闭包）
- ✓ **零配置**：使用默认配置即可运行
- ✓ **轻量级**：不包含中间件、管道等高级功能
- ✓ **快速上手**：几行代码即可启动客户端

### 适用场景

- 快速原型开发
- 小型应用
- 学习和测试
- 需要完全控制消息处理流程的场景

### 基本使用

```rust
use flare_core::client::builder::ClientBuilder;
use flare_core::common::protocol::{Frame, frame_with_message_command, generate_message_id, Reliability};
use flare_core::common::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 创建客户端
    let mut client = ClientBuilder::new("ws://127.0.0.1:8080")
        // 设置消息处理函数
        .on_message(|frame| {
            println!("收到消息: {:?}", frame);
            Ok(())
        })
        // 设置事件处理函数
        .on_event(|event| {
            println!("事件: {:?}", event);
        })
        .build()?;

    // 连接到服务器
    client.connect().await?;

    // 发送消息示例
    let frame = frame_with_message_command(
        generate_message_id(),
        b"Hello".to_vec(),
        None,
        None,
    );
    client.send_frame(&frame).await?;

    // 等待停止信号
    tokio::signal::ctrl_c().await?;
    client.disconnect().await?;

    Ok(())
}
```

### 配置选项

```rust
ClientBuilder::new("ws://127.0.0.1:8080")
    // 协议配置
    .with_protocol(TransportProtocol::WebSocket)
    // 或启用多协议竞速
    .with_protocol_race(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
    .with_protocol_url(TransportProtocol::WebSocket, "ws://127.0.0.1:8080".to_string())
    .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())

    // 用户和认证
    .with_user_id("user123".to_string())
    .with_token("jwt_token".to_string())

    // 序列化和压缩（用于协商）
    .with_format(SerializationFormat::Json)
    .with_compression(CompressionAlgorithm::None)

    // 心跳配置
    .with_heartbeat(HeartbeatConfig::default())

    // 连接配置：单个协议一次握手（传输建立 + CONNECT_ACK）的时限、交错竞速间隔
    .with_connect_timeout(Duration::from_secs(8))
    .with_race_stagger(Duration::from_millis(300))

    // TLS 配置（QUIC 需要）
    .with_tls(TlsConfig::default())

    .build()?;
```

### 限制

- ✗ 不支持中间件
- ✗ 不支持消息管道
- ✗ 不支持自定义观察器
- ✗ 不支持事件处理器
- ✗ 不支持消息路由

---

## ObserverClientBuilder（基本装修）

### 概述

提供基本实现，用户可以自定义观察器和处理器。支持事件处理、消息路由等基本功能。

### 特点

- ✓ **自定义观察器**：实现 `ConnectionObserver` trait 自定义消息处理
- ✓ **事件处理**：支持自定义事件处理器
- ✓ **消息路由**：支持消息路由功能
- ✓ **灵活扩展**：可以添加自定义的观察器和处理器

### 适用场景

- 需要自定义消息处理逻辑
- 需要事件驱动的架构
- 需要消息路由功能

### 基本使用

```rust
use flare_core::client::{ConnectionObserver, ObserverClientBuilder};
use flare_core::transport::events::ConnectionEvent;
use flare_core::common::error::Result;
use std::sync::Arc;

// 实现自定义观察器
struct MyObserver {
    // 自定义字段
}

impl ConnectionObserver for MyObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                println!("已连接");
            }
            ConnectionEvent::Disconnected(reason) => {
                println!("连接断开: {}", reason);
            }
            ConnectionEvent::Message(data) => {
                println!("收到消息: {} bytes", data.len());
                // 自定义处理逻辑
            }
            ConnectionEvent::Error(err) => {
                eprintln!("错误: {:?}", err);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 创建自定义观察器
    let observer = Arc::new(MyObserver {});

    // 创建客户端
    let mut client = ObserverClientBuilder::new("ws://127.0.0.1:8080")
        .with_observer(observer)
        .build()?;

    client.connect().await?;

    tokio::signal::ctrl_c().await?;
    client.disconnect().await?;

    Ok(())
}
```

### 高级功能

#### 事件处理器

```rust
use flare_core::client::events::handler::ClientEventHandler;
use async_trait::async_trait;

struct MyEventHandler;

#[async_trait]
impl ClientEventHandler for MyEventHandler {
    async fn handle_system_command(&self, cmd_type: SystemCommandType, frame: &Frame) -> Result<()> {
        // 处理系统命令
        Ok(())
    }

    async fn handle_message_command(&self, cmd_type: PayloadCommandType, frame: &Frame) -> Result<()> {
        // 处理消息命令
        Ok(())
    }

    async fn handle_connection_event(&self, event: &ConnectionEvent) -> Result<()> {
        // 处理连接事件
        Ok(())
    }
}

ObserverClientBuilder::new("ws://127.0.0.1:8080")
    .with_observer(observer)
    .with_event_handler(Arc::new(MyEventHandler))
    .build()?;
```

#### 消息路由

```rust
ObserverClientBuilder::new("ws://127.0.0.1:8080")
    .with_observer(observer)
    .enable_router()  // 启用消息路由
    .build()?;

// 通过 ClientCore 注册路由处理器
// 注意：需要通过其他方式访问 ClientCore，或使用观察者模式
```

### 配置选项

```rust
ObserverClientBuilder::new("ws://127.0.0.1:8080")
    // 必须：设置观察器
    .with_observer(observer)

    // 可选：事件处理器
    .with_event_handler(event_handler)

    // 协议配置
    .with_protocol_race(vec![TransportProtocol::WebSocket, TransportProtocol::QUIC])
    .with_protocol_url(TransportProtocol::WebSocket, "ws://127.0.0.1:8080".to_string())
    .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())

    // 用户和认证
    .with_user_id("user123".to_string())
    .with_token("jwt_token".to_string())

    // 序列化和压缩（用于协商）
    .with_format(SerializationFormat::Protobuf)
    .with_compression(CompressionAlgorithm::Gzip)

    // 设备信息
    .with_device_info(DeviceInfo::new("device_id", DevicePlatform::PC))

    // 其他配置（同 ClientBuilder）
    .with_heartbeat(HeartbeatConfig::default())
    .with_connect_timeout(Duration::from_secs(10))
    .enable_router()

    .build()?;
```

### 协议竞速

```rust
// 使用协议竞速连接（自动选择最快的协议）
let mut client = ObserverClientBuilder::new("ws://127.0.0.1:8080")
    .with_observer(observer)
    .with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket])
    .with_protocol_url(TransportProtocol::WebSocket, "ws://127.0.0.1:8080".to_string())
    .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())
    .build_with_race()  // 使用协议竞速
    .await?;
```

### 限制

- ✗ 不支持消息管道
- ✗ 不支持中间件
- ✗ 不支持自动序列化/压缩协商（需要手动处理）

---

## FlareClientBuilder（精装修）

### 概述

提供完整功能，包含所有 `common` 和 `client` 模块的能力。用户只需简单配置即可使用，也可以自定义中间件、处理器等扩展功能。

### 特点

- ✓ **消息管道**：自动处理序列化、压缩、加密
- ✓ **中间件支持**：日志、性能监控、验证等
- ✓ **处理器链**：可组合多个处理器
- ✓ **序列化协商**：自动协商最佳序列化格式（JSON/Protobuf）
- ✓ **压缩协商**：自动协商压缩算法（Gzip/Zstd/None）
- ✓ **加密支持**：AES-256-GCM 加密
- ✓ **心跳管理**：自动心跳和超时管理
- ✓ **自动重连**：支持断线重连
- ✓ **多协议支持**：WebSocket + QUIC 双协议竞速
- ✓ **简单易用**：只需实现 `MessageListener` 即可
- ✓ **高度可扩展**：可以自定义中间件、处理器覆盖默认实现

### 适用场景

- 生产环境
- 需要完整功能的企业应用
- 需要高性能和可扩展性的场景
- 需要统一消息处理流程的场景

### 基本使用

```rust
use flare_core::client::builder::{FlareClientBuilder, MessageListener};
use flare_core::common::protocol::Frame;
use flare_core::common::error::Result;
use std::sync::Arc;
use async_trait::async_trait;

// 实现消息监听器
struct MyListener;

#[async_trait]
impl MessageListener for MyListener {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        println!("收到消息: {:?}", frame);
        Ok(None)
    }

    // 连接结束（对端关闭、读写出错、心跳判死、被踢、计划性关闭、主动断开）都走这里
    async fn on_disconnect(&self, reason: &DisconnectReason) -> Result<()> {
        println!("连接断开: {reason}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = Arc::new(MyListener);

    // 创建客户端（自动包含所有功能）
    let client = FlareClientBuilder::new("ws://127.0.0.1:8080")
        .with_listener(listener)
        .build_with_race()  // 使用协议竞速
        .await?;

    // 发送消息
    let frame = frame_with_message_command(
        generate_message_id(),
        b"Hello".to_vec(),
        None,
        None,
    );
    client.send_frame(&frame).await?;

    tokio::signal::ctrl_c().await?;
    client.disconnect().await?;

    Ok(())
}
```

### 中间件使用

```rust
use flare_core::common::message::{
    LoggingMiddleware, MetricsMiddleware, ValidationMiddleware, LogLevel
};

FlareClientBuilder::new("ws://127.0.0.1:8080")
    .with_listener(listener)

    // 添加中间件（按添加顺序执行）
    .with_middleware(Arc::new(
        LoggingMiddleware::new("ClientLogging")
            .with_level(LogLevel::Info)
    ))
    .with_middleware(Arc::new(
        MetricsMiddleware::new("ClientMetrics")
    ))
    .with_middleware(Arc::new(
        ValidationMiddleware::new("ClientValidation", |frame| {
            // 自定义验证逻辑
            if frame.message_id.is_empty() {
                Err(FlareError::message_format_error("Message ID is empty".to_string()))
            } else {
                Ok(())
            }
        })
    ))

    .build_with_race()
    .await?;
```

### 自定义处理器

```rust
use flare_core::common::message::{MessageProcessor, MessageContext, FunctionProcessor};

// 使用函数处理器
let echo_processor = Arc::new(FunctionProcessor::new("EchoProcessor", |ctx| async move {
    // 处理逻辑
    Ok(None)
}));

FlareClientBuilder::new("ws://127.0.0.1:8080")
    .with_listener(listener)
    .with_processor(echo_processor)
    .build_with_race()
    .await?;
```

### 完整配置示例

```rust
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::message::{LoggingMiddleware, MetricsMiddleware, LogLevel};
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::protocol::SerializationFormat;
use flare_core::common::config_types::{TransportProtocol, HeartbeatConfig};

let device_info = DeviceInfo::new("device-123", DevicePlatform::PC)
    .with_model("MacBook Pro".to_string())
    .with_app_version("1.0.0".to_string())
    .with_system_version("macOS 14.0".to_string());

let client = FlareClientBuilder::new("ws://127.0.0.1:8080")
    // 必须：设置消息监听器
    .with_listener(listener)

    // 中间件
    .with_middleware(Arc::new(LoggingMiddleware::new("ClientLogging")))
    .with_middleware(Arc::new(MetricsMiddleware::new("ClientMetrics")))

    // 协议配置（协议竞速）
    .with_protocol_race(vec![TransportProtocol::QUIC, TransportProtocol::WebSocket])
    .with_protocol_url(TransportProtocol::WebSocket, "ws://127.0.0.1:8080".to_string())
    .with_protocol_url(TransportProtocol::QUIC, "quic://127.0.0.1:8081".to_string())

    // 用户和认证
    .with_user_id("user123".to_string())
    .with_token("jwt_token".to_string())

    // 设备信息
    .with_device_info(device_info)

    // 序列化和压缩（用于协商）
    .with_format(SerializationFormat::Json)  // 初始使用 JSON，协商后可能切换到 Protobuf
    .with_compression(CompressionAlgorithm::Gzip)

    // 或强制指定格式（不协商）
    // .force_format(SerializationFormat::Protobuf)
    // .force_compression(CompressionAlgorithm::Zstd)

    // 心跳配置
    .with_heartbeat(HeartbeatConfig::default()
        .with_interval(Duration::from_secs(30))
        .with_timeout(Duration::from_secs(90)))

    // 连接配置：单个协议一次握手（传输建立 + CONNECT_ACK）的时限、交错竞速间隔
    .with_connect_timeout(Duration::from_secs(8))
    .with_race_stagger(Duration::from_millis(300))

    // TLS 配置（QUIC 需要）
    .with_tls(TlsConfig::default())

    // 消息路由（可选）
    .enable_router()

    .build_with_race()
    .await?;
```

### 自动功能

使用 `FlareClientBuilder` 时，以下功能会自动启用：

1. **序列化协商**：连接时自动协商序列化格式（JSON/Protobuf）
2. **压缩协商**：自动协商压缩算法（Gzip/Zstd/None）
3. **加密协商**：自动协商加密方式（AES-256-GCM/None）
4. **心跳检测**：自动发送心跳并检测连接健康状态
5. **不自己重连**：连接断开后以类型化原因（`DisconnectReason`）回调 `on_disconnect`，未连接时发送直接
   返回 `NOT_CONNECTED`；何时、向哪里重连由上层决定（新建客户端）
6. **消息解析**：根据协商结果自动解析消息

### 扩展能力

#### 自定义中间件

```rust
use flare_core::common::message::{MessageMiddleware, MessageContext};

struct MyCustomMiddleware {
    name: String,
}

#[async_trait]
impl MessageMiddleware for MyCustomMiddleware {
    async fn before(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        println!("[{}] 处理前: {:?}", self.name, ctx.frame);
        Ok(None)
    }

    async fn after(&self, ctx: &MessageContext, response: Option<Frame>) -> Result<Option<Frame>> {
        println!("[{}] 处理后: {:?}", self.name, response);
        Ok(None)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

FlareClientBuilder::new("ws://127.0.0.1:8080")
    .with_listener(listener)
    .with_middleware(Arc::new(MyCustomMiddleware { name: "Custom".to_string() }))
    .build_with_race()
    .await?;
```

#### 自定义处理器

```rust
use flare_core::common::message::{MessageProcessor, MessageContext};

struct MyCustomProcessor {
    name: String,
}

#[async_trait]
impl MessageProcessor for MyCustomProcessor {
    async fn process(&self, ctx: &MessageContext) -> Result<Option<Frame>> {
        // 自定义处理逻辑
        Ok(None)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

FlareClientBuilder::new("ws://127.0.0.1:8080")
    .with_listener(listener)
    .with_processor(Arc::new(MyCustomProcessor { name: "Custom".to_string() }))
    .build_with_race()
    .await?;
```

---

## 功能对比表

| 功能 | ClientBuilder | ObserverClientBuilder | FlareClientBuilder |
|------|---------------|----------------------|-------------------|
| **消息处理** | ✓ 闭包 | ✓ ConnectionObserver | ✓ MessageListener |
| **中间件** | ✗ | ✗ | ✓ |
| **消息管道** | ✗ | ✗ | ✓ |
| **序列化协商** | ✓ 手动 | ✓ 手动 | ✓ 自动 |
| **压缩协商** | ✓ 手动 | ✓ 手动 | ✓ 自动 |
| **加密支持** | ✗ | ✗ | ✓ |
| **事件处理器** | ✗ | ✓ | ✓ |
| **消息路由** | ✗ | ✓ | ✓ |
| **心跳管理** | ✓ | ✓ | ✓ |
| **自动重连** | ✓ | ✓ | ✓ |
| **多协议支持** | ✓ | ✓ | ✓ |
| **协议竞速** | ✓ | ✓ | ✓ |
| **自定义观察器** | ✗ | ✓ | ✗ |
| **自定义中间件** | ✗ | ✗ | ✓ |
| **自定义处理器** | ✗ | ✗ | ✓ |
| **代码复杂度** |  低 |  中 |  高 |
| **功能完整性** |  基础 |  中等 |  完整 |

---

## 迁移指南

### 从 ClientBuilder 迁移到 ObserverClientBuilder

```rust
// 之前：使用闭包
let client = ClientBuilder::new("ws://127.0.0.1:8080")
    .on_message(|frame| {
        // 处理逻辑
        Ok(())
    })
    .build()?;

// 之后：使用 ConnectionObserver
struct MyObserver;
impl ConnectionObserver for MyObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        if let ConnectionEvent::Message(data) = event {
            // 处理逻辑
        }
    }
}

let client = ObserverClientBuilder::new("ws://127.0.0.1:8080")
    .with_observer(Arc::new(MyObserver))
    .build()?;
```

### 从 ObserverClientBuilder 迁移到 FlareClientBuilder

```rust
// 之前：使用 ConnectionObserver
struct MyObserver;
impl ConnectionObserver for MyObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        // 处理逻辑
    }
}

let client = ObserverClientBuilder::new("ws://127.0.0.1:8080")
    .with_observer(Arc::new(MyObserver))
    .build()?;

// 之后：使用 MessageListener（更简单）
struct MyListener;
#[async_trait]
impl MessageListener for MyListener {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        // 处理逻辑（自动处理序列化、压缩等）
        Ok(None)
    }
}

let client = FlareClientBuilder::new("ws://127.0.0.1:8080")
    .with_listener(Arc::new(MyListener))
    .with_middleware(Arc::new(LoggingMiddleware::new("Logging")))
    .build_with_race()
    .await?;
```

---

## 总结

- **ClientBuilder**：适合快速原型和小型应用，提供最小实现
- **ObserverClientBuilder**：适合需要自定义处理和事件驱动的场景
- **FlareClientBuilder**：适合生产环境，提供完整功能和最佳实践

根据您的需求选择合适的构建器，也可以从简单到复杂逐步迁移。

