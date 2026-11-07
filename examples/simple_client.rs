//! 简化的 WebSocket 聊天室客户端示例
//!
//! 使用简单模式的 Builder（ClientBuilder）构建客户端
//! 只需要简单的配置和消息处理函数即可完成长链接通信
//!
//! 实现一个功能完整的聊天室客户端，支持发送和接收消息
//! 注意：此示例使用纯 WebSocket 连接（ws://），不使用 TLS/SSL
//!
//! 功能特性：
//! - 简化的 API：使用闭包定义处理逻辑，无需实现 trait
//! - 自动连接管理：自动处理连接、重连等
//! - 心跳检测：自动保持连接活跃（通过配置）
//! - 消息路由：可选的消息路由功能
//! - 连接状态：完整的连接状态跟踪
//! - 消息统计：接收消息计数
//!
//! 此示例展示了如何：
//! 1. 使用闭包定义消息和事件处理逻辑（无需实现 trait）
//! 2. 使用 ClientBuilder 快速创建客户端
//! 3. 配置心跳、重连等功能
//! 4. 自动处理观察者注册和连接管理
//! 5. 完整的聊天室功能实现和测试

use flare_core::client::ClientBuilder;
use flare_core::common::config_types::HeartbeatConfig;
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::{
    Reliability, frame_with_message_command, generate_message_id, send_message,
};
use flare_core::transport::events::ConnectionEvent;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

// 全局消息计数器（用于测试和统计）
static MESSAGE_COUNT: AtomicU64 = AtomicU64::new(0);
static EVENT_COUNT: AtomicU64 = AtomicU64::new(0);

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 初始化 tracing（默认使用 debug 级别，方便调试）
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")),
        )
        .init();

    println!("=== 简化的 WebSocket 聊天室客户端（完整功能）===");
    println!();
    println!("功能特性：");
    println!("  - 简化的 API：使用闭包定义处理逻辑");
    println!("  - 自动连接管理：自动处理连接、重连等");
    println!("  - 心跳检测：自动保持连接活跃");
    println!("  - 消息统计：接收消息计数");
    println!("  - 连接状态：完整的连接状态跟踪");
    println!();

    // 获取用户名
    print!("请输入您的用户名: ");
    io::stdout().flush()?;
    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();

    if username.is_empty() {
        println!("用户名不能为空");
        return Err("用户名不能为空".into());
    }

    println!("\n正在连接到聊天室服务器...");

    // 配置心跳（30秒间隔，60秒超时）
    let heartbeat_config = HeartbeatConfig::new()
        .with_interval(Duration::from_secs(30))
        .with_timeout(Duration::from_secs(60));

    // 使用 ClientBuilder 创建客户端，只需定义消息处理逻辑
    // 展示所有可用的配置选项
    let mut client = ClientBuilder::new("ws://127.0.0.1:8080")
        // 处理接收到的消息
        .on_message({
            let username = username.clone();
            move |frame| {
                // 更新消息计数（用于测试）
                MESSAGE_COUNT.fetch_add(1, Ordering::Relaxed);

                // 检查是否是消息命令
                if let Some(cmd) = &frame.command
                    && let Some(Type::Payload(msg_cmd)) = &cmd.r#type
                {
                    let message = match String::from_utf8(msg_cmd.payload.clone()) {
                        Ok(text) => text,
                        Err(_) => {
                            // 如果不是有效的UTF-8，则显示十六进制调试信息
                            format!("<protobuf_binary_data: {} bytes>", msg_cmd.payload.len())
                        }
                    };

                    // 检查是否是系统通知
                    if let Some(type_bytes) = msg_cmd.metadata.get("type") {
                        let msg_type = match String::from_utf8(type_bytes.clone()) {
                            Ok(text) => text,
                            Err(_) => {
                                // 如果不是有效的UTF-8，则显示十六进制调试信息
                                format!("<invalid_type_{}>", hex::encode(type_bytes))
                            }
                        };
                        if msg_type == "join" || msg_type == "leave" {
                            println!("\n[系统] {}", message);
                        } else {
                            println!("\n{}", message);
                        }
                    } else {
                        println!("\n{}", message);
                    }

                    // 显示输入提示
                    print!("{}> ", username);
                    let _ = io::stdout().flush();
                }
                Ok(())
            }
        })
        // 处理连接事件
        .on_event({
            let username = username.clone();
            move |event| {
                // 更新事件计数（用于测试）
                EVENT_COUNT.fetch_add(1, Ordering::Relaxed);

                match event {
                    ConnectionEvent::Connected => {
                        println!("\n[系统] ✅ 已连接到聊天室服务器！");
                        print!("{}> ", username);
                        let _ = io::stdout().flush();
                    }
                    ConnectionEvent::Disconnected(reason) => {
                        println!("\n[系统] ❌ 连接已断开: {}", reason);
                    }
                    ConnectionEvent::Error(e) => {
                        eprintln!("\n[错误] {:?}", e);
                    }
                    _ => {}
                }
            }
        })
        // 配置心跳
        .with_heartbeat(heartbeat_config)
        // 配置连接超时
        .with_connect_timeout(Duration::from_secs(10))
        // 可选：启用消息路由（如果需要路由功能）
        // .enable_router()
        .build()?;

    // 连接服务器
    println!("正在连接...");
    match client.connect().await {
        Ok(_) => {
            println!("✅ 连接成功！");
            println!("使用的协议: {:?}", client.active_protocol());
            println!("连接 ID: {:?}", client.connection_id().await);
            println!(
                "连接状态: {}",
                if client.is_connected().await {
                    "已连接"
                } else {
                    "未连接"
                }
            );
            println!();
        }
        Err(e) => {
            eprintln!("❌ 连接失败: {}", e);
            eprintln!("提示: 请确保服务器已启动（运行 websocket_server 或 hybrid_server 示例）");
            return Err(format!("连接失败: {}", e).into());
        }
    }

    // 发送用户名信息（首次连接时）
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("username".to_string(), username.as_bytes().to_vec());
    metadata.insert("type".to_string(), "init".as_bytes().to_vec());

    let init_msg = send_message(
        generate_message_id(),
        format!("{} 已加入聊天室", username).into_bytes(),
        Some(metadata),
        None,
    );

    let init_frame = frame_with_message_command(init_msg, Reliability::BestEffort);

    if let Err(e) = client.send_frame(&init_frame).await {
        eprintln!("⚠️  发送初始化消息失败: {}", e);
    }

    println!("欢迎来到聊天室！");
    println!("命令说明：");
    println!("  - 输入消息后按回车发送");
    println!("  - 输入 '/quit' 或 '/exit' 退出");
    println!("  - 输入 '/stats' 查看消息和事件统计");
    println!("  - 输入 '/status' 查看连接状态");
    println!("  - 输入 '/test' 发送测试消息");
    println!();
    print!("{}> ", username);
    io::stdout().flush()?;

    // 创建异步输入任务
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    // 测试计数器
    let mut test_message_count = 0u32;

    loop {
        // 使用 tokio::select! 同时监听输入和连接状态
        tokio::select! {
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => {
                        // EOF
                        println!("\n输入结束，断开连接...");
                        break;
                    }
                    Ok(_) => {
                        let input = line.trim().to_string();
                        line.clear();

                        if input.is_empty() {
                            print!("{}> ", username);
                            let _ = io::stdout().flush();
                            continue;
                        }

                        // 处理命令
                        match input.as_str() {
                            "/quit" | "/exit" => {
                                println!("退出聊天室...");
                                break;
                            }
                            "/stats" => {
                                let msg_count = MESSAGE_COUNT.load(Ordering::Relaxed);
                                let event_count = EVENT_COUNT.load(Ordering::Relaxed);
                                println!("\n[统计] 接收消息数: {}", msg_count);
                                println!("[统计] 接收事件数: {}", event_count);
                                println!("[统计] 测试消息数: {}", test_message_count);
                                print!("{}> ", username);
                                let _ = io::stdout().flush();
                                continue;
                            }
                            "/status" => {
                                let is_connected = client.is_connected().await;
                                let conn_id = client.connection_id().await;
                                let protocol = client.active_protocol();
                                println!("\n[状态] 连接状态: {}", if is_connected { "已连接" } else { "未连接" });
                                println!("[状态] 连接 ID: {:?}", conn_id);
                                println!("[状态] 使用协议: {:?}", protocol);
                                print!("{}> ", username);
                                let _ = io::stdout().flush();
                                continue;
                            }
                            "/test" => {
                                // 发送测试消息
                                test_message_count += 1;
                                let test_msg = format!("测试消息 #{}", test_message_count);

                                let mut msg_metadata = std::collections::HashMap::new();
                                msg_metadata.insert("username".to_string(), username.as_bytes().to_vec());
                                msg_metadata.insert("type".to_string(), "test".as_bytes().to_vec());

                                let chat_msg = send_message(
                                    generate_message_id(),
                                    test_msg.as_bytes().to_vec(),
                                    Some(msg_metadata),
                                    None,
                                );

                                let chat_frame = frame_with_message_command(
                                    chat_msg,
                                    Reliability::BestEffort,
                                );

                                if let Err(e) = client.send_frame(&chat_frame).await {
                                    eprintln!("\n[错误] 发送测试消息失败: {}", e);
                                } else {
                                    println!("\n[测试] 已发送测试消息 #{}", test_message_count);
                                }
                                print!("{}> ", username);
                                let _ = io::stdout().flush();
                                continue;
                            }
                            _ => {
                                // 发送消息
                                let mut msg_metadata = std::collections::HashMap::new();
                                msg_metadata.insert("username".to_string(), username.as_bytes().to_vec());

                                let chat_msg = send_message(
                                    generate_message_id(),
                                    input.as_bytes().to_vec(),
                                    Some(msg_metadata),
                                    None,
                                );

                                let chat_frame = frame_with_message_command(
                                    chat_msg,
                                    Reliability::BestEffort,
                                );

                                if let Err(e) = client.send_frame(&chat_frame).await {
                                    eprintln!("\n[错误] 发送消息失败: {}", e);
                                    println!("提示: 连接可能已断开");
                                    // 检查连接状态
                                    if !client.is_connected().await {
                                        println!("连接已断开，尝试重连...");
                                        // 客户端会自动重连（如果配置了重连）
                                        if let Err(e) = client.connect().await {
                                            eprintln!("重连失败: {}", e);
                                            break;
                                        } else {
                                            println!("✅ 重连成功");
                                        }
                                    }
                                }

                                print!("{}> ", username);
                                let _ = io::stdout().flush();
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("\n[错误] 读取输入失败: {}", e);
                        break;
                    }
                }
            }
        }
    }

    // 断开连接
    println!("\n正在断开连接...");
    if let Err(e) = client.disconnect().await {
        eprintln!("断开连接时出错: {}", e);
    } else {
        println!("✅ 已断开连接");
    }

    // 显示最终统计
    let final_msg_count = MESSAGE_COUNT.load(Ordering::Relaxed);
    let final_event_count = EVENT_COUNT.load(Ordering::Relaxed);
    println!();
    println!("📊 最终统计:");
    println!("   - 接收消息数: {}", final_msg_count);
    println!("   - 接收事件数: {}", final_event_count);
    println!("   - 发送测试消息数: {}", test_message_count);
    println!();
    println!("感谢使用！");

    Ok(())
}
