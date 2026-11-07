//! QUIC 聊天室客户端
//!
//! 使用基础结构（HybridClient）直接构建客户端
//! 实现一个简单的聊天室客户端，支持发送和接收消息
//!
//! 注意：此示例使用 QUIC 协议，需要 TLS 证书
//! 客户端会自动信任服务器证书（自签名证书）
//!
//! 此示例展示了如何：
//! 1. 实现 ConnectionObserver trait 来接收消息
//! 2. 使用 HybridClient::connect_with_race() 直接连接服务器（单协议即只有一个协议的竞速）
//! 3. 手动添加观察者和处理连接状态

use flare_core::client::HybridClient;
use flare_core::client::{Client, ClientConfig};
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::{
    Reliability, frame_with_message_command, generate_message_id, send_message,
};
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use std::io::{self, Write};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

// 消息观察者，用于接收和显示聊天消息
struct ChatObserver {
    username: String,
}

impl ConnectionObserver for ChatObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                // 解析接收到的消息（默认使用JSON，parse()会自动检测实际格式）
                if let Ok(frame) = flare_core::common::MessageParser::json().parse(data)
                    && let Some(cmd) = &frame.command
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
                    print!("{}> ", self.username);
                    let _ = io::stdout().flush();
                }
            }
            ConnectionEvent::Connected => {
                println!("\n[系统] 已连接到聊天室服务器！");
                print!("{}> ", self.username);
                let _ = io::stdout().flush();
            }
            ConnectionEvent::Disconnected(reason) => {
                println!("\n[系统] 连接已断开: {}", reason);
            }
            ConnectionEvent::Error(e) => {
                eprintln!("\n[错误] {:?}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 tracing（默认使用 debug 级别，方便调试）
    // 可以通过环境变量 RUST_LOG 覆盖：RUST_LOG=info cargo run --example quic_client
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")),
        )
        .init();

    println!("=== QUIC 聊天室客户端 ===");

    // 获取用户名
    print!("请输入您的用户名: ");
    io::stdout().flush()?;
    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();

    if username.is_empty() {
        println!("用户名不能为空，使用默认用户名: 匿名用户");
        return Err("用户名不能为空".into());
    }

    println!("正在连接到聊天室服务器...");

    // 使用 quic:// 协议
    // 不指定格式，将使用服务端默认JSON（客户端可以在协商时指定格式）
    let quic_config = ClientConfig::new("quic://127.0.0.1:8081".to_string()).quic();
    // 可选：指定序列化格式（如果不指定，使用服务端默认JSON）
    // .with_format(flare_core::common::protocol::SerializationFormat::Protobuf)

    match HybridClient::connect_with_race(quic_config).await {
        Ok(mut quic_client) => {
            println!("连接成功！");
            println!("使用的协议: {:?}", quic_client.active_protocol());

            // 创建并添加消息观察者
            let observer = Arc::new(ChatObserver {
                username: username.clone(),
            });
            quic_client.add_observer(Arc::clone(&observer) as Arc<dyn ConnectionObserver>);

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

            let _ = quic_client.send_frame(&init_frame).await;

            println!("\n欢迎来到聊天室！输入消息后按回车发送，输入 '/quit' 退出");
            print!("{}> ", username);
            io::stdout().flush()?;

            // 创建异步输入任务
            let stdin = tokio::io::stdin();
            let mut reader = BufReader::new(stdin);
            let mut line = String::new();

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

                                // 检查退出命令
                                if input == "/quit" || input == "/exit" {
                                    println!("退出聊天室...");
                                    break;
                                }

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

                                if let Err(e) = quic_client.send_frame(&chat_frame).await {
                                    eprintln!("\n[错误] 发送消息失败: {}", e);
                                    break;
                                }

                                print!("{}> ", username);
                                let _ = io::stdout().flush();
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
            let _ = quic_client.disconnect().await;
            println!("已断开连接");
        }
        Err(e) => {
            eprintln!("连接失败: {:?}", e);
        }
    }

    Ok(())
}
