//! TCP 聊天室客户端
//!
//! 使用 `HybridClient` + 原始 TCP（length-prefixed Frame，与 QUIC bi-stream 同帧格式）。
//!
//! ## 启动前
//!
//! ```bash
//! # 终端 1：服务端（需 --features tcp）
//! cargo run --example flare_chat_server --features tcp
//!
//! # 终端 2：客户端（需 --features tcp）
//! RUST_LOG=info cargo run --example tcp_client --features tcp
//! ```

use flare_core::client::HybridClient;
use flare_core::client::{Client, ClientConfig};
use flare_core::common::MessageParser;
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::encryption::{Aes256GcmEncryptor, EncryptionAlgorithm, EncryptionUtil};
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::{
    Reliability, SerializationFormat, frame_with_message_command, generate_message_id, send_message,
};
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::warn;

struct ChatObserver {
    username: String,
    parser: MessageParser,
}

impl ChatObserver {
    fn new(username: String) -> Self {
        Self {
            username,
            parser: negotiated_chat_parser(),
        }
    }
}

impl ConnectionObserver for ChatObserver {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Message(data) => {
                if let Ok(frame) = self.parser.parse(data)
                    && let Some(cmd) = &frame.command
                    && let Some(Type::Payload(msg_cmd)) = &cmd.r#type
                {
                    let message = match String::from_utf8(msg_cmd.payload.clone()) {
                        Ok(text) => text,
                        Err(_) => format!("<binary_data: {} bytes>", msg_cmd.payload.len()),
                    };

                    if let Some(type_bytes) = msg_cmd.metadata.get("type") {
                        let msg_type = String::from_utf8(type_bytes.clone())
                            .unwrap_or_else(|_| "unknown".to_string());
                        if msg_type == "join" || msg_type == "leave" {
                            println!("\n[系统] {message}");
                        } else {
                            println!("\n{message}");
                        }
                    } else {
                        println!("\n{message}");
                    }

                    print!("{}> ", self.username);
                    let _ = io::stdout().flush();
                }
            }
            ConnectionEvent::Connected => {
                println!("\n[系统] 已通过 TCP 连接到聊天室服务器！");
                print!("{}> ", self.username);
                let _ = io::stdout().flush();
            }
            ConnectionEvent::Disconnected(reason) => {
                println!("\n[系统] 连接已断开: {reason}");
            }
            ConnectionEvent::Error(e) => {
                eprintln!("\n[错误] {e:?}");
            }
        }
    }
}

fn negotiated_chat_parser() -> MessageParser {
    MessageParser::new(
        SerializationFormat::Protobuf,
        CompressionAlgorithm::Gzip,
        EncryptionAlgorithm::Aes256Gcm,
    )
}

fn register_demo_encryptor() -> Result<(), Box<dyn std::error::Error>> {
    let encryption_key = if let Ok(key) = std::env::var("ENCRYPTION_KEY") {
        key.as_bytes().to_vec()
    } else {
        warn!("⚠️  使用默认示例密钥（需与 flare_chat_server 一致）");
        b"01234567890123456789012345678901".to_vec()
    };

    if encryption_key.len() != 32 {
        return Err(format!(
            "ENCRYPTION_KEY 必须为 32 字节，当前 {} 字节",
            encryption_key.len()
        )
        .into());
    }

    let encryptor = Aes256GcmEncryptor::new(&encryption_key)?;
    EncryptionUtil::register_custom(Arc::new(encryptor));
    Ok(())
}

async fn wait_until_ready(client: &HybridClient, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    while !client.is_connected() {
        if Instant::now() >= deadline {
            return Err("协商超时：请确认服务端已启动且 ENCRYPTION_KEY 一致".to_string());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    register_demo_encryptor()?;

    println!("=== TCP 聊天室客户端 ===");

    let server_url =
        std::env::var("TCP_SERVER_URL").unwrap_or_else(|_| "tcp://127.0.0.1:8090".to_string());

    print!("请输入您的用户名: ");
    io::stdout().flush()?;
    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();
    if username.is_empty() {
        return Err("用户名不能为空".into());
    }

    println!("正在通过 TCP 连接 {server_url} ...");

    let config = ClientConfig::new(server_url)
        .tcp()
        .with_user_id(username.clone());

    match HybridClient::connect_with_race(config).await {
        Ok(mut client) => {
            println!("TCP 链路已建立，等待 CONNECT 协商...");
            wait_until_ready(&client, Duration::from_secs(10)).await?;
            println!("连接成功！协议: {:?}", client.active_protocol());

            let observer = Arc::new(ChatObserver::new(username.clone()));
            client.add_observer(observer as Arc<dyn ConnectionObserver>);

            let mut metadata = std::collections::HashMap::new();
            metadata.insert("username".to_string(), username.as_bytes().to_vec());
            metadata.insert("type".to_string(), b"init".to_vec());

            let init_frame = frame_with_message_command(
                send_message(
                    generate_message_id(),
                    format!("{username} 已加入聊天室").into_bytes(),
                    Some(metadata),
                    None,
                ),
                Reliability::BestEffort,
            );
            if let Err(e) = client.send_frame(&init_frame).await {
                eprintln!("[错误] 发送加入消息失败: {e}");
            }

            println!("\n输入消息后回车发送，/quit 退出");
            print!("{username}> ");
            io::stdout().flush()?;

            let mut reader = BufReader::new(tokio::io::stdin());
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        let input = line.trim().to_string();
                        if input.is_empty() {
                            print!("{username}> ");
                            let _ = io::stdout().flush();
                            continue;
                        }
                        if input == "/quit" || input == "/exit" {
                            break;
                        }

                        let mut msg_metadata = std::collections::HashMap::new();
                        msg_metadata.insert("username".to_string(), username.as_bytes().to_vec());

                        let frame = frame_with_message_command(
                            send_message(
                                generate_message_id(),
                                input.into_bytes(),
                                Some(msg_metadata),
                                None,
                            ),
                            Reliability::BestEffort,
                        );

                        if let Err(e) = client.send_frame(&frame).await {
                            eprintln!("\n[错误] 发送失败: {e}");
                            break;
                        }
                        print!("{username}> ");
                        let _ = io::stdout().flush();
                    }
                    Err(e) => {
                        eprintln!("\n[错误] 读取输入失败: {e}");
                        break;
                    }
                }
            }

            let _ = client.disconnect().await;
            println!("已断开连接");
        }
        Err(e) => eprintln!("连接失败: {e:?}"),
    }

    Ok(())
}
