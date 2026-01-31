//! Bài học 3: Chat P2P qua Terminal
//! 
//! Mục tiêu: Hai người kết nối và chat trực tiếp với nhau (Gửi nhận tin nhắn 2 chiều).

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use iroh::{discovery::pkarr::dht::DhtDiscovery, Endpoint, EndpointId, SecretKey};
use tokio::io::AsyncWriteExt; // Chỉ cần AsyncWriteExt cho write_all
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{info, error};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Chế độ lắng nghe (Chờ bạn chat)
    Listen,
    /// Chế độ kết nối (Gọi cho bạn)
    Connect {
        /// Node ID của bạn chat (hex format)
        node_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Cấu hình log gọn gàng
    tracing_subscriber::fmt()
        .with_env_filter("error,code=info,03_p2p_chat=info")
        .init();

    let cli = Cli::parse();

    // 1. Tạo Node (Có ALPN riêng cho chat)
    // Dùng ALPN "k2-chat" để không lẫn với tutorial khác
    let alpn = b"k2-chat";
    
    // iroh 0.95: Tạo secret key trước
    let secret_key = SecretKey::generate(&mut rand::rng());
    let my_id = secret_key.public();

    let dht_discovery = DhtDiscovery::builder()
        .n0_dns_pkarr_relay()
        .dht(true)
        .include_direct_addresses(true)
        .secret_key(secret_key.clone());

    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .discovery(dht_discovery)
        .alpns(vec![alpn.to_vec()])
        .bind()
        .await?;

    info!("CHAT TERMINAL SẴN SÀNG!");
    info!("ID của bạn: {}", my_id);

    match cli.command {
        Commands::Listen => {
            info!("Đang chờ cuộc gọi...");
            info!("Gửi ID của bạn cho bạn bè để họ connect.");

            while let Some(incoming) = endpoint.accept().await {
                let connecting = match incoming.accept() {
                    Ok(c) => c,
                    Err(e) => { error!("Lỗi accept: {}", e); continue; }
                };

                let connection = match connecting.await {
                    Ok(c) => c,
                    Err(e) => { error!("Lỗi handshake: {}", e); continue; }
                };

                // iroh 0.95: dùng remote_id() thay vì remote_node_id()
                let remote_id_str = connection.remote_id().to_string();
                info!("Có người gọi đến từ: {}", remote_id_str);

                match connection.accept_bi().await {
                    Ok((send_stream, recv_stream)) => {
                        info!("🚀 Đã vào phòng chat! Hãy gõ gì đó...");
                        if let Err(e) = handle_chat(send_stream, recv_stream).await {
                             error!("Lỗi trong phiên chat: {}", e);
                        }
                    }
                    Err(e) => error!("Lỗi mở stream chat: {}", e),
                }
            }
        }
        Commands::Connect { node_id } => {
            // iroh 0.95: Parse EndpointId từ hex string
            let bytes = hex::decode(&node_id)?;
            let arr: [u8; 32] = bytes.try_into().map_err(|_| anyhow::anyhow!("Invalid Node ID length"))?;
            let target_node_id = EndpointId::from_bytes(&arr)?;
            info!("Đang kết nối tới {}...", target_node_id);

            let connection = endpoint.connect(target_node_id, alpn).await?;
            
            info!("Đã kết nối! Đang mở kênh chat...");

            let (send_stream, recv_stream) = connection.open_bi().await?;
            
            info!("Đã vào phòng chat! Hãy gõ gì đó...");
            handle_chat(send_stream, recv_stream).await?;
        }
    }

    Ok(())
}

async fn handle_chat(
    mut send_stream: iroh::endpoint::SendStream,
    mut recv_stream: iroh::endpoint::RecvStream
) -> Result<()> {
    // Kênh đọc từ bàn phím (Stdin)
    let stdin = tokio::io::stdin();
    let mut framed_stdin = Framed::new(stdin, LinesCodec::new());

    // Kênh nhận tin nhắn (RecvStream check từng byte và decode thành dòng)
    // Iroh stream là raw bytes, ta dùng buffer để đọc
    let mut buf = vec![0u8; 1024];

    loop {
        tokio::select! {
            // 1. Có tin nhắn đến từ mạng -> In ra
            read_result = recv_stream.read(&mut buf) => {
                match read_result {
                    Ok(Some(n)) => {
                         let msg = String::from_utf8_lossy(&buf[..n]);
                         println!("\x1b[32m[Bạn bè]: {}\x1b[0m", msg.trim());
                    }
                    Ok(None) => {
                        info!("👋 Bạn chat đã thoát.");
                        break;
                    },
                    Err(e) => {
                        error!("Lỗi nhận tin: {}", e);
                        break;
                    }
                }
            }

            // 2. Người dùng gõ phím -> Gửi đi
            input = framed_stdin.next() => {
                match input {
                    Some(Ok(line)) => {
                        let msg_bytes = format!("{}\n", line).into_bytes();
                        if let Err(e) = send_stream.write_all(&msg_bytes).await {
                             error!("Lỗi gửi tin: {}", e);
                             break;
                        }
                    }
                    Some(Err(e)) => error!("Lỗi đọc phím: {}", e),
                    None => break, // CTRL+D
                }
            }
        }
    }

    let _ = send_stream.finish(); 
    Ok(())
}
