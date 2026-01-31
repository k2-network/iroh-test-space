//! Bài học 4: Chat Nhóm (Gossip) với Ticket
//! 
//! Mục tiêu: Nhiều người cùng tham gia một Topic và chat với nhau (Broadcast).
//! 
//! Cách dùng:
//! - Terminal 1: cargo run --example 04_gossip_chat -- open
//!   (Copy ticket và gửi cho bạn bè)
//! - Terminal 2+: cargo run --example 04_gossip_chat -- join <TICKET>

use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use iroh::{discovery::static_provider::StaticProvider, protocol::Router, Endpoint, RelayMode};
use iroh_gossip::{
    api::Event,
    net::{Gossip, GOSSIP_ALPN},
    proto::TopicId,
};
use serde::{Deserialize, Serialize};

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
    /// Mở phòng chat mới và in ra ticket
    Open,
    /// Tham gia phòng chat bằng ticket
    Join {
        /// Ticket nhận được từ người mở phòng
        ticket: String,
    },
}

/// Ticket chứa thông tin phòng chat + địa chỉ peers
#[derive(Debug, Serialize, Deserialize)]
struct Ticket {
    topic: TopicId,
    peers: Vec<iroh::EndpointAddr>,
}

impl Ticket {
    fn to_string(&self) -> String {
        let bytes = postcard::to_stdvec(self).expect("serialize ticket");
        data_encoding::BASE32_NOPAD.encode(&bytes).to_lowercase()
    }
    
    fn from_str(s: &str) -> Result<Self> {
        let bytes = data_encoding::BASE32_NOPAD.decode(s.to_uppercase().as_bytes())?;
        Ok(postcard::from_bytes(&bytes)?)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Cấu hình log
    tracing_subscriber::fmt()
        .with_env_filter("error,04_gossip_chat=info")
        .init();

    let cli = Cli::parse();

    // 1. Tạo static provider cho discovery
    let static_provider = StaticProvider::new();

    // 2. Tạo Iroh Endpoint
    let endpoint = Endpoint::builder()
        .discovery(static_provider.clone())
        .relay_mode(RelayMode::Default) // Dùng relay mặc định của n0
        .bind()
        .await?;

    let my_id = endpoint.secret_key().public();
    info!("GOSSIP CHAT SẴN SÀNG!");
    info!("ID của bạn: {}", my_id);

    // Chờ kết nối relay
    endpoint.online().await;
    info!("Đã kết nối relay!");

    // 3. Khởi tạo Gossip
    let gossip = Gossip::builder().spawn(endpoint.clone());

    // 4. Đăng ký vào Router
    let router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    // 5. Xử lý lệnh
    let (topic, peers) = match &cli.command {
        Commands::Open => {
            // Tạo topic mới
            let topic = TopicId::from_bytes(rand::random());
            info!("Đã mở phòng chat mới!");
            
            // Tạo ticket 
            let my_addr = endpoint.addr();
            let ticket = Ticket {
                topic,
                peers: vec![my_addr],
            };
            
            println!("\n========================================");
            println!("TICKET (Gửi cho bạn bè để họ join):");
            println!("{}", ticket.to_string());
            println!("========================================\n");
            
            (topic, vec![])
        }
        Commands::Join { ticket } => {
            let ticket = Ticket::from_str(ticket)?;
            info!("Đang tham gia phòng: {}", ticket.topic);
            
            // Thêm peer addresses vào static provider
            for peer in &ticket.peers {
                static_provider.add_endpoint_info(peer.clone());
            }
            
            (ticket.topic, ticket.peers.iter().map(|p| p.id).collect())
        }
    };

    // 6. Subscribe và join
    let gossip_topic = if peers.is_empty() {
        info!("Đang chờ người khác tham gia...");
        gossip.subscribe(topic, vec![]).await?
    } else {
        info!("Đang kết nối tới {} peers...", peers.len());
        gossip.subscribe_and_join(topic, peers).await?
    };
    
    let (mut sender, mut receiver) = gossip_topic.split();
    info!("Đã vào phòng! Hãy bắt đầu chat...");

    // Task đọc bàn phím và gửi tin
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut framed_stdin = Framed::new(stdin, LinesCodec::new());

        while let Some(Ok(line)) = framed_stdin.next().await {
            let msg = bytes::Bytes::from(line);
            if let Err(e) = sender.broadcast(msg).await {
                error!("Lỗi gửi tin: {}", e);
            }
        }
    });

    // Vòng lặp nhận tin nhắn
    while let Some(event_result) = receiver.next().await {
        match event_result {
            Ok(Event::Received(msg)) => {
                let text = String::from_utf8_lossy(&msg.content);
                let sender_short = &msg.delivered_from.to_string()[..8];
                println!("\x1b[36m[{}..]: {}\x1b[0m", sender_short, text.trim());
            }
            Ok(Event::NeighborUp(peer)) => {
                info!("👋 {} đã tham gia!", &peer.to_string()[..8]);
            }
            Ok(Event::NeighborDown(peer)) => {
                info!("💨 {} đã rời phòng!", &peer.to_string()[..8]);
            }
            Ok(Event::Lagged) => {
                // Bị lag, bỏ qua một số tin nhắn
                info!("⚠️ Kết nối bị lag, có thể mất một số tin nhắn");
            }
            Err(e) => {
                error!("Lỗi: {}", e);
                break;
            }
        }
    }

    router.shutdown().await?;
    Ok(())
}
