//! Bài học 5: Chat Nhóm với Topic Discovery
//! 
//! Logic:
//! 1. Thử tìm anchor qua DNS/DHT (nhanh)
//! 2. Nếu không thấy → Trở thành anchor
//! 3. Relay tự động được dùng làm fallback
//!
//! Cách dùng:
//! - Terminal 1: cargo run --example 05_topic_discovery -- --topic market
//! - Terminal 2: cargo run --example 05_topic_discovery -- --topic market

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use iroh::{
    discovery::pkarr::dht::DhtDiscovery,
    protocol::Router,
    Endpoint, PublicKey, SecretKey,
};
use iroh_gossip::{
    api::{Event, GossipReceiver},
    net::{Gossip, GOSSIP_ALPN},
    proto::TopicId,
};
use std::time::Duration;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{info, error, warn};

#[derive(Parser)]
#[command(author, version, about = "Chat nhóm P2P - Topic Discovery")]
struct Cli {
    /// Tên topic (phòng chat) muốn tham gia
    #[arg(short, long, default_value = "general")]
    topic: String,
    
    /// Force trở thành anchor (node đầu tiên)
    #[arg(long)]
    anchor: bool,
}

/// Từ topic name, tạo ra một SecretKey cố định cho anchor
fn topic_to_secret_key(topic: &str) -> SecretKey {
    let hash = blake3::hash(format!("iroh-topic-anchor-v1:{}", topic).as_bytes());
    SecretKey::from_bytes(hash.as_bytes())
}

fn topic_to_public_key(topic: &str) -> PublicKey {
    topic_to_secret_key(topic).public()
}

#[tokio::main]
async fn main() -> Result<()> {
    // Giảm log noise - chỉ hiện INFO của app, ẩn warnings của mainline/iroh
    tracing_subscriber::fmt()
        .with_env_filter("error,05_topic_discovery=info")
        .init();

    let cli = Cli::parse();
    let topic_name = &cli.topic;

    info!("🔍 TOPIC DISCOVERY CHAT");
    info!("Topic: '{}'", topic_name);

    let topic_anchor_pk = topic_to_public_key(topic_name);
    info!("Topic Anchor: {}", &topic_anchor_pk.to_string()[..16]);

    // Quyết định role: anchor hay joiner
    let is_anchor = cli.anchor;  // Có thể force bằng --anchor flag
    
    // Tạo discovery (DHT + relay fallback tự động)
    let discovery = DhtDiscovery::builder()
        .n0_dns_pkarr_relay()
        .dht(true)
        .include_direct_addresses(true)
        .build()?;

    // Tạo endpoint
    let endpoint = if is_anchor {
        info!("📢 Bạn là ANCHOR (node đầu tiên)");
        Endpoint::builder()
            .secret_key(topic_to_secret_key(topic_name))
            .discovery(discovery)
            .bind()
            .await?
    } else {
        info!("� Bạn là JOINER (sẽ tìm và kết nối anchor)");
        Endpoint::builder()
            .discovery(discovery)
            .bind()
            .await?
    };

    let my_id = endpoint.secret_key().public();
    info!("ID: {}", &my_id.to_string()[..16]);

    // Online và publish địa chỉ
    info!("⏳ Đang kết nối...");
    endpoint.online().await;
    info!("✅ Đã online!");

    // Khởi tạo Gossip
    let gossip = Gossip::builder().spawn(endpoint.clone());

    // Đăng ký vào Router
    let router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    // Tạo Topic ID
    let topic_id = TopicId::from_bytes(blake3::hash(topic_name.as_bytes()).into());

    // Subscribe
    info!("📡 Đang tham gia topic...");
    
    let gossip_topic = if is_anchor {
        // Anchor: chờ người khác đến
        gossip.subscribe(topic_id, vec![]).await?
    } else {
        // Joiner: thử kết nối đến anchor với timeout
        match tokio::time::timeout(
            Duration::from_secs(10),
            gossip.subscribe_and_join(topic_id, vec![topic_anchor_pk])
        ).await {
            Ok(Ok(topic)) => {
                info!("✅ Đã kết nối với anchor!");
                topic
            }
            _ => {
                warn!("⚠️ Không tìm thấy anchor. Đang subscribe bình thường...");
                gossip.subscribe(topic_id, vec![]).await?
            }
        }
    };

    let (sender, receiver) = gossip_topic.split();
    
    info!("💬 Sẵn sàng chat! Gõ tin nhắn và nhấn Enter.");
    if is_anchor {
        info!("ℹ️  Bạn là anchor. Người khác chạy KHÔNG có --anchor để kết nối.");
    } else {
        info!("ℹ️  Nếu không có anchor, hãy chạy lại với --anchor");
    }

    // Task gửi tin
    let mut sender_clone = sender.clone();
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut framed_stdin = Framed::new(stdin, LinesCodec::new());

        while let Some(Ok(line)) = framed_stdin.next().await {
            if line.trim().is_empty() {
                continue;
            }
            let msg = bytes::Bytes::from(line);
            if let Err(e) = sender_clone.broadcast(msg).await {
                error!("Lỗi gửi: {}", e);
            }
        }
    });

    // Nhận tin
    let mut receiver: GossipReceiver = receiver;
    while let Some(event_result) = receiver.next().await {
        match event_result {
            Ok(Event::Received(msg)) => {
                let text = String::from_utf8_lossy(&msg.content);
                let from = &msg.delivered_from.to_string()[..8];
                println!("\x1b[36m[{}..]: {}\x1b[0m", from, text.trim());
            }
            Ok(Event::NeighborUp(peer)) => {
                info!("👋 {} đã tham gia!", &peer.to_string()[..8]);
            }
            Ok(Event::NeighborDown(peer)) => {
                info!("💨 {} đã rời đi!", &peer.to_string()[..8]);
            }
            Ok(Event::Lagged) => {
                warn!("⚠️ Lag");
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
