//! Bài học 06: Self-Healing Gossip Network
//! 
//! Kiến trúc:
//! - 1 topic chung (ví dụ: "market")
//! - Anchor node dùng topic_key, ping mỗi 10s
//! - Nếu không thấy ping trong 10s, node có NodeId nhỏ nhất thử kế vị
//! 
//! Demo trên 1 máy:
//! Terminal 1: cargo run --example 06_dht_discovery -- --topic market
//! Terminal 2: cargo run --example 06_dht_discovery -- --topic market
//! Terminal 3: cargo run --example 06_dht_discovery -- --topic market
//! 
//! Thử: Tắt Terminal 1 (anchor) → Xem node khác kế vị!

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use iroh::{
    discovery::pkarr::dht::DhtDiscovery,
    protocol::Router,
    Endpoint, PublicKey, SecretKey,
};
use iroh_gossip::{
    api::{Event, GossipReceiver, GossipSender},
    net::{Gossip, GOSSIP_ALPN},
    proto::TopicId,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{info, warn};

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "market")]
    topic: String,
}

/// Message types trong gossip
#[derive(Clone, Debug)]
enum GossipMessage {
    AnchorPing,                    // Anchor còn sống
    Chat(String),                  // Chat bình thường
    AnchorAnnounce(PublicKey),     // Thông báo anchor mới
}

impl GossipMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            GossipMessage::AnchorPing => b"PING".to_vec(),
            GossipMessage::Chat(msg) => format!("CHAT:{}", msg).into_bytes(),
            GossipMessage::AnchorAnnounce(pk) => format!("ANCHOR:{}", pk).into_bytes(),
        }
    }
    
    fn decode(data: &[u8]) -> Option<Self> {
        let s = String::from_utf8_lossy(data);
        if s == "PING" {
            Some(GossipMessage::AnchorPing)
        } else if let Some(msg) = s.strip_prefix("CHAT:") {
            Some(GossipMessage::Chat(msg.to_string()))
        } else if let Some(pk_str) = s.strip_prefix("ANCHOR:") {
            pk_str.parse().ok().map(GossipMessage::AnchorAnnounce)
        } else {
            None
        }
    }
}

/// Tạo TopicId từ tên
fn topic_to_id(topic: &str) -> TopicId {
    TopicId::from_bytes(blake3::hash(topic.as_bytes()).into())
}

/// Tạo Anchor SecretKey từ topic (deterministic)
fn topic_to_anchor_key(topic: &str) -> SecretKey {
    let hash = blake3::hash(format!("anchor:{}", topic).as_bytes());
    SecretKey::from_bytes(hash.as_bytes())
}

/// State của node
struct NodeState {
    is_anchor: bool,
    last_anchor_ping: Instant,
    known_peers: HashSet<PublicKey>,
    anchor_id: PublicKey,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("error,06_dht_discovery=info")
        .init();

    let cli = Cli::parse();
    let topic_name = &cli.topic;
    let topic_id = topic_to_id(topic_name);
    let anchor_key = topic_to_anchor_key(topic_name);
    let anchor_id = anchor_key.public();

    info!("SELF-HEALING GOSSIP NETWORK");
    info!("Topic: '{}' | Anchor ID: {}", topic_name, &anchor_id.to_string()[..16]);

    // Discovery
    let discovery = DhtDiscovery::builder()
        .n0_dns_pkarr_relay()
        .dht(true)
        .include_direct_addresses(true)
        .build()?;

    // Thử connect anchor trước
    let temp_key = SecretKey::generate(&mut rand::rng());
    let temp_endpoint = Endpoint::builder()
        .secret_key(temp_key.clone())
        .discovery(discovery.clone())
        .bind()
        .await?;
    
    temp_endpoint.online().await;
    
    let i_am_anchor = match tokio::time::timeout(
        Duration::from_secs(5),
        temp_endpoint.connect(anchor_id, GOSSIP_ALPN)
    ).await {
        Ok(Ok(_)) => {
            info!("[OK] Anchor online, I am member");
            false
        }
        _ => {
            info!("[!] Anchor offline, I WILL BE ANCHOR!");
            true
        }
    };
    
    // Đóng temp endpoint
    drop(temp_endpoint);
    
    // Tạo endpoint thật
    let my_key = if i_am_anchor { anchor_key.clone() } else { temp_key };
    let my_id = my_key.public();
    
    let discovery2 = DhtDiscovery::builder()
        .n0_dns_pkarr_relay()
        .dht(true)
        .include_direct_addresses(true)
        .build()?;
        
    let endpoint = Endpoint::builder()
        .secret_key(my_key)
        .discovery(discovery2)
        .bind()
        .await?;
    
    endpoint.online().await;
    info!("My ID: {} | Is Anchor: {}", &my_id.to_string()[..16], i_am_anchor);

    // Gossip + Router
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    // Join gossip
    let bootstrap = if i_am_anchor { vec![] } else { vec![anchor_id] };
    
    let gossip_topic = if i_am_anchor {
        gossip.subscribe(topic_id, vec![]).await?
    } else {
        match tokio::time::timeout(
            Duration::from_secs(10),
            gossip.subscribe_and_join(topic_id, bootstrap)
        ).await {
            Ok(Ok(t)) => {
                info!("[OK] Joined gossip!");
                t
            }
            _ => {
                warn!("[!] Cannot join, subscribing normally");
                gossip.subscribe(topic_id, vec![]).await?
            }
        }
    };

    let (sender, receiver) = gossip_topic.split();
    
    // State
    let state = Arc::new(RwLock::new(NodeState {
        is_anchor: i_am_anchor,
        last_anchor_ping: Instant::now(),
        known_peers: HashSet::new(),
        anchor_id,
    }));

    info!("Ready! Type message and press Enter.");
    if i_am_anchor {
        info!("I am ANCHOR - ping every 10s");
    }

    // Task: Anchor ping mỗi 10s
    let sender_ping = sender.clone();
    let state_ping = state.clone();
    tokio::spawn(async move {
        let mut sender = sender_ping;
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            
            let s = state_ping.read().await;
            if s.is_anchor {
                drop(s);
                let msg = GossipMessage::AnchorPing;
                let _ = sender.broadcast(bytes::Bytes::from(msg.encode())).await;
                info!("PING sent");
            }
        }
    });

    // Task: Check anchor health + kế vị
    let state_check = state.clone();
    let my_id_check = my_id;
    let anchor_key_for_takeover = anchor_key.clone();
    let endpoint_for_check = endpoint.clone();
    let gossip_for_takeover = gossip.clone();
    let topic_id_for_takeover = topic_id;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            let s = state_check.read().await;
            if s.is_anchor {
                continue; // Tôi là anchor, không cần check
            }
            
            let elapsed = s.last_anchor_ping.elapsed();
            let peers: Vec<_> = s.known_peers.iter().cloned().collect();
            drop(s);
            
            if elapsed > Duration::from_secs(15) {
                // Anchor chết quá 15s
                // Kiểm tra tôi có NodeId nhỏ nhất không
                let i_am_smallest = peers.iter().all(|p| my_id_check < *p);
                
                if i_am_smallest || peers.is_empty() {
                    info!("[!] Anchor offline 15s, I have smallest ID -> TRY TAKEOVER!");
                    
                    // Thử connect đến anchor (xem có ai khác kế vị chưa)
                    match tokio::time::timeout(
                        Duration::from_secs(3),
                        endpoint_for_check.connect(anchor_key_for_takeover.public(), GOSSIP_ALPN)
                    ).await {
                        Ok(Ok(_)) => {
                            info!("[OK] New anchor found!");
                            let mut s = state_check.write().await;
                            s.last_anchor_ping = Instant::now();
                        }
                        _ => {
                            info!("[!] Still no anchor... (Need restart with anchor key to takeover)");
                            // Trong thực tế, cần restart process với anchor_key
                            // Hoặc implement hot-swap identity (phức tạp hơn)
                        }
                    }
                }
            }
        }
    });

    // Task: Stdin input
    let sender_chat = sender.clone();
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut framed = Framed::new(stdin, LinesCodec::new());
        let mut sender = sender_chat;
        
        while let Some(Ok(line)) = framed.next().await {
            if line.trim().is_empty() { continue; }
            let msg = GossipMessage::Chat(line);
            let _ = sender.broadcast(bytes::Bytes::from(msg.encode())).await;
        }
    });

    // Receive loop
    let mut receiver: GossipReceiver = receiver;
    let state_recv = state.clone();
    
    while let Some(event) = receiver.next().await {
        match event {
            Ok(Event::Received(msg)) => {
                let from = &msg.delivered_from.to_string()[..8];
                
                if let Some(gm) = GossipMessage::decode(&msg.content) {
                    match gm {
                        GossipMessage::AnchorPing => {
                            let mut s = state_recv.write().await;
                            s.last_anchor_ping = Instant::now();
                            info!("PING from anchor");
                        }
                        GossipMessage::Chat(text) => {
                            println!("\x1b[36m[{}]: {}\x1b[0m", from, text);
                        }
                        GossipMessage::AnchorAnnounce(new_anchor) => {
                            let mut s = state_recv.write().await;
                            s.anchor_id = new_anchor;
                            s.last_anchor_ping = Instant::now();
                            info!("New anchor: {}", &new_anchor.to_string()[..16]);
                        }
                    }
                }
            }
            Ok(Event::NeighborUp(peer)) => {
                info!("[+] Peer joined: {}", &peer.to_string()[..8]);
                let mut s = state_recv.write().await;
                s.known_peers.insert(peer);
            }
            Ok(Event::NeighborDown(peer)) => {
                info!("[-] Peer left: {}", &peer.to_string()[..8]);
                let mut s = state_recv.write().await;
                s.known_peers.remove(&peer);
            }
            Ok(Event::Lagged) => {
                warn!("[!] Lagged");
            }
            Err(e) => {
                warn!("Error: {}", e);
                break;
            }
        }
    }

    router.shutdown().await?;
    Ok(())
}
