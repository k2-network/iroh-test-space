//! Bài 09: Tracker-based Chat UI (Professional CLI)
//!
//! Cải tiến từ Bài 08:
//! - Hardcoded Tracker ID
//! - Professional UI (Clean, No Logs)
//! - Username support

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use iroh::{
    discovery::pkarr::dht::DhtDiscovery,
    protocol::Router,
    Endpoint, EndpointId, SecretKey,
};
use iroh_blobs::HashAndFormat;
use iroh_content_discovery::{
    announce, query,
    protocol::{AbsoluteTime, Announce, AnnounceKind, Query, QueryFlags, SignedAnnounce, ALPN},
};
use iroh_gossip::{
    net::{Gossip, GOSSIP_ALPN},
    proto::TopicId,
};
use std::time::Duration;
use tokio_util::codec::{Framed, LinesCodec};
use serde::{Serialize, Deserialize};

// --- CONSTANTS ---
const DEFAULT_TRACKER: &str = "71853750efc1219d7976639087c5fb25cf8d4b49f6d509366f2e094a3f781623";

#[derive(Parser)]
struct Cli {
    /// Topic name to join
    #[arg(short, long, default_value = "lobby")]
    topic: String,

    /// Username to display
    #[arg(short, long, default_value = "Anonymous")]
    name: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ChatMessage {
    sender: String,
    text: String,
}

/// Convert topic name to TopicId
fn topic_to_id(topic: &str) -> TopicId {
    TopicId::from_bytes(blake3::hash(topic.as_bytes()).into())
}

/// Convert topic name to HashAndFormat for tracker
fn topic_to_hash(topic: &str) -> HashAndFormat {
    let topic_id = topic_to_id(topic);
    let hash = iroh_blobs::Hash::from_bytes(topic_id.as_bytes().clone());
    HashAndFormat::raw(hash)
}

/// Parse tracker NodeId from hex string
fn parse_tracker_id(s: &str) -> Result<EndpointId> {
    let bytes = hex::decode(s)?;
    let arr: [u8; 32] = bytes.try_into().map_err(|_| anyhow::anyhow!("Invalid tracker ID length"))?;
    Ok(EndpointId::from_bytes(&arr)?)
}

// --- COLOR MACROS (Simple ANSI) ---
macro_rules! system {
    ($($arg:tt)*) => { println!("\x1b[90m[SYS] {}\x1b[0m", format!($($arg)*)); }
}
macro_rules! error {
    ($($arg:tt)*) => { println!("\x1b[31m[ERR] {}\x1b[0m", format!($($arg)*)); }
}
macro_rules! chat {
    ($name:expr, $msg:expr) => { println!("\x1b[36m[{}]\x1b[0m: {}", $name, $msg); }
}
macro_rules! me {
    ($name:expr, $msg:expr) => { println!("\x1b[32m[{}]\x1b[0m: {}", $name, $msg); }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Disable logging to keep UI clean
    tracing_subscriber::fmt()
        .with_env_filter("error") 
        .init();

    let cli = Cli::parse();
    let topic_name = &cli.topic;
    let username = &cli.name;
    let topic_id = topic_to_id(topic_name);
    let topic_hash = topic_to_hash(topic_name);
    let tracker_id = parse_tracker_id(DEFAULT_TRACKER)?;

    println!("");
    println!("\x1b[1m  K2-NETWORK CHAT v1.0\x1b[0m");
    println!("  --------------------");
    println!("  Topic:    {}", topic_name);
    println!("  User:     {}", username);
    println!("  Tracker:  {}...", &DEFAULT_TRACKER[..8]);
    println!("  --------------------");
    
    system!("Initializing node...");

    // Step 1: Create endpoint
    let secret_key = SecretKey::generate(&mut rand::rng());
    let my_id = secret_key.public();

    let discovery = DhtDiscovery::builder()
        .n0_dns_pkarr_relay()
        .dht(true)
        .include_direct_addresses(true)
        .secret_key(secret_key.clone())
        .build()?;

    let endpoint = Endpoint::builder()
        .secret_key(secret_key.clone())
        .discovery(discovery)
        .alpns(vec![GOSSIP_ALPN.to_vec(), ALPN.to_vec()])
        .bind()
        .await?;

    // endpoint.online().await; // Wait for relay connection (optional but good for stability)
    system!("Node ready. ID: {}...", &my_id.to_string()[..8]);

    // Step 2: Query tracker
    system!("Connecting to tracker...");
    
    let query_args = Query {
        content: topic_hash,
        flags: QueryFlags {
            complete: false,
            verified: false,
        },
    };

    let mut peer_node_ids = vec![];
    
    match query(&endpoint, tracker_id, query_args).await {
        Ok(announces) => {
            for announce in announces {
                let peer_id = announce.host;
                if peer_id != EndpointId::from(my_id) {
                    peer_node_ids.push(peer_id);
                }
            }
            system!("Found {} peers in topic.", peer_node_ids.len());
        }
        Err(e) => {
            error!("Tracker unreachable: {}. Continuing offline...", e);
        }
    }

    // Step 3: Announce self
    system!("Announcing presence...");
    let announce_data = Announce {
        host: EndpointId::from(my_id),
        content: topic_hash,
        kind: AnnounceKind::Complete,
        timestamp: AbsoluteTime::now(),
    };
    
    let signed_announce = SignedAnnounce::new(announce_data, &secret_key)?;
    
    // Announce best-effort
    if let Err(_) = announce(&endpoint, tracker_id, signed_announce.clone()).await {
        // Silent fail
    }

    // Step 4: Gossip
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    system!("Joining channel (connecting to {} peers)...", peer_node_ids.len());

    let gossip_topic = if peer_node_ids.is_empty() {
        gossip.subscribe(topic_id, vec![]).await?
    } else {
        // Try to join with timeout. If peers are dead (stale tracker data), don't hang for too long.
        match tokio::time::timeout(
            Duration::from_secs(3), 
            gossip.subscribe_and_join(topic_id, peer_node_ids)
        ).await {
            Ok(res) => res?,
            Err(_) => {
                // Timeout -> Just subscribe alone
                // error!("Connection to peers timed out. Entering offline mode...");
                gossip.subscribe(topic_id, vec![]).await?
            }
        }
    };

    system!("Ready.");
    println!("\x1b[90m  (Type message and press Enter)\x1b[0m\n");

    // Re-announce task
    let endpoint_clone = endpoint.clone();
    let secret_key_clone = secret_key.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let ad = Announce {
                host: EndpointId::from(my_id),
                content: topic_hash,
                kind: AnnounceKind::Complete,
                timestamp: AbsoluteTime::now(),
            };
            if let Ok(sa) = SignedAnnounce::new(ad, &secret_key_clone) {
                let _ = announce(&endpoint_clone, tracker_id, sa).await;
            }
        }
    });

    let (sender, mut receiver) = gossip_topic.split();

    // Input Task
    let sender_clone = sender.clone();
    let my_username = username.clone();
    
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut framed = Framed::new(stdin, LinesCodec::new());

        // Simple prompt? No advanced readline library to avoid dependencies issues for now.
        // Just print >
        
        while let Some(Ok(line)) = framed.next().await {
            if line.trim().is_empty() { continue; }
            
            // Move cursor up 1 line and clear it to remove raw input
            // (Only works on terminals supporting ANSI)
            print!("\x1b[1A\x1b[2K"); 
            me!(my_username, line.trim());

            let msg = ChatMessage {
                sender: my_username.clone(),
                text: line.trim().to_string(),
            };
            let bytes = postcard::to_stdvec(&msg).unwrap(); // Should not fail
            
            if let Err(_) = sender_clone.broadcast(bytes.into()).await {
                error!("Failed to send message");
            }
        }
    });

    // Receive Loop
    while let Some(event) = receiver.next().await {
        use iroh_gossip::api::Event;
        match event {
            Ok(Event::Received(msg)) => {
                // Try deserialize ChatMessage
                match postcard::from_bytes::<ChatMessage>(&msg.content) {
                    Ok(chat_msg) => {
                        chat!(chat_msg.sender, chat_msg.text);
                    }
                    Err(_) => {
                        // Fallback for raw strings (from example 08 nodes)
                        let text = String::from_utf8_lossy(&msg.content);
                        let from_short = &msg.delivered_from.to_string()[..8];
                        chat!(from_short, text.trim());
                    }
                }
            }
            Ok(Event::NeighborUp(_)) => {
                // system!("Peer connected."); // Too noisy for large groups?
            }
            Ok(Event::NeighborDown(_)) => {
                // system!("Peer disconnected.");
            }
            _ => {}
        }
    }

    router.shutdown().await?;
    Ok(())
}
