//! Bài học 08: Tracker-based Topic Discovery
//!
//! Sử dụng iroh-content-tracker để join topic mà KHÔNG CẦN:
//! - Bootstrap node
//! - NodeId của bất kỳ ai
//! - Ticket
//!
//! CHỈ CẦN:
//! - Tên topic (ví dụ: "market")
//! - Tracker NodeId (cố định, hardcode trong app)
//!
//! Cách chạy:
//! 1. Chạy tracker: cd iroh-experiments/content-discovery && cargo run --release
//! 2. Terminal 1: cargo run --example 08_tracker_topic -- --topic market --tracker <tracker_id>
//! 3. Terminal 2: cargo run --example 08_tracker_topic -- --topic market --tracker <tracker_id>
//!
//! Khi chạy lần đầu, lưu tracker ID vào env hoặc config.

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
use tracing::{info, warn};

#[derive(Parser)]
struct Cli {
    /// Topic name to join
    #[arg(short, long, default_value = "market")]
    topic: String,

    /// Tracker NodeId (required)
    #[arg(long)]
    tracker: String,
    
    /// Re-announce interval in seconds
    #[arg(long, default_value = "60")]
    reannounce_interval: u64,
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("error,08_tracker_topic=info")
        .init();

    let cli = Cli::parse();
    let topic_name = &cli.topic;
    let topic_id = topic_to_id(topic_name);
    let topic_hash = topic_to_hash(topic_name);
    let tracker_id = parse_tracker_id(&cli.tracker)?;

    info!("========================================");
    info!("TRACKER-BASED TOPIC DISCOVERY");
    info!("========================================");
    info!("Topic: '{}'", topic_name);
    info!("Tracker: {}", &cli.tracker[..16]);
    info!("========================================");

    // Step 1: Create endpoint with discovery
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

    endpoint.online().await;
    info!("My NodeId: {}", my_id);

    // Step 2: Query tracker for existing peers in topic
    info!("Querying tracker for peers in topic '{}'...", topic_name);
    
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
            info!("Found {} peers from tracker", announces.len());
            for announce in announces {
                let peer_id = announce.host;
                if peer_id != EndpointId::from(my_id) {
                    info!("[+] Found peer: {}", &peer_id.to_string()[..16]);
                    peer_node_ids.push(peer_id);
                }
            }
        }
        Err(e) => {
            warn!("Failed to query tracker: {} (continuing anyway)", e);
        }
    }

    // Step 3: Announce myself to tracker
    info!("Announcing myself to tracker...");
    
    let announce_data = Announce {
        host: EndpointId::from(my_id),
        content: topic_hash,
        kind: AnnounceKind::Complete,
        timestamp: AbsoluteTime::now(),
    };
    
    let signed_announce = SignedAnnounce::new(announce_data, &secret_key)?;
    
    match announce(&endpoint, tracker_id, signed_announce).await {
        Ok(()) => info!("[OK] Announced to tracker"),
        Err(e) => warn!("Failed to announce: {}", e),
    }

    // Step 4: Setup Gossip
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    // Step 5: Join gossip with found peers
    let gossip_topic = if peer_node_ids.is_empty() {
        info!("No peers found. Starting new topic...");
        gossip.subscribe(topic_id, vec![]).await?
    } else {
        info!("Joining topic with {} peers...", peer_node_ids.len());
        gossip.subscribe_and_join(topic_id, peer_node_ids).await?
    };

    info!("[OK] Joined topic '{}' via gossip", topic_name);

    // Task: Re-announce periodically
    let endpoint_for_announce = endpoint.clone();
    let secret_key_for_announce = secret_key.clone();
    let reannounce_interval = cli.reannounce_interval;
    
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(reannounce_interval)).await;
            
            let announce_data = Announce {
                host: EndpointId::from(my_id),
                content: topic_hash,
                kind: AnnounceKind::Complete,
                timestamp: AbsoluteTime::now(),
            };
            
            match SignedAnnounce::new(announce_data, &secret_key_for_announce) {
                Ok(signed) => {
                    match announce(&endpoint_for_announce, tracker_id, signed).await {
                        Ok(()) => info!("[Re-announce] Success"),
                        Err(e) => warn!("[Re-announce] Failed: {}", e),
                    }
                }
                Err(e) => warn!("[Re-announce] Sign failed: {}", e),
            }
        }
    });

    let (sender, mut receiver) = gossip_topic.split();

    info!("");
    info!("==========================================");
    info!("CHAT READY - Type message and press Enter");
    info!("==========================================");

    // Task: Stdin input
    let sender_clone = sender.clone();
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut framed = Framed::new(stdin, LinesCodec::new());

        while let Some(Ok(line)) = framed.next().await {
            if line.trim().is_empty() {
                continue;
            }
            if let Err(e) = sender_clone.broadcast(bytes::Bytes::from(line)).await {
                warn!("Failed to send: {}", e);
            }
        }
    });

    // Receive loop
    while let Some(event) = receiver.next().await {
        use iroh_gossip::api::Event;
        match event {
            Ok(Event::Received(msg)) => {
                let from = &msg.delivered_from.to_string()[..8];
                let text = String::from_utf8_lossy(&msg.content);
                println!("\x1b[36m[{}]: {}\x1b[0m", from, text.trim());
            }
            Ok(Event::NeighborUp(peer)) => {
                info!("[+] Peer joined: {}", &peer.to_string()[..16]);
            }
            Ok(Event::NeighborDown(peer)) => {
                info!("[-] Peer left: {}", &peer.to_string()[..16]);
            }
            Ok(Event::Lagged) => {
                warn!("[!] Message queue lagged");
            }
            Err(e) => {
                warn!("Gossip error: {}", e);
                break;
            }
        }
    }

    router.shutdown().await?;
    Ok(())
}
