//! Bai hoc 07: Pure DHT Topic Discovery (NO TRACKER!)
//! 
//! Su dung Mainline DHT truc tiep de tim peers trong topic.
//! HOAN TOAN MIEN PHI - Khong can server, khong can tracker!
//!
//! Cach dung:
//! Terminal 1: cargo run --example 07_dht_topic -- --topic market
//! Terminal 2: cargo run --example 07_dht_topic -- --topic market
//! Terminal 3: cargo run --example 07_dht_topic -- --topic market

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use iroh::{
    discovery::pkarr::dht::DhtDiscovery,
    protocol::Router,
    Endpoint, SecretKey,
};
use iroh_gossip::{
    api::{Event, GossipReceiver},
    net::{Gossip, GOSSIP_ALPN},
    proto::TopicId,
};
use mainline::Dht;
use std::time::Duration;
use std::net::SocketAddr;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{info, warn};

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "market")]
    topic: String,
    
    #[arg(long, default_value = "11204")]
    port: u16,
}

/// Tao TopicId tu ten
fn topic_to_id(topic: &str) -> TopicId {
    TopicId::from_bytes(blake3::hash(topic.as_bytes()).into())
}

/// Convert topic to mainline infohash (20 bytes)
fn topic_to_infohash(topic: &str) -> mainline::Id {
    let hash = blake3::hash(topic.as_bytes());
    let mut data = [0u8; 20];
    data.copy_from_slice(&hash.as_bytes()[..20]);
    mainline::Id::from_bytes(data).unwrap()
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("error,07_dht_topic=info")
        .init();

    let cli = Cli::parse();
    let topic_name = &cli.topic;
    let topic_id = topic_to_id(topic_name);
    let infohash = topic_to_infohash(topic_name);

    info!("PURE DHT TOPIC DISCOVERY (NO TRACKER!)");
    info!("Topic: '{}' | Infohash: {}", topic_name, infohash);

    // Step 1: Khoi tao Mainline DHT client
    info!("Initializing Mainline DHT...");
    let dht = Dht::client().map_err(|e| anyhow::anyhow!("DHT init failed: {}", e))?;
    
    // Step 2: Query DHT de tim peers da co trong topic
    info!("Querying DHT for existing peers...");
    let mut found_addrs: Vec<SocketAddr> = vec![];
    
    match dht.get_peers(infohash) {
        Ok(receiver) => {
            // Collect peers with timeout
            let query_result = tokio::time::timeout(Duration::from_secs(15), async {
                for addrs in receiver.into_iter().take(5) {
                    for addr in addrs {
                        info!("[+] Found peer from DHT: {}", addr);
                        found_addrs.push(addr);
                    }
                }
            }).await;
            
            if query_result.is_err() {
                info!("DHT query timeout (normal if no peers yet)");
            }
        }
        Err(e) => {
            warn!("DHT query failed: {} (continuing anyway)", e);
        }
    }
    
    info!("Found {} peer addresses from DHT", found_addrs.len());

    // Step 3: Tao Iroh endpoint
    let secret_key = SecretKey::generate(&mut rand::rng());
    let my_id = secret_key.public();

    let discovery = DhtDiscovery::builder()
        .n0_dns_pkarr_relay()
        .dht(true)
        .include_direct_addresses(true)
        .build()?;

    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .discovery(discovery)
        .bind()
        .await?;

    endpoint.online().await;
    let my_addrs = endpoint.bound_sockets();
    info!("My ID: {} | Port: {:?}", &my_id.to_string()[..16], my_addrs);

    // Step 4: Announce minh len DHT
    info!("Announcing myself to DHT...");
    let dht_async = dht.clone().as_async();
    let announce_result = dht_async.announce_peer(infohash, Some(cli.port)).await;
    match announce_result {
        Ok(stored_at) => info!("Announced to {} DHT nodes", stored_at),
        Err(e) => warn!("Announce failed: {} (continuing anyway)", e),
    }

    // Step 5: Gossip + Router
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    // Step 6: Join gossip
    // NOTE: DHT tra ve SocketAddr, nhung Iroh gossip can NodeId
    // De don gian, ta subscribe topic va cho peers tu connect
    // Sau nay co the implement NodeId exchange protocol
    
    let gossip_topic = gossip.subscribe(topic_id, vec![]).await?;
    info!("[OK] Subscribed to topic. Waiting for peers...");

    // Task: Re-announce dinh ky
    let dht_for_announce = dht.clone().as_async();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(300)).await;
            match dht_for_announce.announce_peer(infohash, Some(cli.port)).await {
                Ok(n) => info!("Re-announced to {} DHT nodes", n),
                Err(e) => warn!("Re-announce failed: {}", e),
            }
        }
    });

    let (sender, receiver) = gossip_topic.split();
    
    info!("===========================================");
    info!("PURE P2P CHAT - NO SERVER, NO TRACKER!");
    info!("Type message and press Enter.");
    info!("Share your NodeId with friends to connect!");
    info!("NodeId: {}", my_id);
    info!("===========================================");

    // Task: Stdin input
    let sender_clone = sender.clone();
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut framed = Framed::new(stdin, LinesCodec::new());
        
        while let Some(Ok(line)) = framed.next().await {
            if line.trim().is_empty() { continue; }
            let _ = sender_clone.broadcast(bytes::Bytes::from(line)).await;
        }
    });

    // Receive loop
    let mut receiver: GossipReceiver = receiver;
    
    while let Some(event) = receiver.next().await {
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
                warn!("Error: {}", e);
                break;
            }
        }
    }

    router.shutdown().await?;
    Ok(())
}
