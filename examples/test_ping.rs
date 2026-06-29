use anyhow::Result;
use iroh::{
    discovery::{pkarr::dht::DhtDiscovery, ConcurrentDiscovery, mdns::MdnsDiscovery},
    Endpoint, SecretKey,
};
use std::time::{Duration, Instant};
use std::io::{self, Write};
use iroh_base::PublicKey;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Khởi tạo danh tính
    let secret_key = SecretKey::generate(&mut rand::rng());
    let my_id = secret_key.public();
    println!("==========================================");
    println!("🚀 K2 PING TEST TOOL (Iroh 0.95)");
    println!("MY NODE ID (HEX): {}", hex::encode(my_id.as_bytes()));
    println!("MY NODE ID (z32): {}", my_id.to_string());
    println!("==========================================");

    // 2. Cấu hình Discovery (Pkarr + DNS + mDNS)
    println!("📡 Initializing discovery (DNS + Pkarr + DHT + mDNS)...");
    
    // DHT/Pkarr Discovery
    let dht_discovery = DhtDiscovery::builder()
        .n0_dns_pkarr_relay()
        .dht(true)
        .include_direct_addresses(true)
        .secret_key(secret_key.clone())
        .build()?;

    // Kết hợp với Local Discovery (mDNS)
    let discovery = ConcurrentDiscovery::from_services(vec![
        Box::new(dht_discovery),
        Box::new(MdnsDiscovery::builder().build(my_id.into())?),
    ]);

    // 3. Khởi tạo Endpoint
    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .discovery(discovery)
        .alpns(vec![iroh_blobs::ALPN.to_vec()])
        .bind()
        .await?;

    println!("✅ Endpoint is ready and bound.");

    // Check for CLI arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        let target_hex = &args[1];
        ping_target(&endpoint, target_hex).await?;
        return Ok(());
    }

    println!("Waiting for discovery to warm up (5s)...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    loop {
        println!("\n------------------------------------------");
        print!("ENTER REMOTE NODE ID (HEX) or 'exit': ");
        io::stdout().flush()?;
        
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let target_hex = input.trim();
        
        if target_hex.is_empty() { continue; }
        if target_hex == "exit" { break; }

        ping_target(&endpoint, target_hex).await?;
    }

    Ok(())
}

async fn ping_target(endpoint: &iroh::Endpoint, target_hex: &str) -> Result<()> {
    // Parse Hex string to PublicKey
    let target_id = match hex::decode(target_hex) {
        Ok(bytes) => {
            if bytes.len() != 32 {
                println!("❌ Invalid Hex length: expected 64 chars (32 bytes)");
                return Ok(());
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            match PublicKey::from_bytes(&arr) {
                Ok(id) => id,
                Err(e) => {
                    println!("❌ Invalid Key: {:?}", e);
                    return Ok(());
                }
            }
        }
        Err(e) => {
            println!("❌ Invalid Hex format: {:?}", e);
            return Ok(());
        }
    };

    println!("🔍 Attempting to connect to {}...", target_id);
    let start = Instant::now();

    // Thử kết nối với timeout 30s
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        endpoint.connect(target_id, iroh_blobs::ALPN)
    ).await;

    match result {
        Ok(Ok(_conn)) => {
            let duration = start.elapsed();
            println!("✅ ONLINE! Connected in {:?}", duration);
        }
        Ok(Err(e)) => {
            println!("📴 OFFLINE (Connect Error): {:?}", e);
        }
        Err(_) => {
            println!("⏳ OFFLINE (Timeout after 30s)");
            println!("Tip: Make sure the remote node is running and has announced itself to Pkarr.");
        }
    }
    Ok(())
}
