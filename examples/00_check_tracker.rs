//! Check if the K2 tracker is online by sending a query.
//!
//! Usage: cargo run --example 00_check_tracker

use anyhow::Result;
use iroh::endpoint::presets;
use iroh::{Endpoint, SecretKey};
use iroh_blobs::HashAndFormat;
use iroh_content_discovery::{
    protocol::{Query, QueryFlags},
    query,
};

const DEFAULT_TRACKER: &str = "71853750efc1219d7976639087c5fb25cf8d4b49f6d509366f2e094a3f781623";

fn parse_tracker_id(s: &str) -> Result<iroh::EndpointId> {
    let bytes = hex::decode(s)?;
    let arr: [u8; 32] = bytes.try_into().map_err(|_| anyhow::anyhow!("Invalid tracker ID"))?;
    Ok(iroh::EndpointId::from_bytes(&arr)?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let secret_key = SecretKey::generate();
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(vec![iroh_content_discovery::protocol::ALPN.to_vec()])
        .bind()
        .await?;

    let tracker_id = parse_tracker_id(DEFAULT_TRACKER)?;
    println!("Tracker ID: {}", tracker_id);
    println!("Connecting to tracker...");

    let dummy_hash = HashAndFormat::raw(iroh_blobs::Hash::from_bytes([0u8; 32]));
    let query_args = Query {
        content: dummy_hash,
        flags: QueryFlags {
            complete: false,
            verified: false,
        },
    };

    match query(&endpoint, tracker_id, query_args).await {
        Ok(anns) => println!("✓ Tracker ONLINE ({} announces found)", anns.len()),
        Err(e) => println!("✗ Tracker OFFLINE: {}", e),
    }

    Ok(())
}
