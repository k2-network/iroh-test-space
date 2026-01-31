
use anyhow::Result;
use clap::{Parser, Subcommand};
use iroh::{discovery::pkarr::dht::DhtDiscovery, Endpoint, EndpointId, SecretKey};
use std::time::Duration;
use tracing::{info, error};


#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Listen,
    Connect {
        node_id: String,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Cấu hình log: Chỉ hiện log của bài học (info) và ẩn log thư viện (error)
    tracing_subscriber::fmt()
        .with_env_filter("error,code=info,01_p2p_connect=info,02_file_transfer=info")
        .init();

    let cli = Cli::parse();

    info!("Khởi tạo Iroh Node...");

    // Tạo secret key để lấy node ID
    let secret_key = SecretKey::generate(&mut rand::rng());
    let my_node_id = secret_key.public();

    let dht_discovery = DhtDiscovery::builder()
        .n0_dns_pkarr_relay()
        .dht(true)
        .include_direct_addresses(true)
        .secret_key(secret_key.clone());
    
    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .discovery(dht_discovery)
        .alpns(vec![b"k2-tutorial".to_vec()])
        .bind()
        .await?;
    
    info!("Node ID: {}", my_node_id);

    match &cli.command {
        Commands::Listen => {
            info!("Đang chờ kết nối... (Giữ terminal này mở)");
            
            while let Some(incoming) = endpoint.accept().await {
                
                match incoming.accept() {
                    Ok(connecting) => {
                        tokio::spawn(async move {
                            match connecting.await {
                                Ok(connection) => {
                                    // iroh 0.95: dùng remote_id() thay vì remote_node_id()
                                    let remote_id = connection.remote_id();
                                    info!("Đã kết nối thành công với Peer: {}", remote_id);
                                }
                                Err(e) => {
                                    error!("Lỗi khi bắt tay (handshake): {}", e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("Lỗi khi chấp nhận kết nối: {}", e);
                    }
                }
            }
        }

        Commands::Connect {node_id} => {
            // iroh 0.95: Parse EndpointId từ hex string
            let bytes = hex::decode(node_id)?;
            let arr: [u8; 32] = bytes.try_into().map_err(|_| anyhow::anyhow!("Invalid Node ID length"))?;
            let target_node_id = EndpointId::from_bytes(&arr)?;
            info!("Đang kết nối {}...", target_node_id);

            let alpn = b"k2-tutorial";

            let connect_result = tokio::time::timeout(
                Duration::from_secs(20),
                endpoint.connect(target_node_id, alpn)
            ).await;

            match connect_result {
                Ok(Ok(_connection)) => {
                    info!("Kết nối thành công");
                }
                Ok(Err(e)) => {
                    error!("Lỗi kết nối: {}", e);
                }

                Err(_)=> {
                    error!("Hết thời gian chờ (Timeout)");
                }
            }
        }
    }

    Ok(())
}