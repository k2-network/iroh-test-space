//! Bài học 2: Chia sẻ file
//! 
//! Mục tiêu: Chia sẻ và tải file qua mạng P2P dùng Iroh Blobs (MemStore).

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use iroh::{discovery::pkarr::dht::DhtDiscovery, protocol::Router, Endpoint, SecretKey};
use iroh_blobs::{store::mem::MemStore, BlobsProtocol, ticket::BlobTicket};
use std::{path::PathBuf, str::FromStr, sync::Arc};
use tracing::{info, error};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Chia sẻ file
    Share {
        /// Đường dẫn file cần gửi
        path: PathBuf,
    },
    /// Tải file
    Download {
        /// Ticket nhận được từ người gửi
        ticket: String,
        /// Thư mục lưu file (mặc định: thư mục hiện tại)
        #[arg(short, long, default_value = ".")]
        output_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("error,code=info,01_p2p_connect=info,02_file_transfer=info")
        .init();

    let cli = Cli::parse();

    // 1. Khởi tạo Node giống k2-core
    info!("🚀 Khởi tạo Iroh Node với Storage...");

    // iroh 0.95: Tạo secret key trước
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
        .bind()
        .await?;

    // Tạo bộ nhớ tạm (RAM) để lưu file chia sẻ, giống k2-core
    let store = MemStore::new();
    let blobs = BlobsProtocol::new(&store, None);

    // Gắn giao thức Blobs vào Router để xử lý request tải file
    // iroh 0.95 + iroh-blobs 0.97: spawn() không cần .await
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn();

    let node = NodeWrapper { endpoint, blobs, store, _router: Arc::new(router) };

    info!("✅ Node sẵn sàng: {}", my_node_id);

    match &cli.command {
        Commands::Share { path } => {
            if !path.exists() {
                error!("❌ File không tồn tại!");
                return Ok(());
            }

            info!("📤 Đang đọc file vào bộ nhớ...");
            let content = tokio::fs::read(path).await?;
            let filename = path.file_name().unwrap_or_default().to_string_lossy();

            // Thêm data vào store
            let tag = node.store.add_slice(&content).await?;
            
            // iroh 0.95: dùng endpoint.addr() thay vì node_addr()
            let addr = node.endpoint.addr();
            let ticket = BlobTicket::new(addr, tag.hash, tag.format);

            // Format ticket kèm tên file giống k2-core
            let share_string = format!("{}|{}", filename, ticket);

            info!("✨ ĐÃ CHIA SẺ THÀNH CÔNG!");
            println!("\nCopy chuỗi dưới đây gửi cho người nhận:");
            println!("{}", share_string);
            println!("\nGiữ chương trình chạy để seed file...");
            
            // Chờ mãi mãi
            tokio::signal::ctrl_c().await?;
            info!("🛑 Đang dừng chia sẻ...");
        }
        Commands::Download { ticket, output_dir } => {
            // Parse custom format: filename|ticket
            let parts: Vec<&str> = ticket.split('|').collect();
            if parts.len() != 2 {
                error!("❌ Sai định dạng ticket! Phải có dạng 'filename|ticket'");
                return Ok(());
            }

            let filename = parts[0];
            let raw_ticket = parts[1];
            let ticket = BlobTicket::from_str(raw_ticket).context("Invalid ticket")?;

            info!("⬇️  Đang tải file: {}", filename);
            info!("🔍 Đang tìm kiếm peer...");

            // Tải dữ liệu qua blobs API
            let downloader = node.blobs.downloader(&node.endpoint);
            
            // iroh-blobs 0.97: download trả về Result
            match downloader.download(ticket.hash(), vec![ticket.addr().id]).await {
                Ok(_stats) => {
                    // Download hoàn thành
                }
                Err(e) => {
                    error!("❌ Lỗi tải file: {}", e);
                    return Ok(());
                }
            }
            
            // Đọc từ MemStore ra
            let content = node.store.get_bytes(ticket.hash()).await?;

            // Ghi ra đĩa
            if !output_dir.exists() {
                tokio::fs::create_dir_all(output_dir).await?;
            }
            let save_path = output_dir.join(filename);
            tokio::fs::write(&save_path, content).await?;

            info!("🎉 TẢI XONG! File lưu tại: {:?}", save_path);
        }
    }

    Ok(())
}

struct NodeWrapper {
    endpoint: Endpoint,
    blobs: BlobsProtocol,
    store: MemStore,
    _router: Arc<Router>,
}
