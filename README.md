# Iroh P2P Tutorials

Các bài học thực hành về P2P networking với Iroh framework.

## Prerequisites

```bash
# Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Examples

| # | Tên | Mô tả | Command |
|---|-----|-------|---------|
| 01 | P2P Connect | Kết nối cơ bản giữa 2 nodes | `cargo run --example 01_p2p_connect` |
| 02 | File Transfer | Gửi file qua P2P | `cargo run --example 02_file_transfer` |
| 03 | P2P Chat | Chat 1-1 | `cargo run --example 03_p2p_chat` |
| 04 | Gossip Chat | Group chat với Gossip protocol | `cargo run --example 04_gossip_chat` |
| 05 | Topic Discovery | Invite link discovery | `cargo run --example 05_topic_discovery` |
| 06 | DHT Discovery | DHT với shared anchor key | `cargo run --example 06_dht_discovery` |
| 07 | DHT Topic | Pure DHT (không hoạt động tốt) | ❌ Deprecated |
| 08 | Tracker Topic | Tracker-based discovery (Basic) | `cargo run --example 08_tracker_topic` |
| 09 | **Tracker Chat UI** | **Professional CLI Chat** | ✅ Recommended |
| 10 | Tracker Optimized | Optimized Gossip Logic | `cargo run --example 10_tracker_gossip_optimized` |
| 11 | Ratatui Chat | Basic TUI Chat | `cargo run --example 11_ratatui_chat` |
| 12 | **K2 Premium Chat** | **Advanced TUI with Intro** | 🌟 Premium |

## Quick Start (New) - Chat với UI xịn

### Bước 1: Chạy Tracker (Bắt buộc cho các bài 08-12)

Tracker đóng vai trò giúp các peers tìm thấy nhau trong cùng một topic.

```bash
cd ../iroh-experiments/content-discovery
cargo run --release
# Output: tracker addr: <TRACKER_ID>
# Copy TRACKER_ID này nếu bạn muốn sửa code, tuy nhiên các bài 09-12 đã hardcode mặc định.
```

### Bước 2: Chạy Chat App (Professional CLI)

Bài 09 cung cấp giao diện CLI sạch sẽ, ẩn các log rác và hỗ trợ username.

```bash
# Terminal 1 (Alice)
cargo run --example 09_tracker_chat_ui -- --name Alice --topic marketing

# Terminal 2 (Bob)
cargo run --example 09_tracker_chat_ui -- --name Bob --topic marketing
```

### Bước 3: Chạy Premium Chat (TUI) - Bài 12

Bài 12 mang đến trải nghiệm Visual đỉnh cao với Intro screen, giao diện Ratatui và ASCII Art.

```bash
# Terminal 1
cargo run --example 12_k2_premium_chat -- --name Admin --topic general

# Terminal 2
cargo run --example 12_k2_premium_chat -- --name Guest --topic general
```

> **Lưu ý:** Bài 09 và 12 sử dụng **chung** logic tracker và protocol, nên chúng có thể chat thấy nhau nếu cùng topic!

## Dependencies

```toml
[dependencies]
iroh = { version = "0.95.0", features = ["discovery-pkarr-dht"] }
iroh-blobs = "0.97"
iroh-gossip = "0.95.0"
# ...
iroh-content-discovery = { path = "iroh-content-discovery", features = ["client"] }
ratatui = "0.29"
crossterm = "0.28"
```

## Related Documents

- [P2P_RESEARCH.md](../P2P_RESEARCH.md) - Tài liệu nghiên cứu chi tiết
