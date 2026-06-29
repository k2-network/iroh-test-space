//! Bài 13: K2-Network Marketplace Monitor
//!
//! - Specialized TUI for monitoring K2 Marketplace transactions
//! - Supports JSON protocol used by K2 Tauri App
//! - Displays Offers and Interests in a structured format
//!

use anyhow::Result;
use chrono::{DateTime, Local, NaiveDateTime, Utc};
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
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
use ratatui::{
    symbols::Marker,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, BorderType, List, ListItem, Paragraph, canvas::{Canvas, Context, Rectangle}},
    Frame, Terminal,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, io, sync::Arc, time::Duration};
use tokio::sync::mpsc;

// --- CONSTANTS ---
const DEFAULT_TRACKER: &str = "71853750efc1219d7976639087c5fb25cf8d4b49f6d509366f2e094a3f781623";

const PRIMARY: Color = Color::Rgb(255, 165, 0); // Orange for Marketplace
const SECONDARY: Color = Color::Rgb(0, 200, 200); // Cyan for Network
const TEXT_DIM: Color = Color::Rgb(100, 110, 130);

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "Freelance Job")]
    topic: String,
    #[arg(short, long, default_value = "Monitor")]
    name: String,
    #[arg(short, long)]
    connect: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
enum MessageType {
    // K2 Marketplace Protocol
    K2Offer {
        sender_node_id: String,
        message_type: String, // "offer" | "interest"
        topic: String,
        form_data: serde_json::Value,
        #[serde(default)]
        timestamp: u64,
    },
    // Fallback
    Unknown(serde_json::Value),
}

#[derive(Clone)]
struct ChatMessage {
    timestamp: String,
    received_at: String,
    msg_type: MessageType,
}

#[derive(PartialEq, Clone)]
enum ViewState { Intro, Monitor }

struct AppState {
    messages: Vec<ChatMessage>,
    peers: HashSet<String>,
    status: String,
    topic: String,
    username: String,
    peer_count: usize,
    view: ViewState,
    loading_step: usize,
    loading_text: String,
}

impl AppState {
    fn new(topic: String, username: String) -> Self {
        Self {
            messages: vec![],
            peers: HashSet::new(),
            status: "Initializing...".to_string(),
            topic,
            username,
            peer_count: 0,
            view: ViewState::Intro,
            loading_step: 0,
            loading_text: "CONNECTING TO K2 MARKETPLACE...".to_string(),
        }
    }

    fn add_message(&mut self, msg: ChatMessage) {
        self.messages.push(msg);
        if self.messages.len() > 200 { self.messages.remove(0); }
    }
}

fn topic_to_id(topic: &str) -> TopicId {
    TopicId::from_bytes(blake3::hash(topic.as_bytes()).into())
}

fn topic_to_hash(topic: &str) -> HashAndFormat {
    let hash = iroh_blobs::Hash::from_bytes(topic_to_id(topic).as_bytes().clone());
    HashAndFormat::raw(hash)
}

fn parse_tracker_id(s: &str) -> Result<EndpointId> {
    let bytes = hex::decode(s)?;
    let arr: [u8; 32] = bytes.try_into().map_err(|_| anyhow::anyhow!("Invalid tracker ID"))?;
    Ok(EndpointId::from_bytes(&arr)?)
}

fn username_color(name: &str) -> Color {
    let hash = blake3::hash(name.as_bytes());
    let colors = [
        Color::Rgb(0, 200, 200), Color::Rgb(100, 200, 150), Color::Rgb(200, 150, 100),
        Color::Rgb(150, 100, 200), Color::Rgb(200, 100, 150), Color::Rgb(100, 150, 200),
    ];
    colors[(hash.as_bytes()[0] as usize) % colors.len()]
}

fn ui(f: &mut Frame, state: &AppState) {
    match state.view {
        ViewState::Intro => render_intro(f, state),
        ViewState::Monitor => render_monitor(f, state),
    }
}

fn render_intro(f: &mut Frame, state: &AppState) {
    let area = f.area();
    let layout = Layout::default().direction(Direction::Vertical).constraints([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(3),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
    ]).split(area);

    f.render_widget(
        Paragraph::new(" K2 MARKETPLACE MONITOR").style(Style::default().fg(PRIMARY).add_modifier(Modifier::BOLD)),
        layout[1]
    );
    
    let info = Line::from(vec![
        Span::styled(" [ ", Style::default().fg(TEXT_DIM)),
        Span::styled("Real-time P2P Trading", Style::default().fg(Color::White)),
        Span::styled(" ]", Style::default().fg(TEXT_DIM)),
    ]);
    f.render_widget(Paragraph::new(info), layout[2]);
    
    let w = layout[4].width.saturating_sub(2) as usize;
    let p = (state.loading_step * w / 100).min(w);
    let bar = format!(" {}", "█".repeat(p)); 
    f.render_widget(Paragraph::new(bar).style(Style::default().fg(PRIMARY)), layout[4]);
    
    f.render_widget(
        Paragraph::new(format!(" {}", state.loading_text)).style(Style::default().fg(TEXT_DIM)),
        layout[5]
    );
}

fn render_monitor(f: &mut Frame, state: &AppState) {
    let area = f.area();
    let main_layout = Layout::default().direction(Direction::Vertical).constraints([
        Constraint::Length(3), // Header with stats
        Constraint::Min(10),   // Content
        Constraint::Length(1), // Footer
    ]).split(area);

    // === HEADER ===
    let header_block = Block::default().borders(Borders::BOTTOM).border_style(Style::default().fg(TEXT_DIM));
    let header_inner = header_block.inner(main_layout[0]);
    f.render_widget(header_block, main_layout[0]);

    let header_text = vec![
        Line::from(vec![
            Span::styled("K2 MARKETPLACE ", Style::default().fg(PRIMARY).add_modifier(Modifier::BOLD)),
            Span::styled(format!("#{} ", state.topic), Style::default().fg(Color::White)),
            Span::styled("• ", Style::default().fg(TEXT_DIM)),
            Span::styled(format!("{} nodes connected", state.peer_count + 1), Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
             Span::styled(format!("Listening for JSON Offers/Interests..."), Style::default().fg(TEXT_DIM)),
        ])
    ];
    f.render_widget(Paragraph::new(header_text), header_inner);

    // === LIST ===
    let list_area = main_layout[1];
    
    let items: Vec<ListItem> = state.messages.iter().rev().map(|msg| {
        let mut lines = vec![];
        
        match &msg.msg_type {
            MessageType::K2Offer { sender_node_id, message_type, topic: _, form_data, timestamp: _ } => {
                 let title = form_data.get("title").and_then(|v| v.as_str()).unwrap_or("No Title");
                 let desc = form_data.get("description").and_then(|v| v.as_str()).unwrap_or("");
                 let price_range = form_data.get("priceRange");
                 
                 let price_str = if let Some(range) = price_range {
                     let min = range.get("min").and_then(|v| v.as_u64()).unwrap_or(0);
                     let max = range.get("max").and_then(|v| v.as_u64()).unwrap_or(0);
                     let currency = range.get("currency").and_then(|v| v.as_str()).unwrap_or("USD");
                     if max > min {
                        format!("{} - {} {}", min, max, currency)
                     } else {
                        format!("{} {}", min, currency)
                     }
                 } else {
                     let min = form_data.get("price_min").and_then(|v| v.as_u64()).unwrap_or(0);
                     format!("${}", min)
                 };

                 let (icon, color, label) = if message_type == "offer" {
                     ("📦", Color::Yellow, "OFFER")
                 } else {
                     ("🤝", Color::Magenta, "INTEREST")
                 };

                 // Header Line
                 lines.push(Line::from(vec![
                     Span::styled(format!("{} {}", icon, label), Style::default().bg(color).fg(Color::Black).add_modifier(Modifier::BOLD)),
                     Span::styled(" ", Style::default()),
                     Span::styled(title, Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                     Span::styled(" ", Style::default()),
                     Span::styled(price_str, Style::default().fg(Color::Green)),
                     Span::styled(format!("  [{}]", msg.received_at), Style::default().fg(TEXT_DIM)),
                 ]));

                 // Details Line
                 lines.push(Line::from(vec![
                     Span::styled("   From: ", Style::default().fg(TEXT_DIM)),
                     Span::styled(&sender_node_id[..12], Style::default().fg(username_color(sender_node_id))),
                     Span::styled("...", Style::default().fg(TEXT_DIM)),
                 ]));
                 
                 if !desc.is_empty() {
                    lines.push(Line::from(vec![
                        Span::styled(format!("   \"{}\"", desc), Style::default().fg(Color::Gray).add_modifier(Modifier::ITALIC)),
                    ]));
                 }
                 lines.push(Line::from("")); // Spacer
            },
            MessageType::Unknown(v) => {
                lines.push(Line::from(vec![
                     Span::styled("❓ UNKNOWN FORMAT", Style::default().fg(Color::DarkGray)),
                     Span::styled(format!("  [{}]", msg.received_at), Style::default().fg(TEXT_DIM)),
                ]));
                lines.push(Line::from(vec![
                     Span::styled(format!("   {:?}", v), Style::default().fg(Color::DarkGray)),
                ]));
                lines.push(Line::from(""));
            }
        }
        
        ListItem::new(lines)
    }).collect();

    f.render_widget(List::new(items), list_area);

    // === FOOTER ===
    let footer_text = "Press 'Esc' or 'q' to quit";
    f.render_widget(
        Paragraph::new(footer_text).style(Style::default().fg(TEXT_DIM)).alignment(Alignment::Center),
        main_layout[2]
    );
}

enum UiEvent {
    Gossip(ChatMessage),
    PeerUp(String),
    PeerDown(String),
    Progress(usize, String),
    Ready,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, cli).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;
    if let Err(e) = result { eprintln!("Error: {:?}", e); }
    Ok(())
}

async fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, cli: Cli) -> Result<()> {
    let topic_name = cli.topic.clone();
    let username = cli.name.clone();
    let topic_id = topic_to_id(&topic_name);
    let topic_hash = topic_to_hash(&topic_name);
    let tracker_id = parse_tracker_id(DEFAULT_TRACKER)?;
    let connect_target = cli.connect.clone();

    let mut state = AppState::new(topic_name.clone(), username.clone());
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();

    // Spawn connection task
    let ui_tx2 = ui_tx.clone();
    tokio::spawn(async move {
        ui_tx2.send(UiEvent::Progress(10, "GENERATING KEYS...".into())).ok();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let secret_key = SecretKey::generate(&mut rand::rng());
        let my_id = secret_key.public();

        ui_tx2.send(UiEvent::Progress(30, "CONNECTING TO RELAY...".into())).ok();
        let discovery = DhtDiscovery::builder().n0_dns_pkarr_relay().dht(true).include_direct_addresses(true).secret_key(secret_key.clone()).build().unwrap();
        let endpoint = Endpoint::builder().secret_key(secret_key.clone()).discovery(discovery).alpns(vec![GOSSIP_ALPN.to_vec(), ALPN.to_vec()]).bind().await.unwrap();

        ui_tx2.send(UiEvent::Progress(50, format!("NODE: {}...", &my_id.to_string()[..8]))).ok();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut peers = vec![];
        if let Some(target) = connect_target {
            ui_tx2.send(UiEvent::Progress(40, format!("CONNECTING TO TARGET: {}...", &target[..8]))).ok();
            if let Ok(bytes) = hex::decode(&target) {
                if let Ok(pk) = iroh::PublicKey::from_bytes(&bytes.try_into().unwrap_or([0;32])) {
                    // Direct connect
                    let _ = endpoint.connect(pk, GOSSIP_ALPN).await;
                    peers.push(EndpointId::from(pk)); // Add to gossip join list
                    ui_tx2.send(UiEvent::Progress(45, "TARGET CONNECTED!".into())).ok();
                } else {
                    ui_tx2.send(UiEvent::Progress(45, "INVALID TARGET ID".into())).ok();
                }
            }
        }

        ui_tx2.send(UiEvent::Progress(70, "QUERYING TRACKER...".into())).ok();
        let query_args = Query { content: topic_hash, flags: QueryFlags { complete: false, verified: false } };
        // existing peers vec used here
        if let Ok(anns) = query(&endpoint, tracker_id, query_args).await {
            for a in anns { if a.host != EndpointId::from(my_id) { peers.push(a.host); } }
        }

        ui_tx2.send(UiEvent::Progress(85, format!("FOUND {} PEERS...", peers.len()))).ok();
        let ann = Announce { host: EndpointId::from(my_id), content: topic_hash, kind: AnnounceKind::Complete, timestamp: AbsoluteTime::now() };
        let _ = announce(&endpoint, tracker_id, SignedAnnounce::new(ann, &secret_key).unwrap()).await;

        let gossip = Gossip::builder().spawn(endpoint.clone());
        let _router = Router::builder(endpoint.clone()).accept(GOSSIP_ALPN, gossip.clone()).spawn();

        let targets = {
            use rand::seq::SliceRandom;
            let mut rng = rand::rng();
            peers.shuffle(&mut rng);
            peers.into_iter().take(10).collect::<Vec<_>>()
        };

        let topic = if targets.is_empty() {
            gossip.subscribe(topic_id, vec![]).await.unwrap()
        } else {
            match tokio::time::timeout(Duration::from_secs(10), gossip.subscribe_and_join(topic_id, targets)).await {
                Ok(Ok(t)) => t,
                _ => gossip.subscribe(topic_id, vec![]).await.unwrap(),
            }
        };

        ui_tx2.send(UiEvent::Progress(100, "CONNECTED!".into())).ok();
        tokio::time::sleep(Duration::from_millis(500)).await;
        ui_tx2.send(UiEvent::Ready).ok();

        let (sender, mut receiver) = topic.split();
        let sender = Arc::new(sender);
        
        while let Some(ev) = receiver.next().await {
            use iroh_gossip::api::Event as GE;
            match ev {
                Ok(GE::Received(m)) => {
                    // Try to parse as MessageType
                    if let Ok(json_msg) = serde_json::from_slice::<MessageType>(&m.content) {
                         // Send to UI
                         ui_tx2.send(UiEvent::Gossip(ChatMessage { 
                            timestamp: Local::now().to_string(),
                            received_at: Local::now().format("%H:%M:%S").to_string(),
                            msg_type: json_msg.clone() 
                        })).ok();

                        // AUTO-REPLY LOGIC
                        if let MessageType::K2Offer { message_type, sender_node_id, form_data, .. } = &json_msg {
                            if message_type == "offer" {
                                // We received an offer, let's send an interest!
                                let reply_payload = serde_json::json!({
                                    "sender_node_id": my_id.to_string(),
                                    "message_type": "interest",
                                    "topic": topic_name,
                                    "form_data": form_data, // Echo back the form data to confirm match
                                    "timestamp": Utc::now().timestamp_millis() as u64
                                });

                                if let Ok(reply_bytes) = serde_json::to_vec(&reply_payload) {
                                    let s = sender.clone();
                                    tokio::spawn(async move {
                                        // Wait a tiny bit to simulate human reaction speed (and avoid race conditions)
                                        tokio::time::sleep(Duration::from_millis(500)).await;
                                        if let Err(e) = s.broadcast(reply_bytes.into()).await {
                                            eprintln!("Failed to send auto-reply: {}", e);
                                        }
                                    });
                                }
                            }
                        }

                    } else {
                        // Try to parse generic JSON if it doesn't match MessageType
                        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&m.content) {
                            ui_tx2.send(UiEvent::Gossip(ChatMessage { 
                                timestamp: Local::now().to_string(),
                                received_at: Local::now().format("%H:%M:%S").to_string(),
                                msg_type: MessageType::Unknown(v)
                            })).ok();
                        }
                    }
                }
                Ok(GE::NeighborUp(_id)) => { ui_tx2.send(UiEvent::PeerUp("Peer Connected".into())).ok(); }
                Ok(GE::NeighborDown(_id)) => { ui_tx2.send(UiEvent::PeerDown("Peer Disconnected".into())).ok(); }
                _ => {}
            }
        }
    });

    loop {
        terminal.draw(|f| ui(f, &state))?;

        while let Ok(ev) = ui_rx.try_recv() {
            match ev {
                UiEvent::Progress(p, t) => { state.loading_step = p; state.loading_text = t; }
                UiEvent::Ready => { state.view = ViewState::Monitor; state.status = "Monitoring".into(); }
                UiEvent::Gossip(m) => { state.add_message(m); }
                UiEvent::PeerUp(_) => { state.peer_count += 1; }
                UiEvent::PeerDown(_) => { state.peer_count = state.peer_count.saturating_sub(1); }
            }
        }

        if crossterm::event::poll(Duration::from_millis(50))? {
            if let Event::Key(k) = event::read()? {
                if k.kind != KeyEventKind::Press { continue; }
                match k.code {
                    KeyCode::Esc | KeyCode::Char('q') => { break; }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}
