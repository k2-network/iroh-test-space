//! Bài 12: K2-Network Chat - Premium TUI Edition
//!
//! - K2 Whale ASCII Art Logo (Geometric/Blocky style)
//! - Gothic Pixel Text (Binsider style)
//! - Intro Loading Screen
//! - Premium Cyan/Teal color scheme

use anyhow::Result;
use chrono::Local;
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

const PRIMARY: Color = Color::Rgb(0, 200, 200);
const TEXT_DIM: Color = Color::Rgb(100, 110, 130);

// Binsider-style font for K2 NETWORK
const INTRO_TEXT: &str = r#"
 █  █  ▀▀█     █▄  █ █▀▀ ▀█▀ █   █ █▀▀█ █▀▀▄ █  █
 █▀▀   ▄▄█     █ █ █ █▀▀  █  █ █ █ █  █ █▄▄▀ █▀▄
 █  █ █▄▄▄     █  ▀█ █▄▄  █  █▄▀▄█ █▄▄█ █  █ █  █
"#;

// Geometric Whale - drawn with Pixel Art Editor
// Pixel Perfect Whale (Bitmap Rendering with 2x Width for Square Pixels)

const SMALL_WHALE: &str = r#"   ▄▄▄▄███▄▄
  ███████████  ▄
  ▀▀███████▀▀ ▀█▀
    █▀   ▀█"#;

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "lobby")]
    topic: String,
    #[arg(short, long, default_value = "Anonymous")]
    name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum MessageType {
    Chat { sender: String, text: String },
    Join { user: String },
    Leave { user: String },
}

#[derive(Clone)]
struct ChatMessage {
    timestamp: String,
    msg_type: MessageType,
}

#[derive(PartialEq, Clone)]
enum ViewState { Intro, Chat }

struct AppState {
    messages: Vec<ChatMessage>,
    input: String,
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
            input: String::new(),
            peers: HashSet::new(),
            status: "Initializing...".to_string(),
            topic,
            username,
            peer_count: 0,
            view: ViewState::Intro,
            loading_step: 0,
            loading_text: "INITIALIZING K2 PROTOCOL...".to_string(),
        }
    }

    fn add_message(&mut self, msg: ChatMessage) {
        self.messages.push(msg);
        if self.messages.len() > 200 { self.messages.remove(0); }
    }

    fn add_system(&mut self, text: &str) {
        self.add_message(ChatMessage {
            timestamp: Local::now().format("%H:%M").to_string(),
            msg_type: MessageType::Chat { sender: "⚡".to_string(), text: text.to_string() },
        });
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

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let v = Layout::default().direction(Direction::Vertical).constraints([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ]).split(r);
    Layout::default().direction(Direction::Horizontal).constraints([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ]).split(v[1])[1]
}

fn ui(f: &mut Frame, state: &AppState) {
    match state.view {
        ViewState::Intro => render_intro(f, state),
        ViewState::Chat => render_chat(f, state),
    }
}

fn render_intro(f: &mut Frame, state: &AppState) {
    let area = f.area();
    
    // Simple vertical layout, no border, left-aligned
    let layout = Layout::default().direction(Direction::Vertical).constraints([
        Constraint::Length(1),  // 0: Empty top margin
        Constraint::Length(1),  // 1: K2 NETWORK Title
        Constraint::Length(1),  // 2: Info line
        Constraint::Min(3),     // 3: Spacer
        Constraint::Length(1),  // 4: Loading bar
        Constraint::Length(1),  // 5: Status
        Constraint::Length(1),  // 6: Bottom margin
    ]).split(area);

    // K2 NETWORK - Plain uppercase, cyan, left-aligned
    f.render_widget(
        Paragraph::new(" K2 NETWORK").style(Style::default().fg(PRIMARY).add_modifier(Modifier::BOLD)),
        layout[1]
    );
    
    // Info line - left-aligned
    let info = Line::from(vec![
        Span::styled(" [ ", Style::default().fg(TEXT_DIM)),
        Span::styled("Github", Style::default().fg(Color::White)),
        Span::styled(" ]  [ ", Style::default().fg(TEXT_DIM)),
        Span::styled("StellarHold", Style::default().fg(Color::White)),
        Span::styled(" ]  [ ", Style::default().fg(TEXT_DIM)),
        Span::styled("1.0.0", Style::default().fg(Color::White)),
        Span::styled(" ]", Style::default().fg(TEXT_DIM)),
    ]);
    f.render_widget(Paragraph::new(info), layout[2]);
    
    // Loading bar (ultra thin)
    let w = layout[4].width.saturating_sub(2) as usize;
    let p = (state.loading_step * w / 100).min(w);
    let bar = format!(" {}", "▁".repeat(p)); // Thinnest block
    f.render_widget(Paragraph::new(bar).style(Style::default().fg(PRIMARY)), layout[4]);
    
    // Status - left-aligned
    f.render_widget(
        Paragraph::new(format!(" {}", state.loading_text)).style(Style::default().fg(TEXT_DIM)),
        layout[5]
    );
}

fn render_chat(f: &mut Frame, state: &AppState) {
    let area = f.area();
    
    // Main layout: Header, Content (Messages + Sidebar), Input, Footer
    let main_layout = Layout::default().direction(Direction::Vertical).constraints([
        Constraint::Length(2),  // Header
        Constraint::Min(10),    // Content Area (Messages + Sidebar)
        Constraint::Length(3),  // Input Box
        Constraint::Length(1),  // Footer
    ]).split(area);

    // === HEADER (No border, just text) ===
    let header = Line::from(vec![
        Span::styled(" K2 ", Style::default().fg(PRIMARY).add_modifier(Modifier::BOLD)),
        Span::styled("NETWORK ", Style::default().fg(Color::White)),
        Span::styled(format!("#{} ", state.topic), Style::default().fg(TEXT_DIM)),
        Span::styled(format!("• {} peers online", state.peer_count), Style::default().fg(Color::Yellow)),
    ]);
    f.render_widget(Paragraph::new(header), main_layout[0]);

    // === CONTENT: Split into Messages (left) + Sidebar (right) ===
    let content_layout = Layout::default().direction(Direction::Horizontal).constraints([
        Constraint::Min(40),      // Messages
        Constraint::Length(22),   // Sidebar (Online Users)
    ]).split(main_layout[1]);

    // === MESSAGES (Discord-style, no border) ===
    let msg_area = content_layout[0];
    let max_msgs = (msg_area.height / 2) as usize; // 2 lines per message
    let skip = state.messages.len().saturating_sub(max_msgs);
    
    let mut msg_lines: Vec<Line> = Vec::new();
    for m in state.messages.iter().skip(skip) {
        match &m.msg_type {
            MessageType::Chat { sender, text } => {
                let col = if sender == "⚡" { TEXT_DIM } 
                          else if sender == &state.username { Color::Green } 
                          else { username_color(sender) };
                // Line 1: Badge + Username + Time
                msg_lines.push(Line::from(vec![
                    Span::styled("⚔️k2-team ", Style::default().fg(TEXT_DIM)),
                    Span::styled(sender, Style::default().fg(col).add_modifier(Modifier::BOLD)),
                    Span::styled(format!("  {}", m.timestamp), Style::default().fg(TEXT_DIM)),
                ]));
                // Line 2: Indented content
                msg_lines.push(Line::from(vec![
                    Span::raw("   "),
                    Span::styled(text, Style::default().fg(Color::White)),
                ]));
            }
            MessageType::Join { user } => {
                msg_lines.push(Line::from(Span::styled(format!(" → {} joined the chat", user), Style::default().fg(Color::Green))));
            }
            MessageType::Leave { user } => {
                msg_lines.push(Line::from(Span::styled(format!(" ← {} left the chat", user), Style::default().fg(Color::Red))));
            }
        }
    }
    f.render_widget(Paragraph::new(msg_lines), msg_area);

    // === SIDEBAR: Online Users ===
    let sidebar_area = content_layout[1];
    let mut user_lines: Vec<Line> = vec![
        Line::from(Span::styled("  ONLINE", Style::default().fg(TEXT_DIM).add_modifier(Modifier::BOLD))),
        Line::from(""),
    ];
    // Add current user first
    user_lines.push(Line::from(vec![
        Span::styled("  ● ", Style::default().fg(Color::Green)),
        Span::styled(format!("{} (you)", state.username), Style::default().fg(Color::Green)),
    ]));
    // Add other peers
    for u in state.peers.iter().filter(|u| *u != &state.username) {
        user_lines.push(Line::from(vec![
            Span::styled("  ● ", Style::default().fg(username_color(u))),
            Span::styled(u.as_str(), Style::default().fg(username_color(u))),
        ]));
    }
    f.render_widget(Paragraph::new(user_lines), sidebar_area);

    // === INPUT BOX (Plain border, Terminal-style with prompt) ===
    let input_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(TEXT_DIM));
    let input_inner = input_block.inner(main_layout[2]);
    f.render_widget(input_block, main_layout[2]);
    
    // Prompt + Input + Block Cursor
    let prompt = format!("k2-network@{}:", state.username);
    let input_line = Line::from(vec![
        Span::styled(&prompt, Style::default().fg(PRIMARY)),
        Span::styled(" ", Style::default()),
        Span::styled(&state.input, Style::default().fg(Color::White)),
        Span::styled("█", Style::default().fg(Color::White)), // Block cursor
    ]);
    f.render_widget(Paragraph::new(input_line), input_inner);

    // === FOOTER (Minimal, no border) ===
    let footer = Line::from(vec![
        Span::styled(" type \"help\" for more information", Style::default().fg(TEXT_DIM)),
    ]);
    f.render_widget(Paragraph::new(footer), main_layout[3]);
}

enum UiEvent {
    Gossip(ChatMessage),
    PeerUp(String),
    PeerDown(String),
    Progress(usize, String),
    Ready(mpsc::UnboundedSender<Vec<u8>>),
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

    let mut state = AppState::new(topic_name.clone(), username.clone());
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();

    // Spawn connection task
    let ui_tx2 = ui_tx.clone();
    let uname = username.clone();
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

        ui_tx2.send(UiEvent::Progress(70, "QUERYING TRACKER...".into())).ok();
        let query_args = Query { content: topic_hash, flags: QueryFlags { complete: false, verified: false } };
        let mut peers = vec![];
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

        let (sender, mut receiver) = topic.split();
        let sender = Arc::new(sender);

        // Broadcast join
        let join = MessageType::Join { user: uname.clone() };
        sender.broadcast(postcard::to_stdvec(&join).unwrap().into()).await.ok();

        // Channel for outgoing messages
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let s = sender.clone();
        tokio::spawn(async move { while let Some(b) = out_rx.recv().await { s.broadcast(b.into()).await.ok(); } });

        ui_tx2.send(UiEvent::Ready(out_tx)).ok();

        while let Some(ev) = receiver.next().await {
            use iroh_gossip::api::Event as GE;
            match ev {
                Ok(GE::Received(m)) => {
                    if let Ok(mt) = postcard::from_bytes::<MessageType>(&m.content) {
                        ui_tx2.send(UiEvent::Gossip(ChatMessage { timestamp: Local::now().format("%H:%M").to_string(), msg_type: mt })).ok();
                    }
                }
                Ok(GE::NeighborUp(id)) => { ui_tx2.send(UiEvent::PeerUp(id.to_string()[..8].into())).ok(); }
                Ok(GE::NeighborDown(id)) => { ui_tx2.send(UiEvent::PeerDown(id.to_string()[..8].into())).ok(); }
                _ => {}
            }
        }
    });

    let mut out_tx: Option<mpsc::UnboundedSender<Vec<u8>>> = None;

    loop {
        terminal.draw(|f| ui(f, &state))?;

        while let Ok(ev) = ui_rx.try_recv() {
            match ev {
                UiEvent::Progress(p, t) => { state.loading_step = p; state.loading_text = t; }
                UiEvent::Ready(tx) => { state.view = ViewState::Chat; state.status = "Online".into(); state.peers.insert(username.clone()); state.add_system("Connected to K2 Network"); out_tx = Some(tx); }
                UiEvent::Gossip(m) => {
                    if let MessageType::Join { ref user } = m.msg_type { state.peers.insert(user.clone()); }
                    if let MessageType::Leave { ref user } = m.msg_type { state.peers.remove(user); }
                    state.add_message(m);
                }
                UiEvent::PeerUp(id) => { state.peer_count += 1; }
                UiEvent::PeerDown(id) => { state.peer_count = state.peer_count.saturating_sub(1); }
            }
        }

        if crossterm::event::poll(Duration::from_millis(50))? {
            if let Event::Key(k) = event::read()? {
                if k.kind != KeyEventKind::Press { continue; }
                match k.code {
                    KeyCode::Esc => {
                        if let Some(ref tx) = out_tx {
                            let _ = tx.send(postcard::to_stdvec(&MessageType::Leave { user: username.clone() }).unwrap());
                        }
                        break;
                    }
                    KeyCode::Enter if state.view == ViewState::Chat => {
                        let text = state.input.trim().to_string();
                        if !text.is_empty() {
                            if text.starts_with("/") {
                                match text.as_str() {
                                    "/quit" | "/q" => { if let Some(ref tx) = out_tx { tx.send(postcard::to_stdvec(&MessageType::Leave { user: username.clone() }).unwrap()).ok(); } break; }
                                    "/clear" => { state.messages.clear(); }
                                    "/users" => { state.add_system(&format!("Online: {}", state.peers.iter().cloned().collect::<Vec<_>>().join(", "))); }
                                    "/help" => { state.add_system("/quit /clear /users /help"); }
                                    _ => { state.add_system("Unknown command. Try /help"); }
                                }
                            } else if let Some(ref tx) = out_tx {
                                let msg = MessageType::Chat { sender: username.clone(), text: text.clone() };
                                state.add_message(ChatMessage { timestamp: Local::now().format("%H:%M").to_string(), msg_type: msg.clone() });
                                tx.send(postcard::to_stdvec(&msg).unwrap()).ok();
                            }
                        }
                        state.input.clear();
                    }
                    KeyCode::Backspace => { state.input.pop(); }
                    KeyCode::Char(c) if state.view == ViewState::Chat => { state.input.push(c); }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}
