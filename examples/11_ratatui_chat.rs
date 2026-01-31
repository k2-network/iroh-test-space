//! Bài 11: K2-Network Chat with Ratatui TUI
//!
//! Features:
//! - ASCII Art Banner with Whale Logo
//! - User list sidebar  
//! - Message history with timestamps
//! - Color-coded usernames
//! - Status bar showing connection info
//! - Commands: /quit, /users, /clear

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
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Frame, Terminal,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    io,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, Mutex};

// --- CONSTANTS ---
const DEFAULT_TRACKER: &str = "71853750efc1219d7976639087c5fb25cf8d4b49f6d509366f2e094a3f781623";

const WHALE_LOGO: &str = r#"        .
       ":"
     ___:____     |"\/"|
   ,'        `.    \  /
   |  O        \___/  |
 ~^~^~^~^~^~^~^~^~^~^~^~"#;

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

struct AppState {
    messages: Vec<ChatMessage>,
    input: String,
    peers: HashSet<String>,
    status: String,
    topic: String,
    username: String,
    should_quit: bool,
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
            should_quit: false,
        }
    }

    fn add_message(&mut self, msg: ChatMessage) {
        self.messages.push(msg);
        if self.messages.len() > 100 {
            self.messages.remove(0);
        }
    }

    fn add_system(&mut self, text: &str) {
        self.add_message(ChatMessage {
            timestamp: Local::now().format("%H:%M").to_string(),
            msg_type: MessageType::Chat {
                sender: "SYS".to_string(),
                text: text.to_string(),
            },
        });
    }
}

fn topic_to_id(topic: &str) -> TopicId {
    TopicId::from_bytes(blake3::hash(topic.as_bytes()).into())
}

fn topic_to_hash(topic: &str) -> HashAndFormat {
    let topic_id = topic_to_id(topic);
    let hash = iroh_blobs::Hash::from_bytes(topic_id.as_bytes().clone());
    HashAndFormat::raw(hash)
}

fn parse_tracker_id(s: &str) -> Result<EndpointId> {
    let bytes = hex::decode(s)?;
    let arr: [u8; 32] = bytes.try_into().map_err(|_| anyhow::anyhow!("Invalid tracker ID"))?;
    Ok(EndpointId::from_bytes(&arr)?)
}

fn username_color(name: &str) -> Color {
    let hash = blake3::hash(name.as_bytes());
    let bytes = hash.as_bytes();
    let colors = [
        Color::Cyan, Color::Green, Color::Yellow, Color::Blue,
        Color::Magenta, Color::LightCyan, Color::LightGreen, Color::LightYellow,
    ];
    colors[(bytes[0] as usize) % colors.len()]
}

fn ui(f: &mut Frame, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(7),  // Banner
            Constraint::Min(5),     // Chat area
            Constraint::Length(3),  // Input
            Constraint::Length(1),  // Status bar
        ])
        .split(f.area());

    // Banner
    let banner = Paragraph::new(format!(
        "{}\n  K2-NETWORK CHAT | Topic: {} | You: {}",
        WHALE_LOGO, state.topic, state.username
    ))
    .style(Style::default().fg(Color::Cyan));
    f.render_widget(banner, chunks[0]);

    // Chat area - messages + user list
    let chat_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(30), Constraint::Length(18)])
        .split(chunks[1]);

    // Messages
    let msg_height = chat_chunks[0].height.saturating_sub(2) as usize;
    let skip = state.messages.len().saturating_sub(msg_height);
    
    let messages: Vec<ListItem> = state
        .messages
        .iter()
        .skip(skip)
        .map(|m| {
            let (prefix, content, color) = match &m.msg_type {
                MessageType::Chat { sender, text } => {
                    let c = if sender == "SYS" {
                        Color::DarkGray
                    } else if sender == &state.username {
                        Color::Green
                    } else {
                        username_color(sender)
                    };
                    (sender.clone(), text.clone(), c)
                }
                MessageType::Join { user } => {
                    ("+".to_string(), format!("{} joined", user), Color::Yellow)
                }
                MessageType::Leave { user } => {
                    ("-".to_string(), format!("{} left", user), Color::Red)
                }
            };

            ListItem::new(Line::from(vec![
                Span::styled(format!("{} ", m.timestamp), Style::default().fg(Color::DarkGray)),
                Span::styled(format!("[{}] ", prefix), Style::default().fg(color).add_modifier(Modifier::BOLD)),
                Span::raw(content),
            ]))
        })
        .collect();

    let messages_list = List::new(messages)
        .block(Block::default().borders(Borders::ALL).title(" Chat "));
    f.render_widget(messages_list, chat_chunks[0]);

    // User list
    let mut users: Vec<ListItem> = state
        .peers
        .iter()
        .map(|u| {
            ListItem::new(Span::styled(
                format!(" ● {}", u),
                Style::default().fg(username_color(u)),
            ))
        })
        .collect();
    
    // Always show self
    if !state.peers.contains(&state.username) {
        users.insert(0, ListItem::new(Span::styled(
            format!(" ● {} (me)", state.username),
            Style::default().fg(Color::Green),
        )));
    }

    let users_list = List::new(users)
        .block(Block::default().borders(Borders::ALL).title(format!(" Online ")));
    f.render_widget(users_list, chat_chunks[1]);

    // Input box
    let input = Paragraph::new(state.input.as_str())
        .style(Style::default().fg(Color::White))
        .block(Block::default().borders(Borders::ALL).title(" Message (Enter to send, Esc to quit) "));
    f.render_widget(input, chunks[2]);

    // Cursor
    f.set_cursor_position((
        chunks[2].x + state.input.len() as u16 + 1,
        chunks[2].y + 1,
    ));

    // Status bar
    let status = Paragraph::new(format!(" {} ", state.status))
        .style(Style::default().fg(Color::Black).bg(Color::Cyan));
    f.render_widget(status, chunks[3]);
}

// Channel for sending messages from gossip to UI
enum UiEvent {
    GossipMessage(ChatMessage),
    PeerUp(String),
    PeerDown(String),
    StatusUpdate(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, cli).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = result {
        eprintln!("Error: {:?}", err);
    }

    Ok(())
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    cli: Cli,
) -> Result<()> {
    let topic_name = cli.topic.clone();
    let username = cli.name.clone();
    let topic_id = topic_to_id(&topic_name);
    let topic_hash = topic_to_hash(&topic_name);
    let tracker_id = parse_tracker_id(DEFAULT_TRACKER)?;

    let mut state = AppState::new(topic_name.clone(), username.clone());
    state.add_system("Initializing...");

    // Channel for UI events from background tasks
    let (ui_tx, mut ui_rx) = mpsc::unbounded_channel::<UiEvent>();

    // Create endpoint
    let secret_key = SecretKey::generate(&mut rand::rng());
    let my_id = secret_key.public();

    state.add_system(&format!("Node ID: {}...", &my_id.to_string()[..12]));
    state.status = "Connecting...".to_string();

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

    state.add_system("Connected to relay");

    // Query tracker
    let query_args = Query {
        content: topic_hash,
        flags: QueryFlags { complete: false, verified: false },
    };

    let mut peer_node_ids = vec![];
    match query(&endpoint, tracker_id, query_args).await {
        Ok(announces) => {
            for ann in announces {
                if ann.host != EndpointId::from(my_id) {
                    peer_node_ids.push(ann.host);
                }
            }
            state.add_system(&format!("Found {} peers from tracker", peer_node_ids.len()));
        }
        Err(e) => {
            state.add_system(&format!("Tracker error: {}", e));
        }
    }

    // Announce self
    let announce_data = Announce {
        host: EndpointId::from(my_id),
        content: topic_hash,
        kind: AnnounceKind::Complete,
        timestamp: AbsoluteTime::now(),
    };
    let signed_announce = SignedAnnounce::new(announce_data, &secret_key)?;
    let _ = announce(&endpoint, tracker_id, signed_announce).await;

    // Setup gossip
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let _router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    // Join gossip
    use rand::seq::SliceRandom;
    let mut rng = rand::rng();
    peer_node_ids.shuffle(&mut rng);
    let targets = peer_node_ids.into_iter().take(10).collect::<Vec<_>>();

    let gossip_topic = if targets.is_empty() {
        state.add_system("No peers found, waiting for others...");
        gossip.subscribe(topic_id, vec![]).await?
    } else {
        state.add_system(&format!("Joining with {} peers...", targets.len()));
        match tokio::time::timeout(Duration::from_secs(10), gossip.subscribe_and_join(topic_id, targets)).await {
            Ok(Ok(topic)) => {
                state.add_system("Successfully joined!");
                topic
            },
            Ok(Err(e)) => {
                state.add_system(&format!("Join error: {}", e));
                gossip.subscribe(topic_id, vec![]).await?
            },
            Err(_) => {
                state.add_system("Timeout, subscribing alone");
                gossip.subscribe(topic_id, vec![]).await?
            },
        }
    };

    state.status = format!("Online | {} peers", state.peers.len());
    state.peers.insert(username.clone());

    let (sender, mut receiver) = gossip_topic.split();
    let sender = Arc::new(sender);

    // Broadcast join
    let join_msg = MessageType::Join { user: username.clone() };
    let bytes = postcard::to_stdvec(&join_msg)?;
    sender.broadcast(bytes.into()).await?;

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

    // Message receiver task
    let ui_tx_clone = ui_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = receiver.next().await {
            use iroh_gossip::api::Event;
            match event {
                Ok(Event::Received(msg)) => {
                    if let Ok(msg_type) = postcard::from_bytes::<MessageType>(&msg.content) {
                        let _ = ui_tx_clone.send(UiEvent::GossipMessage(ChatMessage {
                            timestamp: Local::now().format("%H:%M").to_string(),
                            msg_type,
                        }));
                    }
                }
                Ok(Event::NeighborUp(id)) => {
                    let _ = ui_tx_clone.send(UiEvent::PeerUp(id.to_string()[..8].to_string()));
                }
                Ok(Event::NeighborDown(id)) => {
                    let _ = ui_tx_clone.send(UiEvent::PeerDown(id.to_string()[..8].to_string()));
                }
                _ => {}
            }
        }
    });

    // Main loop
    let my_username = username.clone();
    loop {
        // Draw
        terminal.draw(|f| ui(f, &state))?;

        // Process UI events from gossip
        while let Ok(ev) = ui_rx.try_recv() {
            match ev {
                UiEvent::GossipMessage(msg) => {
                    if let MessageType::Join { ref user } = msg.msg_type {
                        state.peers.insert(user.clone());
                    } else if let MessageType::Leave { ref user } = msg.msg_type {
                        state.peers.remove(user);
                    }
                    state.add_message(msg);
                }
                UiEvent::PeerUp(id) => {
                    state.add_system(&format!("Peer connected: {}", id));
                }
                UiEvent::PeerDown(id) => {
                    state.add_system(&format!("Peer disconnected: {}", id));
                }
                UiEvent::StatusUpdate(s) => {
                    state.status = s;
                }
            }
        }

        // Update status
        state.status = format!("Online | {} peers", state.peers.len());

        // Handle keyboard input (poll with short timeout)
        if crossterm::event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                // Only handle key press events (not release)
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                
                match key.code {
                    KeyCode::Esc => {
                        // Quit
                        let leave_msg = MessageType::Leave { user: my_username.clone() };
                        let bytes = postcard::to_stdvec(&leave_msg)?;
                        let _ = sender.broadcast(bytes.into()).await;
                        break;
                    }
                    KeyCode::Enter => {
                        if !state.input.trim().is_empty() {
                            let text = state.input.trim().to_string();
                            
                            if text.starts_with("/") {
                                match text.as_str() {
                                    "/quit" | "/q" => {
                                        let leave_msg = MessageType::Leave { user: my_username.clone() };
                                        let bytes = postcard::to_stdvec(&leave_msg)?;
                                        let _ = sender.broadcast(bytes.into()).await;
                                        break;
                                    }
                                    "/clear" | "/c" => {
                                        state.messages.clear();
                                    }
                                    "/users" | "/u" => {
                                        let users: Vec<_> = state.peers.iter().cloned().collect();
                                        state.add_system(&format!("Users: {}", users.join(", ")));
                                    }
                                    "/help" | "/h" => {
                                        state.add_system("Commands: /quit /clear /users /help");
                                    }
                                    _ => {
                                        state.add_system(&format!("Unknown: {} (try /help)", text));
                                    }
                                }
                            } else {
                                // Send chat
                                let msg = MessageType::Chat {
                                    sender: my_username.clone(),
                                    text: text.clone(),
                                };
                                let bytes = postcard::to_stdvec(&msg)?;
                                
                                state.add_message(ChatMessage {
                                    timestamp: Local::now().format("%H:%M").to_string(),
                                    msg_type: msg,
                                });
                                
                                let _ = sender.broadcast(bytes.into()).await;
                            }
                        }
                        state.input.clear();
                    }
                    KeyCode::Backspace => {
                        state.input.pop();
                    }
                    KeyCode::Char(c) => {
                        state.input.push(c);
                    }
                    _ => {}
                }
            }
        }

        if state.should_quit {
            break;
        }
    }

    Ok(())
}
