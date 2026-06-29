//! Bài 14: K2 AI Skill Agent - True Minimalist Chat
//! 
//! LUỒNG CHUẨN: User -> AI -> Tool -> Result -> AI Conclusion.

use anyhow::Result;
use chrono::Local;
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame, Terminal,
};
use serde_json;
use std::{io, path::PathBuf, time::Duration};
use tokio::sync::mpsc;
use futures::StreamExt;

// K2 Core Imports
use k2_core::skills::{SkillManifest, SkillHost, SkillMetadata};

const PRIMARY: Color = Color::Rgb(0, 255, 255);
const AI_TEXT: Color = Color::Rgb(200, 160, 255);
const DIM: Color = Color::Rgb(90, 90, 100);

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "E:\\nt170\\p2p\\k2-skills")]
    skills_dir: String,
    #[arg(short, long)]
    api_key: Option<String>,
}

#[derive(Clone, Debug)]
enum Message {
    User(String),
    AI(String),
    ToolCall { name: String, action: String },
    ToolResult(String),
    System(String),
}

#[derive(Clone)]
enum InternalEvent {
    PushMsg(Message),
    UpdateLastAI(String),
    SetBusy(bool),
    ExecuteWasm { skill_id: String, action: String, reply: mpsc::Sender<String> },
}

struct AppState {
    messages: Vec<(String, Message)>, 
    input: String,
    skills_manager: SkillManager,
    api_key: String,
    is_busy: bool,
}

impl AppState {
    fn new(skills_dir: PathBuf, api_key: String) -> Self {
        Self {
            messages: vec![],
            input: String::new(),
            skills_manager: SkillManager::new(skills_dir),
            api_key,
            is_busy: false,
        }
    }
    fn add_msg(&mut self, m: Message) {
        self.messages.push((Local::now().format("%H:%M").to_string(), m));
    }
    fn update_last_ai(&mut self, text: String) {
        if let Some((_, Message::AI(ref mut t))) = self.messages.last_mut() {
            *t = text;
        } else {
            self.add_msg(Message::AI(text));
        }
    }
}

pub struct SkillManager {
    pub skills: Vec<SkillMetadata>,
    pub base_dir: PathBuf,
}

impl SkillManager {
    fn new(base_dir: PathBuf) -> Self { Self { skills: vec![], base_dir } }
    async fn scan(&mut self) -> Result<()> {
        if !self.base_dir.exists() { return Ok(()); }
        self.skills.clear();
        let entries = std::fs::read_dir(&self.base_dir)?;
        for entry in entries {
            let path = entry?.path();
            if path.is_dir() {
                let m_path = path.join("skill.json");
                if m_path.exists() {
                    let s = std::fs::read_to_string(m_path)?;
                    let m: SkillManifest = serde_json::from_str(&s)?;
                    self.skills.push(SkillMetadata { manifest: m, path });
                }
            }
        }
        Ok(())
    }
    fn run(&mut self, id: &str, action: &str) -> String {
        let meta = match self.skills.iter().find(|s| s.manifest.id == id) {
            Some(m) => m,
            None => return format!("Tool not found: {}", id),
        };
        let mut host = match SkillHost::new(meta.path.join(&meta.manifest.entrypoint)) {
            Ok(h) => h,
            Err(e) => return format!("Err Load: {}", e),
        };
        host.init().ok();
        let ev = serde_json::json!({ "event_type": "user_action", "data": action });
        host.dispatch_event(&ev.to_string()).unwrap_or_else(|e| format!("{}", e))
    }
}

// --- UI ---

fn count_lines(lines: &[Line], width: u16) -> usize {
    let mut total = 0;
    for line in lines {
        let w = line.width();
        if w == 0 { total += 1; }
        else { total += (w as f32 / width as f32).ceil() as usize; }
    }
    total
}

fn ui(f: &mut Frame, state: &AppState) {
    let chunks = Layout::default().direction(Direction::Vertical).constraints([
        Constraint::Min(1),
        Constraint::Length(3),
    ]).split(f.area());

    let mut all_lines = Vec::new();
    for (ts, msg) in &state.messages {
        match msg {
            Message::User(t) => {
                all_lines.push(Line::from(vec![Span::styled("YOU", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)), Span::styled(format!(" [{}]", ts), Style::default().fg(DIM))]));
                all_lines.push(Line::from(format!("  {}", t)));
            }
            Message::AI(t) => {
                all_lines.push(Line::from(vec![Span::styled("AI", Style::default().fg(AI_TEXT).add_modifier(Modifier::BOLD)), Span::styled(format!(" [{}]", ts), Style::default().fg(DIM))]));
                for l in t.lines() { all_lines.push(Line::from(format!("  {}", l))); }
            }
            Message::ToolCall { name, action } => {
                all_lines.push(Line::from(vec![Span::styled("  ⚡ CALL: ", Style::default().fg(PRIMARY)), Span::styled(name, Style::default().fg(Color::White)), Span::raw(format!(" ({})", action))]));
            }
            Message::ToolResult(r) => {
                all_lines.push(Line::from(vec![Span::styled("  ⚙ RESULT: ", Style::default().fg(DIM)), Span::styled(r, Style::default().fg(Color::White).add_modifier(Modifier::ITALIC))]));
            }
            Message::System(s) => {
                all_lines.push(Line::from(Span::styled(format!("  ⚡ {}", s), Style::default().fg(DIM))));
            }
        }
        all_lines.push(Line::from(""));
    }

    let chat_block = Block::default().borders(Borders::ALL).border_style(Style::default().fg(DIM)).title(" TAMBO BRIDGE ");
    let inner = chat_block.inner(chunks[0]);
    let total_lines = count_lines(&all_lines, inner.width);
    let scroll = (total_lines as u16).saturating_sub(inner.height);
    f.render_widget(Paragraph::new(all_lines).wrap(Wrap { trim: false }).block(chat_block).scroll((scroll, 0)), chunks[0]);

    let input_style = if state.is_busy { Style::default().fg(Color::Yellow) } else { Style::default().fg(PRIMARY) };
    let input_block = Block::default().borders(Borders::ALL).border_style(input_style)
        .title(if state.is_busy { " AI WORKING... " } else { " PROMPT " });
    f.render_widget(Paragraph::new(format!(" > {}", state.input)).block(input_block), chunks[1]);
}

// --- ENGINE ---

async fn agent_loop(
    prompt: String,
    api_key: String,
    skills: Vec<SkillMetadata>,
    tx: mpsc::UnboundedSender<InternalEvent>
) -> Result<()> {
    let client = reqwest::Client::new();
    let mut history = vec![
        serde_json::json!({ 
            "role": "system", 
            "content": "Bạn là Tambo AI. Sử dụng công cụ ngay khi cần. Phải trả lời tiếng Việt. Không giải thích quá dài dòng." 
        }),
        serde_json::json!({ "role": "user", "content": prompt })
    ];

    let mut tools = vec![];
    for s in &skills {
        tools.push(serde_json::json!({
            "type": "function", "function": {
                "name": s.manifest.id.replace(".", "_"), 
                "description": s.manifest.description,
                "parameters": { "type": "object", "properties": { "action": { "type": "string" } }, "required": ["action"] }
            }
        }));
    }

    loop {
        let mut turn_text = String::new();
        let mut tool_calls = vec![];
        
        tx.send(InternalEvent::SetBusy(true)).ok();
        tx.send(InternalEvent::PushMsg(Message::AI("".into()))).ok();

        let mut resp = client.post("https://api.groq.com/openai/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", api_key))
            .json(&serde_json::json!({
                "model": "openai/gpt-oss-120b", "messages": history, 
                "tools": if tools.is_empty() { serde_json::Value::Null } else { serde_json::json!(tools) }, "stream": true
            })).send().await?.bytes_stream();

        let mut buffer = String::new();
        while let Some(chunk) = resp.next().await {
            let b = chunk.unwrap_or_default();
            buffer.push_str(&String::from_utf8_lossy(&b));
            while let Some(pos) = buffer.find('\n') {
                let line = buffer[..pos].trim().to_string();
                buffer = buffer[pos+1..].to_string();
                if !line.starts_with("data: ") || line == "data: [DONE]" { continue; }
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&line[6..]) {
                    if let Some(delta) = json["choices"][0].get("delta") {
                        if let Some(c) = delta["content"].as_str() {
                            turn_text.push_str(c);
                            tx.send(InternalEvent::UpdateLastAI(turn_text.clone())).ok();
                        }
                        if let Some(calls) = delta.get("tool_calls") {
                            for c in calls.as_array().unwrap() {
                                let idx = c["index"].as_u64().unwrap_or(0) as usize;
                                while tool_calls.len() <= idx { tool_calls.push(serde_json::json!({ "id": "", "function": { "name": "", "arguments": "" } })); }
                                if let Some(id) = c.get("id").and_then(|v| v.as_str()) { tool_calls[idx]["id"] = serde_json::json!(id); }
                                if let Some(n) = c.get("function").and_then(|f| f.get("name")).and_then(|v| v.as_str()) {
                                    let old = tool_calls[idx]["function"]["name"].as_str().unwrap_or("");
                                    tool_calls[idx]["function"]["name"] = serde_json::json!(format!("{}{}", old, n));
                                }
                                if let Some(a) = c.get("function").and_then(|f| f.get("arguments")).and_then(|v| v.as_str()) {
                                    let old = tool_calls[idx]["function"]["arguments"].as_str().unwrap_or("");
                                    tool_calls[idx]["function"]["arguments"] = serde_json::json!(format!("{}{}", old, a));
                                }
                            }
                        }
                    }
                }
            }
        }

        history.push(serde_json::json!({ "role": "assistant", "content": turn_text, "tool_calls": if tool_calls.is_empty() { serde_json::Value::Null } else { serde_json::json!(tool_calls) } }));
        if tool_calls.is_empty() { break; }

        for call in tool_calls {
            let id = call["id"].as_str().unwrap_or_default().to_string();
            let name = call["function"]["name"].as_str().unwrap_or_default().replace("_", ".");
            let args: serde_json::Value = serde_json::from_str(call["function"]["arguments"].as_str().unwrap_or("{}")).unwrap_or_default();
            let action = args["action"].as_str().unwrap_or("ping").to_string();

            tx.send(InternalEvent::PushMsg(Message::ToolCall { name: name.clone(), action: action.clone() })).ok();
            let (res_tx, mut res_rx) = mpsc::channel(1);
            tx.send(InternalEvent::ExecuteWasm { skill_id: name.clone(), action, reply: res_tx }).ok();
            let res = res_rx.recv().await.unwrap_or_else(|| "Error".to_string());
            tx.send(InternalEvent::PushMsg(Message::ToolResult(res.clone()))).ok();
            history.push(serde_json::json!({ "role": "tool", "tool_call_id": id, "name": name.replace(".", "_"), "content": res }));
        }
    }
    tx.send(InternalEvent::SetBusy(false)).ok();
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let api_key = cli.api_key.or_else(|| std::env::var("GROQ_API_KEY").ok()).unwrap_or_else(|| "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".to_string());

    enable_raw_mode()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;

    let mut state = AppState::new(PathBuf::from(cli.skills_dir), api_key);
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(state.skills_manager.scan())?;
    
    state.add_msg(Message::System("Tambo Bridge Online. Type 'check ping'.".into()));

    let (tx, mut rx) = mpsc::unbounded_channel::<InternalEvent>();

    loop {
        terminal.draw(|f| ui(f, &state))?;

        while let Ok(ev) = rx.try_recv() {
            match ev {
                InternalEvent::PushMsg(m) => state.add_msg(m),
                InternalEvent::UpdateLastAI(t) => state.update_last_ai(t),
                InternalEvent::SetBusy(b) => state.is_busy = b,
                InternalEvent::ExecuteWasm { skill_id, action, reply } => {
                    let r = state.skills_manager.run(&skill_id, &action);
                    let _ = reply.try_send(r);
                }
            }
        }

        if event::poll(Duration::from_millis(20))? {
            if let Event::Key(k) = event::read()? {
                if k.kind != KeyEventKind::Press { continue; }
                match k.code {
                    KeyCode::Esc => break,
                    KeyCode::Backspace => { state.input.pop(); }
                    KeyCode::Char(c) => { state.input.push(c); }
                    KeyCode::Enter => {
                        let t = state.input.trim().to_string();
                        if !t.is_empty() && !state.is_busy {
                            state.add_msg(Message::User(t.clone()));
                            state.is_busy = true;
                            let (itx, ak, sk) = (tx.clone(), state.api_key.clone(), state.skills_manager.skills.clone());
                            std::thread::spawn(move || {
                                let rt = tokio::runtime::Runtime::new().unwrap();
                                rt.block_on(agent_loop(t, ak, sk, itx)).ok();
                            });
                        }
                        state.input.clear();
                    }
                    _ => {}
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    Ok(())
}
