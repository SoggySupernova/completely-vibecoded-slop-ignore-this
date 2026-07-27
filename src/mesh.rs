//! The transport-agnostic mesh core: "flood all connections with
//! deduplication". Both the peripheral role (src/peripheral.rs, handles
//! *incoming* BLE connections from other centrals) and the central role
//! (src/central.rs, handles connections *we* initiate to other peripherals)
//! register their links here as plain byte-sinks. Everything above the raw
//! bytes -- dedup, TTL, fan-out -- lives in one place so the flood logic is
//! identical regardless of which BLE role produced/consumes a given link.

use crate::protocol::Packet;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

/// How many message IDs we remember for de-dup purposes. Old entries are
/// evicted both by count (VecDeque cap) and by age (SEEN_TTL), whichever
/// comes first -- keeps memory bounded on long-running nodes.
const SEEN_CAPACITY: usize = 2048;
const SEEN_TTL: Duration = Duration::from_secs(300);

pub type LinkId = u64;

/// A handle each BLE role uses to push freshly-received bytes into the
/// mesh, and to receive an outgoing byte stream to write/notify back out.
pub struct LinkHandle {
    pub id: LinkId,
    pub outgoing_rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

struct SeenEntry {
    id: [u8; 16],
    at: Instant,
}

struct MeshState {
    links: HashMap<LinkId, mpsc::UnboundedSender<Vec<u8>>>,
    seen_set: std::collections::HashSet<[u8; 16]>,
    seen_order: VecDeque<SeenEntry>,
    next_link_id: LinkId,
}

pub struct Mesh {
    state: Mutex<MeshState>,
    pub nickname: String,
    /// Fired for every new (non-duplicate) chat message, so the UI layer
    /// can print it. Cloned to whoever calls `subscribe_ui`.
    ui_tx: mpsc::UnboundedSender<UiEvent>,
}

#[derive(Debug)]
pub enum UiEvent {
    Message { sender: String, text: String },
    LinkCountChanged(usize),
}

impl Mesh {
    pub fn new(nickname: String) -> (std::sync::Arc<Mesh>, mpsc::UnboundedReceiver<UiEvent>) {
        let (ui_tx, ui_rx) = mpsc::unbounded_channel();
        let mesh = std::sync::Arc::new(Mesh {
            state: Mutex::new(MeshState {
                links: HashMap::new(),
                seen_set: std::collections::HashSet::new(),
                seen_order: VecDeque::new(),
                next_link_id: 0,
            }),
            nickname,
            ui_tx,
        });
        (mesh, ui_rx)
    }

    /// Register a new link (either direction). Returns a LinkHandle whose
    /// `outgoing_rx` yields encoded packets that the caller must actually
    /// write out over its BLE connection (as a GATT write or notify).
    pub async fn register_link(&self) -> LinkHandle {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut state = self.state.lock().await;
        let id = state.next_link_id;
        state.next_link_id += 1;
        state.links.insert(id, tx);
        let count = state.links.len();
        drop(state);
        let _ = self.ui_tx.send(UiEvent::LinkCountChanged(count));
        LinkHandle {
            id,
            outgoing_rx: rx,
        }
    }

    pub async fn unregister_link(&self, id: LinkId) {
        let mut state = self.state.lock().await;
        state.links.remove(&id);
        let count = state.links.len();
        drop(state);
        let _ = self.ui_tx.send(UiEvent::LinkCountChanged(count));
    }

    /// Call this whenever raw bytes arrive on any link (peripheral write,
    /// or central notification). Handles decode, dedup, local display, and
    /// re-flooding to every *other* link.
    pub async fn handle_incoming(&self, from_link: LinkId, data: &[u8]) {
        let packet = match Packet::decode(data) {
            Ok(p) => p,
            Err(_) => return, // malformed packet, silently drop
        };

        let is_new = {
            let mut state = self.state.lock().await;
            self.prune_seen_locked(&mut state);
            if state.seen_set.contains(&packet.msg_id) {
                false
            } else {
                state.seen_set.insert(packet.msg_id);
                state.seen_order.push_back(SeenEntry {
                    id: packet.msg_id,
                    at: Instant::now(),
                });
                true
            }
        };

        if !is_new {
            return; // already seen -> drop, this is the whole "dedup" step
        }

        let _ = self.ui_tx.send(UiEvent::Message {
            sender: packet.sender.clone(),
            text: packet.payload.clone(),
        });

        if let Some(forwarded) = packet.with_decremented_ttl() {
            self.broadcast(&forwarded, Some(from_link)).await;
        }
    }

    /// Called for a message we authored locally (typed into our own chat
    /// input). Marks it seen immediately so it's not re-processed if it
    /// loops back to us, then floods to every link.
    pub async fn send_local(&self, text: &str) {
        let packet = Packet::new(&self.nickname, text);
        {
            let mut state = self.state.lock().await;
            state.seen_set.insert(packet.msg_id);
            state.seen_order.push_back(SeenEntry {
                id: packet.msg_id,
                at: Instant::now(),
            });
        }
        self.broadcast(&packet, None).await;
    }

    async fn broadcast(&self, packet: &Packet, exclude: Option<LinkId>) {
        let encoded = match packet.encode() {
            Ok(b) => b,
            Err(_) => return, // e.g. too long; drop rather than crash the mesh
        };
        let state = self.state.lock().await;
        for (id, tx) in state.links.iter() {
            if Some(*id) == exclude {
                continue;
            }
            let _ = tx.send(encoded.clone());
        }
    }

    fn prune_seen_locked(&self, state: &mut MeshState) {
        let now = Instant::now();
        while let Some(front) = state.seen_order.front() {
            if now.duration_since(front.at) > SEEN_TTL || state.seen_order.len() > SEEN_CAPACITY {
                let removed = state.seen_order.pop_front().unwrap();
                state.seen_set.remove(&removed.id);
            } else {
                break;
            }
        }
    }

    pub async fn link_count(&self) -> usize {
        self.state.lock().await.links.len()
    }
}
