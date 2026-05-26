use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::bebop::RelayFrame;

/// Inbound subscription request from an internal consumer.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    #[serde(rename = "subscribe")]
    Subscribe {
        /// Chain IDs to receive. Empty = all chains.
        chain_ids: Option<Vec<u64>>,
        /// Specific bases to receive. Empty = all.
        bases: Option<Vec<String>>,
        /// Engine-style symbols: "eip155:{chain_id}:{base}-{quote}".
        /// Parsed into chain_ids + bases filter entries.
        symbols: Option<Vec<String>>,
    },
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        chain_ids: Option<Vec<u64>>,
        bases: Option<Vec<String>>,
    },
    #[serde(rename = "ping")]
    Ping,
}

/// When both `chain_ids` and `bases` are empty the client receives everything.
#[derive(Debug)]
struct ClientFilter {
    chain_ids: HashSet<u64>,
    bases: HashSet<String>,
}

impl ClientFilter {
    fn accepts(&self, frame: &RelayFrame) -> bool {
        if !self.chain_ids.is_empty() && !self.chain_ids.contains(&frame.chain_id) {
            return false;
        }
        if !self.bases.is_empty() && !self.bases.contains(&frame.base) {
            return false;
        }
        true
    }
}

/// Shared relay state.
pub struct RelayState {
    /// Broadcast channel from Bebop client.
    source_rx: Mutex<broadcast::Receiver<RelayFrame>>,
    /// Per-client filters (keyed by an opaque client id for this session).
    filters: RwLock<HashMap<usize, ClientFilter>>,
    next_id: Mutex<usize>,
}

impl RelayState {
    pub fn new(source_rx: broadcast::Receiver<RelayFrame>) -> Self {
        Self {
            source_rx: Mutex::new(source_rx),
            filters: RwLock::new(HashMap::new()),
            next_id: Mutex::new(0),
        }
    }

    async fn register(&self, filter: ClientFilter) -> usize {
        let mut id = self.next_id.lock().await;
        let client_id = *id;
        *id += 1;
        self.filters.write().await.insert(client_id, filter);
        client_id
    }

    async fn unregister(&self, client_id: usize) {
        self.filters.write().await.remove(&client_id);
    }
}

/// Start the internal WebSocket server that multiplexes Bebop data to
/// internal consumers with subscription filtering.
pub async fn serve(
    bind: SocketAddr,
    state: Arc<RelayState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(bind).await?;
    info!("internal relay WS listening on {bind}");

    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("accept error: {e}");
                continue;
            }
        };

        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, addr, state).await {
                warn!("client {addr} error: {e}");
            }
        });
    }
}

async fn handle_client(
    stream: TcpStream,
    addr: SocketAddr,
    state: Arc<RelayState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut ws_tx, mut ws_rx) = ws.split();

    // Default filter: receive everything.
    let filter = ClientFilter {
        chain_ids: HashSet::new(),
        bases: HashSet::new(),
    };
    let client_id = state.register(filter).await;
    info!("client {addr} connected (id={client_id})");

    // Spawn a task that reads from the Bebop broadcast and forwards to this
    // client. The relay source is shared; each client gets its own rx stream.
    let mut source_rx = {
        let mut guard = state.source_rx.lock().await;
        guard.resubscribe()
    };
    let state_clone = state.clone();
    let ws_tx = Arc::new(Mutex::new(ws_tx));

    let tx_for_recv = ws_tx.clone();
    let recv_handle = tokio::spawn(async move {
        loop {
            let frame = match source_rx.recv().await {
                Ok(f) => f,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!("client {client_id} lagged by {skipped} frames");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };

            // Apply filter
            let filters = state_clone.filters.read().await;
            let client_filter = match filters.get(&client_id) {
                Some(f) => f,
                None => break,
            };
            if !client_filter.accepts(&frame) {
                continue;
            }
            drop(filters);

            let json = serde_json::to_string(&frame).unwrap_or_default();
            let mut guard = tx_for_recv.lock().await;
            if guard.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    // Read subscription messages from the client.
    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(cmd) = serde_json::from_str::<ClientMessage>(&text) {
                    match cmd {
                        ClientMessage::Subscribe { chain_ids, bases, symbols } => {
                            let mut filters = state.filters.write().await;
                            if let Some(f) = filters.get_mut(&client_id) {
                                if let Some(ids) = chain_ids {
                                    f.chain_ids.extend(ids);
                                }
                                if let Some(b) = bases {
                                    f.bases.extend(b);
                                }
                                // Parse engine-style eip155 symbols into
                                // chain_ids and bases for filtering.
                                if let Some(syms) = symbols {
                                    for s in syms {
                                        if let Some((cid, base)) = parse_eip155_symbol(&s) {
                                            f.chain_ids.insert(cid);
                                            f.bases.insert(base.to_lowercase());
                                        }
                                    }
                                }
                            }
                        }
                        ClientMessage::Unsubscribe { chain_ids, bases } => {
                            let mut filters = state.filters.write().await;
                            if let Some(f) = filters.get_mut(&client_id) {
                                if let Some(ids) = chain_ids {
                                    for id in ids {
                                        f.chain_ids.remove(&id);
                                    }
                                }
                                if let Some(b) = bases {
                                    for base in b {
                                        f.bases.remove(&base);
                                    }
                                }
                            }
                        }
                        ClientMessage::Ping => {
                            let mut guard = ws_tx.lock().await;
                            let _ = guard.send(Message::Text("{\"type\":\"pong\"}".into())).await;
                        }
                    }
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }

    recv_handle.abort();
    state.unregister(client_id).await;
    info!("client {addr} disconnected (id={client_id})");
    Ok(())
}

/// Parse an engine-style symbol "eip155:{chain_id}:{base}-{quote}"
/// into (chain_id, base_address). Returns None if the format doesn't match.
fn parse_eip155_symbol(symbol: &str) -> Option<(u64, String)> {
    let rest = symbol.strip_prefix("eip155:")?;
    let (chain_str, pair) = rest.split_once(':')?;
    let chain_id: u64 = chain_str.parse().ok()?;
    let base = pair.split('-').next()?.to_string();
    if base.len() >= 42 && base.starts_with("0x") {
        Some((chain_id, base))
    } else {
        None
    }
}
