use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::TimeZone;
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use serde::ser::{Serialize, SerializeStruct, Serializer};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::proto::{self, BebopChain, BEBOP_CHAINS};

/// Wire format: matches the engine's `DepthFrame::Snapshot` tagged-enum JSON.
/// This allows the engine to consume relay frames with zero mapping via
/// `JsonDepthWsClient`.
#[derive(Debug, Clone)]
pub struct RelayFrame {
    pub provider_id: String,
    pub chain_id: u64,
    pub network: String,
    pub base: String,  // 0x-prefixed hex
    pub quote: String, // 0x-prefixed hex
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    /// Unix timestamp in milliseconds.
    pub timestamp_ms: u64,
}

impl Serialize for RelayFrame {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("RelayFrame", 7)?;
        st.serialize_field("type", "snapshot")?;
        st.serialize_field("provider_id", &self.provider_id)?;
        st.serialize_field(
            "asset",
            &format!("eip155:{}:{}-{}", self.chain_id, self.base, self.quote),
        )?;
        st.serialize_field("bids", &self.bids)?;
        st.serialize_field("asks", &self.asks)?;
        // Emit ISO 8601 for chrono::DateTime<Utc> deserialization.
        let dt = chrono::Utc
            .timestamp_millis_opt(self.timestamp_ms as i64)
            .single()
            .unwrap_or_else(chrono::Utc::now);
        st.serialize_field("timestamp", &dt)?;
        st.end()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Level {
    /// Serialized as a decimal string so the engine's `rust_decimal::Decimal`
    /// deserialization works without float→string round-trip issues.
    #[serde(serialize_with = "serialize_f32_as_string")]
    pub price: f32,
    #[serde(serialize_with = "serialize_f32_as_string")]
    pub size: f32,
}

fn serialize_f32_as_string<S: Serializer>(value: &f32, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&value.to_string())
}

/// Connected Bebop stream for a single chain.
struct ChainStream {
    chain: &'static BebopChain,
    provider_id: String,
    tx: broadcast::Sender<RelayFrame>,
    #[allow(dead_code)]
    url: String,
}

pub struct BebopClient {
    streams: Vec<Arc<ChainStream>>,
    _shutdown_tx: mpsc::Sender<()>,
}

impl BebopClient {
    /// Connect to all Bebop chains and start streaming into `tx`.
    /// Spawns one background task per chain.
    pub async fn connect_all(
        base_url: &str,
        authorization: &str,
        provider_id: &str,
        tx: broadcast::Sender<RelayFrame>,
    ) -> Result<Self, String> {
        let (shutdown_tx, _shutdown_rx) = mpsc::channel::<()>(1);
        let mut streams = Vec::with_capacity(BEBOP_CHAINS.len());

        for chain in BEBOP_CHAINS {
            let ws_url = format!(
                "{}://{}/{}/v3/pricing?authorization={}&format=protobuf&name=rave-relay&gasless=false&expiry_type=standard",
                if base_url.starts_with("https") { "wss" } else { "ws" },
                base_url.trim_start_matches("https://").trim_start_matches("http://").trim_end_matches('/'),
                chain.network,
                authorization
            );

            let stream = Arc::new(ChainStream {
                chain,
                provider_id: provider_id.to_string(),
                tx: tx.clone(),
                url: ws_url.clone(),
            });

            let stream_clone = stream.clone();
            tokio::spawn(async move {
                run_chain_stream(stream_clone).await;
            });

            streams.push(stream);
            info!("spawned Bebop stream for {} (chain {})", chain.name, chain.chain_id);
        }

        Ok(Self {
            streams,
            _shutdown_tx: shutdown_tx,
        })
    }
}

async fn run_chain_stream(stream: Arc<ChainStream>) {
    let chain_id = stream.chain.chain_id;
    let network = stream.chain.network;
    let provider_id = stream.provider_id.clone();
    let url = &stream.url;

    loop {
        info!("connecting to Bebop pricing stream for {network}...");
        let (mut ws, _resp) = match connect_async(url.as_str()).await {
            Ok(conn) => {
                info!("connected to Bebop pricing stream for {network}");
                conn
            }
            Err(e) => {
                error!("Bebop {network} connect failed: {e}; retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let mut last_batch = Instant::now();
        let batch_interval = Duration::from_millis(100);
        // Accumulate frames per batch interval to reduce internal WS message count.
        let mut batch: Vec<RelayFrame> = Vec::with_capacity(64);

        loop {
            let msg = match ws.next().await {
                Some(Ok(Message::Binary(data))) => data,
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                    continue;
                }
                Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None => {
                    warn!("Bebop {network} WS closed; reconnecting...");
                    break;
                }
                Some(Ok(other)) => {
                    warn!("Bebop {network} unexpected message: {other:?}");
                    continue;
                }
                Some(Err(e)) => {
                    error!("Bebop {network} WS error: {e}; reconnecting...");
                    break;
                }
            };

            // Decode protobuf
            match proto::PricingUpdate::decode(msg.as_ref()) {
                Ok(update) => {
                    let now = Instant::now();
                    for pair in update.pairs {
                        let frame = decode_pair(chain_id, network, &provider_id, &pair);
                        batch.push(frame);
                    }
                    // Send batch every interval
                    if now.duration_since(last_batch) >= batch_interval && !batch.is_empty() {
                        for frame in batch.drain(..) {
                            let _ = stream.tx.send(frame);
                        }
                        last_batch = now;
                    }
                }
                Err(e) => {
                    warn!("Bebop {network} protobuf decode error: {e}");
                }
            }
        }

        // Flush remaining batch on disconnect
        for frame in batch.drain(..) {
            let _ = stream.tx.send(frame);
        }
    }
}

fn decode_pair(chain_id: u64, network: &str, provider_id: &str, pair: &proto::PriceUpdate) -> RelayFrame {
    let base = pair.base.as_deref().map(bytes_to_hex).unwrap_or_default();
    let quote = pair.quote.as_deref().map(bytes_to_hex).unwrap_or_default();
    let bids = levels_from_flat(&pair.bids);
    let asks = levels_from_flat(&pair.asks);
    RelayFrame {
        provider_id: provider_id.to_string(),
        chain_id,
        network: network.to_string(),
        base,
        quote,
        bids,
        asks,
        timestamp_ms: pair.last_update_ts.unwrap_or(0),
    }
}

fn levels_from_flat(flat: &[f32]) -> Vec<Level> {
    flat.chunks(2)
        .filter(|chunk| chunk.len() == 2 && chunk[1] > 0.0)
        .map(|chunk| Level {
            price: chunk[0],
            size: chunk[1],
        })
        .collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levels_from_flat() {
        let flat = vec![1.0, 10.0, 2.0, 20.0, 3.0, 0.0];
        let levels = levels_from_flat(&flat);
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0].price, 1.0);
        assert_eq!(levels[0].size, 10.0);
        assert_eq!(levels[1].price, 2.0);
        assert_eq!(levels[1].size, 20.0);
    }

    #[test]
    fn test_bytes_to_hex() {
        assert_eq!(
            bytes_to_hex(&[0xaa, 0xbb, 0xcc]),
            "0xaabbcc"
        );
    }

    #[test]
    fn test_bebop_chains_count() {
        assert_eq!(BEBOP_CHAINS.len(), 5);
    }
}
