mod bebop;
mod proto;
mod relay;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let base_url = std::env::var("BEBOP_PRICE_API_URL")
        .unwrap_or_else(|_| "https://api.bebop.xyz/pmm".to_string());
    let authorization = std::env::var("BEBOP_PRICE_STREAM_AUTH")
        .map_err(|_| "BEBOP_PRICE_STREAM_AUTH environment variable is required")?;
    let provider_id = std::env::var("PROVIDER_ID")
        .unwrap_or_else(|_| "bebop-price-stream".to_string());
    let bind_addr: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()
        .expect("invalid BIND_ADDR");

    // Broadcast channel from Bebop client → relay server.
    // Capacity: enough for ~1 second of frames across 5 chains at ~10 updates/sec each.
    let (bebop_tx, bebop_rx) = broadcast::channel::<bebop::RelayFrame>(2048);

    info!("connecting to Bebop pricing streams (5 chains)...");
    let _bebop = bebop::BebopClient::connect_all(&base_url, &authorization, &provider_id, bebop_tx.clone()).await?;

    info!("starting internal relay WS on {bind_addr}");
    let state = Arc::new(relay::RelayState::new(bebop_rx));
    relay::serve(bind_addr, state).await?;

    Ok(())
}
