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
    let stream_name = std::env::var("BEBOP_STREAM_NAME")
        .unwrap_or_else(|_| "rave-trading".to_string());
    let bind_addr: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()
        .expect("invalid BIND_ADDR");

    // Broadcast channel from Bebop client → relay server.
    // Capacity: enough for ~1 second of frames across 5 chains at ~10 updates/sec each.
    let (bebop_tx, bebop_rx) = broadcast::channel::<bebop::RelayFrame>(2048);

    // Startup delay to avoid Bebop connection conflicts during Railway rolling deploys.
    // The old instance may still hold the single-allowed connection; a brief pause
    // lets it drain before the new instance connects.
    let startup_delay: u64 = std::env::var("STARTUP_DELAY_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    if startup_delay > 0 {
        info!("startup delay: waiting {startup_delay}s before connecting to Bebop...");
        tokio::time::sleep(std::time::Duration::from_secs(startup_delay)).await;
    }

    info!("connecting to Bebop pricing streams (5 chains)...");
    let _bebop = bebop::BebopClient::connect_all(&base_url, &authorization, &provider_id, &stream_name, bebop_tx.clone()).await?;

    info!("starting internal relay WS on {bind_addr}");
    let state = Arc::new(relay::RelayState::new(bebop_rx));
    relay::serve(bind_addr, state).await?;

    Ok(())
}
