# rave-bebop-stream-relay

## Role
Internal relay that replicates the Bebop Price API protobuf WebSocket stream into a single multiplexed internal WebSocket. All Rave services connect to this relay instead of directly to Bebop.

## Architecture
- **Language**: Rust (Tokio async runtime)
- **Ingest**: 5 persistent protobuf WebSocket connections to Bebop (one per chain: ethereum, polygon, arbitrum, base, bsc)
- **Relay**: Broadcast channel → per-client subscription filtering → batched JSON WebSocket frames
- **Deploy**: Railway Docker service, port 8080

## Key Modules
| Module | Purpose |
|--------|---------|
| `src/main.rs` | Entry point: env parsing, BebopClient init, relay serve |
| `src/bebop.rs` | Bebop protobuf WS client: connect, decode `PricingUpdate` frames, broadcast to relay |
| `src/relay.rs` | Internal multiplexed WS server: client subscription filtering, batch forwarding |
| `src/proto.rs` | Protobuf type definitions + supported chain list |
| `proto/bebop.proto` | Protobuf schema for Bebop pricing messages |

## Build & Test
```bash
cargo build --release
cargo test
```

## Configuration
| Env var | Required | Default |
|---------|----------|---------|
| `BEBOP_PRICE_STREAM_AUTH` | **yes** | — |
| `BEBOP_PRICE_API_URL` | no | `https://api.bebop.xyz/pmm` |
| `BIND_ADDR` | no | `0.0.0.0:8080` |
| `RUST_LOG` | no | `info` |

## Internal WebSocket Protocol
Connect to `ws://<host>:8080`. Receive JSON frames; send subscription JSON text messages.

See README.md for full protocol docs.
