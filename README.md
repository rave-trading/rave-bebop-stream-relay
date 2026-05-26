# rave-bebop-stream-relay

Internal relay that replicates the [Bebop Price API](https://docs.bebop.xyz) pricing WebSocket
stream into a single multiplexed internal WebSocket. All Rave services connect to this relay
instead of directly to Bebop.

## Why

- **One Bebop key, one place.** Staging, production, and dev all consume the same stream.
- **All chains replicated.** Ethereum, Polygon, Arbitrum, Base, BSC — all 5 Bebop chains.
- **Efficient.** Bebop native protobuf on the ingest side; compact JSON with subscription
  filtering on the internal side. Consumers only receive chains/pairs they subscribe to.
- **Independent lifecycle.** Deploy, scale, and monitor separately from the engine.

## Architecture

```
Bebop Price API (protobuf WS, one conn per chain)
        │  eth │ polygon │ arbitrum │ base │ bsc
        ▼     ▼         ▼          ▼      ▼
┌──────────────────────────────────────────────┐
│          rave-bebop-stream-relay             │
│                                              │
│  Decode protobuf → batch frames → broadcast  │
│  Internal WS: subscription-filtered JSON     │
└──────────────────────────────────────────────┘
        │
        │  ws://relay:8080
        ▼
┌──────────────┐  ┌──────────────┐
│ rave-engine  │  │  future svc  │
│ (staging)    │  │              │
└──────────────┘  └──────────────┘
```

## Internal WebSocket protocol

Connect to `ws://<host>:8080`. Receive JSON frames:

```json
{"chain_id":1,"network":"ethereum","base":"0x...","quote":"0x...","bids":[{"price":1980.5,"size":1.5}],"asks":[{"price":1981.2,"size":0.8}],"timestamp":1716930000}
```

Send subscription commands as JSON text messages:

```json
{"type":"subscribe","chain_ids":[1,56]}
{"type":"subscribe","bases":["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"]}
{"type":"unsubscribe","chain_ids":[137]}
{"type":"ping"}
```

By default (no subscriptions), the client receives all chains and pairs.

## Configuration

| Env var | Required | Default | Description |
|---------|----------|---------|-------------|
| `BEBOP_PRICE_STREAM_AUTH` | **yes** | — | Bebop Price API authorization key |
| `BEBOP_PRICE_API_URL` | no | `https://api.bebop.xyz/pmm` | Bebop API base URL |
| `BIND_ADDR` | no | `0.0.0.0:8080` | Internal WS listen address |
| `RUST_LOG` | no | `info` | Log level |

## Deploy (Railway)

1. Create a new service in the Rave project using this repo's Dockerfile.
2. Set `BEBOP_PRICE_STREAM_AUTH` in the service environment.
3. Expose port 8080.
4. The engine connects to this service's internal Railway hostname.

## Build

```bash
cargo build --release
```

## Test

```bash
cargo test
```
