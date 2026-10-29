# Metasquare Oracle (`mso`)

[![CI](https://github.com/Niraj-Kamdar/mso/actions/workflows/ci.yml/badge.svg)](https://github.com/Niraj-Kamdar/mso/actions/workflows/ci.yml)
[![Docker](https://github.com/Niraj-Kamdar/mso/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/Niraj-Kamdar/mso/actions/workflows/docker-publish.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Container Image](https://img.shields.io/badge/ghcr.io-niraj--kamdar%2Fmso-blue?logo=docker)](https://github.com/Niraj-Kamdar/mso/pkgs/container/mso)

Self-hosted, high-performance, multi-asset oracle microservice designed for low-latency decentralized applications and prediction markets. Runs efficiently on lightweight Linux nodes (Raspberry Pi 5 with NVMe SSD) and exposes real-time streaming and REST gateways through Cloudflare Tunnels.

---

## Features

- **Multi-Tier Ingestion**:
  - **Fast Tier (1-second update)**: `SOL/USD` continuous Binance WebSocket stream with Hyperliquid fallback.
  - **Standard Tier (60-second batch)**: `BTC/USD`, `ETH/USD`, `ZEC/USD`, `PAXG/USD` (Tokenized Gold via Binance/Jupiter) and Backed xStocks (`SPY/USD`, `NVDA/USD`, `GOOG/USD`, `QQQ/USD`, `TSLA/USD` via Jupiter DEX with Yahoo Finance fallback).
  - **Ethereum L1 (opt-in)**: any Uniswap V3 pool as an on-chain TWAP oracle (e.g. `WETH/USDT`), read via `observe()` with a configurable window (`hot` 1m / `warm` 10m / `cool` 30m / `cold` 1h / custom).
- **Time-Series Rollup Pyramid**:
  - Auto-downsamples raw ticks $\rightarrow$ `candles_1m` (7-day retention) $\rightarrow$ `candles_1h` (90-day retention) $\rightarrow$ `candles_1d` (permanent archive).
  - Storage footprint stays $<30$ MB forever.
- **In-Memory Hot Caching & Smart TTL**:
  - Sub-millisecond responses directly from memory. Client `max_age_ms` is clamped to sensible lower and upper bounds.
- **API Key Quota Management**:
  - Scoped time-to-live API keys hashed with SHA-256 and cached in-memory.
  - Hard cap of 100 active keys to eliminate resource exhaustion.
- **Dual Interface**:
  - **gRPC (Tonic)**: High-speed unary and streaming price ticks on port `50051`.
  - **REST / Web (Axum)**: REST API endpoints + Scalar Interactive API Reference on port `4000`.

---

## Quickstart

### Option A: Run via Docker (Recommended)

Pre-built multi-arch images (`linux/amd64` and `linux/arm64` for Raspberry Pi) are published automatically to GitHub Packages:

```bash
docker run -d \
  --name metasquare-oracle \
  --restart unless-stopped \
  -p 4000:4000 \
  -p 50051:50051 \
  -v mso-data:/data \
  -v $(pwd)/mso.toml:/config/mso.toml:ro \
  -e MSO_CONFIG=/config/mso.toml \
  ghcr.io/niraj-kamdar/mso:latest
```

### Option B: Run via Docker Compose

```bash
# Clone the repository
git clone https://github.com/Niraj-Kamdar/mso.git
cd mso

# Start in background with persistent volume
docker compose up -d
```

### Option C: Build & Run from Source (Native)

```bash
# Prerequisites: Rust 1.80+ and protoc
# On Debian/Ubuntu/Raspberry Pi OS: sudo apt install -y protobuf-compiler

# Build release binary
cargo build --release

# Run Oracle node with production config (starts feeds, rollups, REST & gRPC servers)
./target/release/mso serve --config mso.toml
```

---

## Configuration

Assets, poll frequency, and retention come from a TOML file. With no file, the built-in
[`src/config.default.toml`](src/config.default.toml) is used (it also serves as the reference).
A custom file only needs the keys it changes; `assets` replaces the default list when present.

```toml
# mso.toml
[poll_secs]
binance = 10          # poll Binance every 10s instead of 60s

[[assets]]
symbol = "SOL/USD"
source = "binance-ws" # binance-ws | binance | hyperliquid | jupiter | yahoo | uniswap
id = "SOLUSDT"
fallback = { source = "hyperliquid", id = "SOL" }

[[assets]]
symbol = "DOGE/USD"
source = "hyperliquid"
id = "DOGE"
```

```bash
./target/release/mso serve --config mso.toml   # or MSO_CONFIG=mso.toml
# Docker: mount the file and set MSO_CONFIG
docker run ... -v $PWD/mso.toml:/config/mso.toml:ro -e MSO_CONFIG=/config/mso.toml ghcr.io/niraj-kamdar/mso:latest
```

Unknown keys, duplicate symbols, unknown sources, and zero intervals are rejected at startup.

### Ethereum mainnet via Uniswap V3 TWAP

Any Uniswap V3 pool can be served as a time-weighted average price. The asset `id` is the
**pool contract address**; the price is `token0` quoted in `token1` (for the WETH/USDT 0.05%
pool, `token0` = WETH and `token1` = USDT, so the feed reports USDT per WETH). Token
addresses and decimals are read from the chain, and the TWAP is computed exactly like the
periphery `OracleLibrary.consult` (`observe([window, 0])`, arithmetic mean tick floored,
`1.0001^tick` scaled by decimals). Chain access goes through [alloy](https://github.com/alloy-rs/alloy)
(`sol!` bindings for `token0`/`token1`/`observe`), with JSON-RPC sent over the same HTTP
client as every other feed.

```toml
[poll_secs]
uniswap = 60                # how often observe() is called

[feeds.uniswap]             # feed-specific settings
twap = "warm"               # default window: hot (1m) | warm (10m) | cool (30m) | cold (1h)
# twap = { custom = 120 }   # or any number of seconds (1 .. 604800)

[[assets]]
symbol = "WETH/USDT"
source = "uniswap"
id = "0x11b815efB8f581194ae79006d24E0d814B7697F6"   # WETH/USDT 0.05% pool
twap = "hot"                                        # optional override (uniswap is primary)
fallback = { source = "binance", id = "ETHUSDT" }

[[assets]]
symbol = "ETH/USD"
source = "binance"
id = "ETHUSDT"
# uniswap as fallback: its window lives inside the fallback table
fallback = { source = "uniswap", id = "0x11b815efB8f581194ae79006d24E0d814B7697F6", twap = { custom = 30 } }
```

The mainnet JSON-RPC endpoint comes from the environment, never the config file (it usually
embeds an API key):

```bash
export MSO_ETH_RPC_URL=https://mainnet.infura.io/v3/<key>   # any Ethereum mainnet JSON-RPC (or ETH_RPC_URL)
./target/release/mso serve --config mso.toml
```

Startup fails fast if Uniswap assets are configured without `MSO_ETH_RPC_URL` (or `ETH_RPC_URL`).
Non-loopback RPC endpoints must use HTTPS to keep credentials secure (HTTP is only permitted for local development on localhost/127.0.0.1).
Pool ids must be valid addresses (EIP-55 checksums are verified when mixed case; ids are normalized to EIP-55), `twap` is only accepted on
the leg whose source is Uniswap (top level for a primary, inside `fallback = { ... }` for a
fallback), and two legs sharing a pool must agree on its window. Longer windows are more
manipulation-resistant but lag spot; a pool must hold enough observation history for the
window (`increaseObservationCardinalityNext`), otherwise the poll logs an `OLD` revert.

To smoke-test the feed against mainnet (queries WETH/USDT on every preset):

```bash
MSO_ETH_RPC_URL=https://... cargo test live_uniswap -- --ignored --nocapture
```

---

## API Key Management

```bash
# When running via Docker:
docker exec -it metasquare-oracle mso key create --app pnl-backend --ttl-days 30

# When running locally:
./target/release/mso key create --app pnl-backend --ttl-days 30

# List all keys and quota status:
./target/release/mso key list

# Revoke a key:
./target/release/mso key revoke --id <KEY_ID>
```

---

## API Reference

All requests must provide an active API key via `x-api-key: <KEY>` header, `Authorization: Bearer <KEY>`, or `?key=<KEY>` query parameter.

| Endpoint | Method | Parameters | Description |
|---|---|---|---|
| `/health` | GET | None (Public) | Node uptime and service health check |
| `/api/v1/price` | GET | `symbol`, `max_age_ms` | Latest price tick with cache indicator |
| `/api/v1/ticks` | GET | `symbol`, `limit` (1..300) | Recent chronological ticks for charts & sparklines |
| `/api/v1/candles` | GET | `symbol`, `interval` (`1s`,`1m`,`1h`,`1d`), `limit` | Historical OHLC + TWAP candle bars |
| `/api/v1/stats` | GET | `symbol`, `window_ms` (10s..24h) | Windowed High/Low, TWAP, and return percentage |
| `/` or `/docs` | GET | None (Public) | Interactive Scalar API Reference & Docs |
| `/openapi.json` | GET | None (Public) | OpenAPI 3.1.0 JSON Specification |

---

## Deployment on Raspberry Pi 5 with Cloudflare Tunnel

### Systemd Service (`/etc/systemd/system/mso.service`)
```ini
[Unit]
Description=Metasquare Oracle (mso)
After=network.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/mso
ExecStart=/home/pi/mso/target/release/mso serve --db /home/pi/mso/oracle.db --config /home/pi/mso/mso.toml
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### Cloudflare Tunnel Config (`~/.cloudflared/config.yml`)
```yaml
tunnel: <TUNNEL_UUID>
credentials-file: /home/pi/.cloudflared/<TUNNEL_UUID>.json

ingress:
  - hostname: oracle.metasquare.tech
    service: http://localhost:4000
  - service: http_status:404
```

---

## License

This project is licensed under the [MIT License](LICENSE).
