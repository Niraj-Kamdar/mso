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

# Run Oracle node (starts feeds, rollups, REST & gRPC servers)
./target/release/mso serve
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
ExecStart=/home/pi/mso/target/release/mso serve --db /home/pi/mso/oracle.db
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
