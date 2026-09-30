pub mod binance;
pub mod hyperliquid;
pub mod jupiter;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

use crate::db::DbManager;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceTickEvent {
    pub symbol: String,
    pub price: f64,
    pub timestamp: i64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedPrice {
    pub symbol: String,
    pub price: f64,
    pub timestamp: i64,
    pub source: String,
    pub cached_at: i64,
}

#[derive(Clone)]
pub struct FeedCoordinator {
    pub l1_cache: Arc<DashMap<String, CachedPrice>>,
    pub broadcast_tx: broadcast::Sender<PriceTickEvent>,
    pub db: DbManager,
}

impl FeedCoordinator {
    pub fn new(db: DbManager) -> Self {
        let (broadcast_tx, _) = broadcast::channel(1024);
        Self {
            l1_cache: Arc::new(DashMap::new()),
            broadcast_tx,
            db,
        }
    }

    pub fn record_tick(&self, symbol: &str, price: f64, timestamp: i64, source: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let cached = CachedPrice {
            symbol: symbol.to_string(),
            price,
            timestamp,
            source: source.to_string(),
            cached_at: now,
        };

        // 1. Update L1 In-Memory Hot State
        self.l1_cache.insert(symbol.to_string(), cached);

        // 2. Broadcast to real-time streaming listeners
        let event = PriceTickEvent {
            symbol: symbol.to_string(),
            price,
            timestamp,
            source: source.to_string(),
        };
        let _ = self.broadcast_tx.send(event);

        // 3. Persist to NVMe SQLite
        let db = self.db.clone();
        let sym = symbol.to_string();
        let src = source.to_string();
        tokio::spawn(async move {
            let _ = db.with_conn(|conn| {
                crate::db::queries::insert_tick(conn, &sym, timestamp, price, &src)
            });
        });
    }

    pub fn start_all(&self) {
        info!("Starting multi-tier ingestion feeds...");
        
        // Fast Tier: SOL 1-second continuous stream
        let coord_sol = self.clone();
        tokio::spawn(async move {
            binance::start_sol_ws(coord_sol).await;
        });

        // Standard Tier: Binance batch (BTC, ETH, ZEC, PAXG) every 60s
        let coord_binance = self.clone();
        tokio::spawn(async move {
            binance::start_batch_poller(coord_binance).await;
        });

        // Standard Tier: Hyperliquid (HYPE) every 60s
        let coord_hl = self.clone();
        tokio::spawn(async move {
            hyperliquid::start_hype_poller(coord_hl).await;
        });

        // Standard Tier: Jupiter 24/7 xStocks & PAXG (with Yahoo fallback) every 60s
        let coord_jup = self.clone();
        tokio::spawn(async move {
            jupiter::start_xstocks_poller(coord_jup).await;
        });
    }
}
