pub mod binance;
pub mod hyperliquid;
pub mod jupiter;
pub mod yahoo;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::Duration;
use tracing::{info, warn};

use crate::config::{self, Source};
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

    /// Routes a price from `(source, id)` to every asset it feeds. Fallback legs
    /// only write while that asset's primary price is stale.
    pub fn ingest(&self, source: Source, id: &str, price: f64, timestamp: i64) {
        if !(price.is_finite() && price > 0.0) {
            return;
        }
        let now = now_ms();
        for a in &config::get().assets {
            if a.source == source && a.id == id {
                self.record_tick(&a.symbol, price, timestamp, source.label());
            } else if a.fallback.as_ref().is_some_and(|f| f.source == source && f.id == id)
                && self.is_stale(&a.symbol, now)
            {
                self.record_tick(&a.symbol, price, timestamp, &format!("{}-fallback", source.label()));
            }
        }
    }

    /// No price yet, or older than the symbol's fallback threshold.
    fn is_stale(&self, symbol: &str, now: i64) -> bool {
        self.l1_cache
            .get(symbol)
            .map_or(true, |e| now - e.timestamp > config::fallback_after_ms(symbol) as i64)
    }

    /// Ids worth fetching from `source` right now: every primary id, plus
    /// fallback ids whose asset is stale. Empty means skip the call.
    pub fn ids_due(&self, source: Source) -> Vec<String> {
        let now = now_ms();
        let mut ids: Vec<String> = Vec::new();
        for a in &config::get().assets {
            let id = if a.source == source {
                &a.id
            } else {
                match &a.fallback {
                    Some(f) if f.source == source && self.is_stale(&a.symbol, now) => &f.id,
                    _ => continue,
                }
            };
            if !ids.contains(id) {
                ids.push(id.clone());
            }
        }
        ids
    }

    pub fn start_all(&self) {
        info!("Starting ingestion feeds from config...");
        let cfg = config::get();
        for source in Source::ALL {
            let ids = cfg.ids_for(source);
            if ids.is_empty() {
                continue;
            }
            let every = cfg.poll_interval(source);
            info!("[{}] {} ids, every {:?}", source.label(), ids.len(), every);
            let coord = self.clone();
            match source {
                Source::BinanceWs => tokio::spawn(binance::start_ws(coord, ids)),
                _ => tokio::spawn(poll(coord, source, every)),
            };
        }
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn poll(coord: FeedCoordinator, source: Source, every: Duration) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();
    let mut ticker = tokio::time::interval(every);

    loop {
        ticker.tick().await;
        let ids = coord.ids_due(source);
        if ids.is_empty() {
            continue;
        }
        let res = match source {
            Source::Binance => binance::fetch_batch(&client, &ids).await,
            Source::Hyperliquid => hyperliquid::fetch_mids(&client, &ids).await,
            Source::Jupiter => jupiter::fetch_prices(&client, &ids).await,
            Source::Yahoo => yahoo::fetch_spots(&client, &ids).await,
            Source::BinanceWs => unreachable!("binance-ws is streamed, not polled"),
        };
        match res {
            Ok(prices) => {
                let now = now_ms();
                for (id, price) in prices {
                    coord.ingest(source, &id, price, now);
                }
            }
            Err(e) => warn!("[{}] poll failed: {}", source.label(), e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ingest_routes_primary_and_fallback() {
        let feed = FeedCoordinator::new(DbManager::in_memory().unwrap());
        let now = now_ms();
        let src = |s: &str| feed.l1_cache.get(s).map(|e| e.source.clone());

        // Fallback fills a symbol with no primary price yet
        feed.ingest(Source::Hyperliquid, "SOL", 140.0, now);
        assert_eq!(src("SOL/USD").as_deref(), Some("hyperliquid-fallback"));

        // Primary always wins; a fresh primary blocks the fallback
        feed.ingest(Source::BinanceWs, "SOLUSDT", 150.0, now);
        feed.ingest(Source::Hyperliquid, "SOL", 141.0, now);
        assert_eq!(feed.l1_cache.get("SOL/USD").unwrap().price, 150.0);

        // Once the primary is stale (> 5s for a 1s feed), fallback writes again
        feed.ingest(Source::BinanceWs, "SOLUSDT", 151.0, now - 6_000);
        feed.ingest(Source::Hyperliquid, "SOL", 142.0, now);
        assert_eq!(src("SOL/USD").as_deref(), Some("hyperliquid-fallback"));

        // Same coin can be primary for one asset only; unknown ids and bad prices are ignored
        feed.ingest(Source::Hyperliquid, "HYPE", 30.0, now);
        assert_eq!(src("HYPE/USD").as_deref(), Some("hyperliquid"));
        feed.ingest(Source::Binance, "DOGEUSDT", 0.1, now);
        feed.ingest(Source::Binance, "BTCUSDT", f64::NAN, now);
        assert!(feed.l1_cache.get("BTC/USD").is_none());
    }

    #[tokio::test]
    async fn test_fallback_ids_only_fetched_when_stale() {
        let feed = FeedCoordinator::new(DbManager::in_memory().unwrap());
        let now = now_ms();

        // Nothing cached: every fallback is due
        assert_eq!(feed.ids_due(Source::Yahoo).len(), 5);
        assert_eq!(feed.ids_due(Source::Hyperliquid), vec!["SOL", "BTC", "ETH", "ZEC", "HYPE"]);

        // Fresh primaries drop their fallback ids; primary ids are always kept
        feed.record_tick("SOL/USD", 1.0, now, "binance-ws");
        feed.record_tick("BTC/USD", 1.0, now, "binance");
        assert_eq!(feed.ids_due(Source::Hyperliquid), vec!["ETH", "ZEC", "HYPE"]);

        // All xStocks fresh from Jupiter: Yahoo has nothing due, so the call is skipped
        for sym in ["SPY/USD", "NVDA/USD", "GOOG/USD", "QQQ/USD", "TSLA/USD"] {
            feed.record_tick(sym, 1.0, now, "jupiter");
        }
        assert!(feed.ids_due(Source::Yahoo).is_empty());
    }
}
