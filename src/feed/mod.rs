pub mod binance;
pub mod hyperliquid;
pub mod jupiter;
pub mod uniswap;
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

const HTTP_TIMEOUT: Duration = Duration::from_secs(8);

async fn poll(coord: FeedCoordinator, source: Source, every: Duration) {
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .unwrap();
    let mut ticker = tokio::time::interval(every);
    // One RPC connection + pool metadata cache for the life of the feed.
    // (`serve` already validated MSO_ETH_RPC_URL, so this only fails on a bad URL.)
    let uni = match source {
        Source::Uniswap => match uniswap::rpc_url().and_then(|u| uniswap::UniswapClient::new(client.clone(), &u)) {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::error!("[uniswap] feed disabled: {}", e);
                return;
            }
        },
        _ => None,
    };

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
            Source::Uniswap => {
                let cfg = config::get();
                let pools: Vec<(String, u32)> =
                    ids.iter().map(|id| (id.clone(), cfg.twap_window(id).secs())).collect();
                uni.as_ref().expect("built above for uniswap").fetch_twaps(&pools).await
            }
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

    // ----- Uniswap V3 TWAP fetcher (alloy client against an in-process mock pool) -----

    use crate::config::TwapWindow;
    use alloy::primitives::{address, Address};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use uniswap::mock::{self, MockPool, MockState, Mode, USDT, WETH, WETH_USDT_POOL};
    use uniswap::{PoolMeta, UniswapClient};

    const WETH_USDT_TICK: i64 = -198_000; // ~2519.99 USDT per WETH
    const EXPECTED_PRICE: f64 = 2519.9921;

    fn client(url: &str) -> UniswapClient {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().unwrap();
        UniswapClient::new(http, url).unwrap()
    }

    fn pool_id(a: Address) -> String {
        a.to_checksum(None) // the form config ids are normalized to
    }

    #[tokio::test]
    async fn test_uniswap_twap_all_presets_and_custom() {
        let st = Arc::new(mock::weth_usdt_state(WETH_USDT_TICK));
        let uni = client(&mock::serve(st.clone()).await);

        let windows: Vec<TwapWindow> = TwapWindow::PRESETS.iter().copied().chain([TwapWindow::Custom(90)]).collect();
        for w in &windows {
            let out = uni.fetch_twaps(&[(pool_id(WETH_USDT_POOL), w.secs())]).await.unwrap();
            assert_eq!(out.len(), 1, "{}", w.label());
            assert_eq!(out[0].0, pool_id(WETH_USDT_POOL), "id echoed back unchanged");
            // token0 (WETH, 18 dec) quoted in token1 (USDT, 6 dec)
            assert!((out[0].1 - EXPECTED_PRICE).abs() < 0.01, "{}: {}", w.label(), out[0].1);
        }
        // observe() was asked for exactly [window, 0] each time
        let asked = st.observe_args.lock().unwrap().clone();
        assert_eq!(asked, windows.iter().map(|w| vec![w.secs(), 0]).collect::<Vec<_>>());

        // Pool metadata came from the chain and is cached after the first call
        let meta = uni.cached_meta(WETH_USDT_POOL).unwrap();
        assert_eq!(meta, PoolMeta { token0: WETH, token1: USDT, decimals0: 18, decimals1: 6 });
        let calls = st.calls.lock().unwrap().clone();
        let count = |name: &str| calls.iter().filter(|(_, n)| *n == name).count();
        assert_eq!(count("token0"), 1, "token0() fetched once");
        assert_eq!(count("token1"), 1, "token1() fetched once");
        assert_eq!(count("decimals"), 2, "decimals() once per token");
        assert_eq!(count("observe"), windows.len());
        // Chain id checked once, not per poll
        assert_eq!(st.requests.load(Ordering::SeqCst), 1 + 4 + windows.len());
    }

    #[tokio::test]
    async fn test_uniswap_price_direction_and_decimals() {
        // An inverted pool (token0 = USDT, token1 = WETH) at the mirrored tick must
        // quote WETH per USDT, i.e. the reciprocal price.
        let mut st = MockState { chain_id: 1, ..Default::default() };
        let inverted = address!("0000000000000000000000000000000000000aaa");
        let same_dec = address!("0000000000000000000000000000000000000bbb");
        let (tok_a, tok_b) = (address!("00000000000000000000000000000000000000a1"), address!("00000000000000000000000000000000000000b2"));
        st.pools.insert(inverted, MockPool { token0: USDT, token1: WETH, tick: -WETH_USDT_TICK, revert: None });
        st.pools.insert(same_dec, MockPool { token0: tok_a, token1: tok_b, tick: 0, revert: None });
        for (t, d) in [(WETH, 18), (USDT, 6), (tok_a, 8), (tok_b, 8)] {
            st.decimals.insert(t, d);
        }
        let uni = client(&mock::serve(Arc::new(st)).await);

        let pools = vec![(pool_id(inverted), 600), (pool_id(same_dec), 600)];
        let out: std::collections::HashMap<String, f64> = uni.fetch_twaps(&pools).await.unwrap().into_iter().collect();
        assert!((out[&pool_id(inverted)] - 1.0 / EXPECTED_PRICE).abs() < 1e-9, "{:?}", out);
        assert_eq!(out[&pool_id(same_dec)], 1.0);
    }

    #[tokio::test]
    async fn test_uniswap_partial_failures_are_isolated() {
        let mut st = mock::weth_usdt_state(WETH_USDT_TICK);
        let stale = address!("0000000000000000000000000000000000000bad");
        st.pools.insert(stale, MockPool { token0: WETH, token1: USDT, tick: 0, revert: Some("OLD") });
        let uni = client(&mock::serve(Arc::new(st)).await);

        // Good pool still prices while the stale one is skipped
        let pools = vec![(pool_id(WETH_USDT_POOL), 3_600), (pool_id(stale), 3_600)];
        let out = uni.fetch_twaps(&pools).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, pool_id(WETH_USDT_POOL));

        // Nothing succeeded: the error names the pool, the decoded revert reason and a hint
        let err = uni.fetch_twaps(&pools[1..]).await.unwrap_err();
        assert!(err.contains(&pool_id(stale)), "{}", err);
        assert!(err.contains("(revert: OLD)") && err.contains("observation history"), "{}", err);
        assert!(!err.contains("revert: revert"), "{}", err);

        // Non-contract address: alloy reports empty return data, not a bogus price
        let nobody = vec![("0x0000000000000000000000000000000000000001".to_string(), 60)];
        let err = uni.fetch_twaps(&nobody).await.unwrap_err();
        assert!(err.contains("returned no data"), "{}", err);

        // Malformed id, zero window: refused before any RPC call
        let err = uni.fetch_twaps(&[("0x1234".to_string(), 60)]).await.unwrap_err();
        assert!(err.contains("invalid pool address"), "{}", err);
        let err = uni.fetch_twaps(&[(pool_id(WETH_USDT_POOL), 0)]).await.unwrap_err();
        assert!(err.contains("> 0"), "{}", err);

        // Empty input is a no-op
        assert!(uni.fetch_twaps(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_uniswap_rpc_transport_failures() {
        let st = Arc::new(mock::weth_usdt_state(WETH_USDT_TICK));
        let uni = client(&mock::serve(st.clone()).await);
        let pools = vec![(pool_id(WETH_USDT_POOL), 600)];

        *st.mode.lock().unwrap() = Mode::Http500;
        let err = uni.fetch_twaps(&pools).await.unwrap_err();
        assert!(err.contains("500"), "{}", err);

        *st.mode.lock().unwrap() = Mode::NotJson;
        let err = uni.fetch_twaps(&pools).await.unwrap_err();
        assert!(err.contains("deserialization"), "{}", err);

        // Recovers once the endpoint is healthy again
        *st.mode.lock().unwrap() = Mode::Ok;
        let out = uni.fetch_twaps(&pools).await.unwrap();
        assert!((out[0].1 - EXPECTED_PRICE).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_uniswap_transport_errors_show_root_cause_without_secrets() {
        // Endpoint not listening: the operator must see *why* (the root of the error
        // chain), and never the URL, which usually embeds an API key.
        for url in ["http://127.0.0.1:1/v2/SECRET", "http://127.0.0.1:1?apikey=SECRET"] {
            let uni = client(url);
            let err = uni.fetch_twaps(&[(pool_id(WETH_USDT_POOL), 60)]).await.unwrap_err();
            assert!(err.contains(&pool_id(WETH_USDT_POOL)), "{}", err);
            assert!(err.contains("Connection refused"), "root cause missing: {}", err);
            assert!(!err.contains("SECRET"), "leaked url: {}", err);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_uniswap_wrong_chain_only_warns() {
        #[derive(Clone)]
        struct LogWriter(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for LogWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let logs = Arc::new(std::sync::Mutex::new(Vec::new()));
        let logs_clone = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || LogWriter(logs_clone.clone()))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let st = Arc::new(MockState { chain_id: 42_161, ..mock::weth_usdt_state(WETH_USDT_TICK) });
        let uni = client(&mock::serve(st).await);
        let out = uni.fetch_twaps(&[(pool_id(WETH_USDT_POOL), 600)]).await.unwrap();
        assert_eq!(out.len(), 1);

        let output = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
        let warn_line = output
            .lines()
            .find(|l| l.contains("42161") && l.contains("not Ethereum mainnet"));
        assert!(
            warn_line.is_some(),
            "expected wrong-chain message mentioning chain id 42161 and not Ethereum mainnet, got logs:\n{}",
            output
        );
        let line = warn_line.unwrap();
        assert!(
            line.contains("WARN"),
            "expected wrong-chain message to be logged at WARN level, got line: {}",
            line
        );
    }

    #[tokio::test]
    async fn test_uniswap_prices_persist_through_coordinator() {
        // End to end: mock chain -> fetcher -> record_tick -> cache + SQLite.
        let st = Arc::new(mock::weth_usdt_state(WETH_USDT_TICK));
        let uni = client(&mock::serve(st).await);
        let feed = FeedCoordinator::new(DbManager::in_memory().unwrap());

        let prices = uni.fetch_twaps(&[(pool_id(WETH_USDT_POOL), TwapWindow::Warm.secs())]).await.unwrap();
        let now = now_ms();
        for (_pool, price) in prices {
            feed.record_tick("WETH/USDT", price, now, Source::Uniswap.label());
        }
        let cached = feed.l1_cache.get("WETH/USDT").unwrap().clone();
        assert_eq!(cached.source, "uniswap");
        assert!((cached.price - EXPECTED_PRICE).abs() < 0.01);

        // record_tick persists asynchronously
        let mut stored = None;
        for _ in 0..50 {
            stored = feed.db.with_conn(|c| crate::db::queries::get_latest_tick(c, "WETH/USDT")).unwrap();
            if stored.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let tick = stored.expect("tick persisted");
        assert_eq!(tick.t, now);
        assert_eq!(tick.source, "uniswap");
        assert!((tick.price - EXPECTED_PRICE).abs() < 0.01);
    }

    /// Live smoke test against Ethereum mainnet. Needs a real RPC endpoint:
    ///   MSO_ETH_RPC_URL=https://... cargo test live_uniswap -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "hits Ethereum mainnet; set MSO_ETH_RPC_URL and run with --ignored"]
    async fn live_uniswap_weth_usdt_twap_all_presets() {
        let url = uniswap::rpc_url().expect("MSO_ETH_RPC_URL must point at an Ethereum mainnet JSON-RPC endpoint");
        let http = reqwest::Client::builder().timeout(Duration::from_secs(20)).build().unwrap();
        let uni = UniswapClient::new(http, &url).unwrap();

        // The pool's own view of its tokens must be WETH / USDT (18 / 6 decimals).
        let meta = uni.pool_meta(WETH_USDT_POOL).await.unwrap();
        println!("pool {} meta: {:?}", WETH_USDT_POOL, meta);
        assert_eq!(meta, PoolMeta { token0: WETH, token1: USDT, decimals0: 18, decimals1: 6 });

        let mut prices = Vec::new();
        for w in TwapWindow::PRESETS.iter().copied().chain([TwapWindow::Custom(120)]) {
            let out = uni.fetch_twaps(&[(pool_id(WETH_USDT_POOL), w.secs())]).await.unwrap();
            let price = out[0].1;
            println!("WETH/USDT twap {:<13} ({:>5}s) = {:.4} USDT", w.label(), w.secs(), price);
            // Sanity band: a decimals mix-up would give ~1e-9 or ~1e15 instead
            assert!(price.is_finite() && price > 1.0 && price < 1e7, "{}: {}", w.label(), price);
            prices.push(price);
        }
        // All windows describe the same market over the last hour; they should agree loosely
        let (lo, hi) = prices.iter().fold((f64::MAX, f64::MIN), |(lo, hi), &p| (lo.min(p), hi.max(p)));
        assert!(hi / lo < 1.25, "presets disagree too much: {:?}", prices);
    }
}
