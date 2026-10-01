use serde::Deserialize;
use std::sync::OnceLock;
use std::time::Duration;

// Bounds
pub const MIN_WINDOW_MS: u64 = 10_000;         // 10 seconds
pub const MAX_WINDOW_MS: u64 = 86_400_000;     // 24 hours
pub const DEFAULT_WINDOW_MS: u64 = 300_000;    // 5 minutes

pub const MIN_LIMIT: i32 = 1;
pub const MAX_LIMIT: i32 = 300;
pub const DEFAULT_LIMIT: i32 = 60;

pub const MAX_ACTIVE_KEYS: usize = 100;

const DEFAULT_TOML: &str = include_str!("config.default.toml");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Source {
    BinanceWs,
    Binance,
    Hyperliquid,
    Jupiter,
    Yahoo,
}

impl Source {
    pub const ALL: [Source; 5] = [
        Source::BinanceWs,
        Source::Binance,
        Source::Hyperliquid,
        Source::Jupiter,
        Source::Yahoo,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Source::BinanceWs => "binance-ws",
            Source::Binance => "binance",
            Source::Hyperliquid => "hyperliquid",
            Source::Jupiter => "jupiter",
            Source::Yahoo => "yahoo",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fallback {
    pub source: Source,
    pub id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Asset {
    pub symbol: String,
    pub source: Source,
    pub id: String,
    pub fallback: Option<Fallback>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollSecs {
    pub binance: u64,
    pub hyperliquid: u64,
    pub jupiter: u64,
    pub yahoo: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retention {
    pub ticks_hours: i64,
    pub candles_1m_days: i64,
    pub candles_1h_days: i64,
}

impl Retention {
    pub fn ticks_ms(&self) -> i64 {
        self.ticks_hours * 3_600_000
    }
    pub fn candles_1m_ms(&self) -> i64 {
        self.candles_1m_days * 86_400_000
    }
    pub fn candles_1h_ms(&self) -> i64 {
        self.candles_1h_days * 86_400_000
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub poll_secs: PollSecs,
    pub retention: Retention,
    pub assets: Vec<Asset>,
}

impl Config {
    /// Built-in defaults, overlaid with `user_toml`. Tables merge key by key; arrays are replaced.
    pub fn parse(user_toml: &str) -> Result<Self, String> {
        let mut base: toml::Table = toml::from_str(DEFAULT_TOML).expect("built-in config is valid");
        let user: toml::Table = toml::from_str(user_toml).map_err(|e| e.to_string())?;
        merge(&mut base, user);
        let mut cfg: Config = toml::Value::Table(base).try_into().map_err(|e| e.to_string())?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn load(path: Option<&str>) -> Result<Self, String> {
        match path {
            Some(p) => {
                let text = std::fs::read_to_string(p).map_err(|e| format!("{}: {}", p, e))?;
                Self::parse(&text).map_err(|e| format!("{}: {}", p, e))
            }
            None => Self::parse(""),
        }
    }

    fn validate(&mut self) -> Result<(), String> {
        if self.assets.is_empty() {
            return Err("at least one [[assets]] entry is required".into());
        }
        let p = &self.poll_secs;
        if [p.binance, p.hyperliquid, p.jupiter, p.yahoo].contains(&0) {
            return Err("poll_secs values must be >= 1".into());
        }
        let r = &self.retention;
        if r.ticks_hours < 1 || r.candles_1m_days < 1 || r.candles_1h_days < 1 {
            return Err("retention values must be >= 1".into());
        }

        let mut seen = std::collections::HashSet::new();
        for a in &mut self.assets {
            a.symbol = a.symbol.trim().to_uppercase();
            if a.symbol.is_empty() || a.id.trim().is_empty() {
                return Err("assets need a non-empty symbol and id".into());
            }
            if !seen.insert(a.symbol.clone()) {
                return Err(format!("duplicate asset symbol '{}'", a.symbol));
            }
            // Binance reports pairs uppercase; other sources' ids are case-sensitive.
            if matches!(a.source, Source::Binance | Source::BinanceWs) {
                a.id = a.id.to_uppercase();
            }
            if let Some(f) = &mut a.fallback {
                if matches!(f.source, Source::Binance | Source::BinanceWs) {
                    f.id = f.id.to_uppercase();
                }
            }
        }
        Ok(())
    }

    pub fn asset(&self, symbol: &str) -> Option<&Asset> {
        self.assets.iter().find(|a| a.symbol == symbol)
    }

    pub fn poll_interval(&self, source: Source) -> Duration {
        Duration::from_secs(match source {
            Source::BinanceWs => 1,
            Source::Binance => self.poll_secs.binance,
            Source::Hyperliquid => self.poll_secs.hyperliquid,
            Source::Jupiter => self.poll_secs.jupiter,
            Source::Yahoo => self.poll_secs.yahoo,
        })
    }

    /// Every id a source must fetch, as primary or fallback.
    pub fn ids_for(&self, source: Source) -> Vec<String> {
        let mut ids: Vec<String> = Vec::new();
        for a in &self.assets {
            let legs = std::iter::once((a.source, &a.id))
                .chain(a.fallback.iter().map(|f| (f.source, &f.id)));
            for (s, id) in legs {
                if s == source && !ids.contains(id) {
                    ids.push(id.clone());
                }
            }
        }
        ids
    }
}

fn merge(base: &mut toml::Table, user: toml::Table) {
    for (k, v) in user {
        match (base.get_mut(&k), v) {
            (Some(toml::Value::Table(b)), toml::Value::Table(u)) => merge(b, u),
            (_, v) => {
                base.insert(k, v);
            }
        }
    }
}

static CONFIG: OnceLock<Config> = OnceLock::new();

/// Install the loaded config. Call once at startup, before anything reads `get()`.
pub fn init(cfg: Config) {
    if CONFIG.set(cfg).is_err() {
        panic!("config::init called twice");
    }
}

/// Active config; the built-in default if `init` was never called (tests, key commands).
pub fn get() -> &'static Config {
    CONFIG.get_or_init(|| Config::parse("").expect("built-in config is valid"))
}

pub fn is_supported_symbol(symbol: &str) -> bool {
    get().asset(symbol).is_some()
}

pub fn supported_symbols() -> String {
    get().assets.iter().map(|a| a.symbol.as_str()).collect::<Vec<_>>().join(", ")
}

pub fn base_resolution_ms(symbol: &str) -> u64 {
    let cfg = get();
    match cfg.asset(symbol) {
        Some(a) => cfg.poll_interval(a.source).as_millis() as u64,
        None => 60_000,
    }
}

/// Age past which an asset's fallback source may overwrite its price.
pub fn fallback_after_ms(symbol: &str) -> u64 {
    std::cmp::max(2 * base_resolution_ms(symbol), 5_000)
}

pub fn clamp_effective_ttl(symbol: &str, requested_ttl_ms: Option<u64>) -> u64 {
    let base = base_resolution_ms(symbol);
    match requested_ttl_ms {
        Some(req) => std::cmp::max(req, base),
        None => base,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supported_symbols() {
        assert!(is_supported_symbol("SOL/USD"));
        assert!(is_supported_symbol("BTC/USD"));
        assert!(is_supported_symbol("PAXG/USD"));
        assert!(is_supported_symbol("SPY/USD"));
        assert!(!is_supported_symbol("SHIB/USD"));
    }

    #[test]
    fn test_smart_ttl_clamping() {
        // Fast tier: SOL/USD base resolution is 1,000ms
        assert_eq!(base_resolution_ms("SOL/USD"), 1_000);
        // Requesting 500ms on 1s feed is clamped up to 1,000ms
        assert_eq!(clamp_effective_ttl("SOL/USD", Some(500)), 1_000);
        // Requesting 5,000ms on 1s feed is respected
        assert_eq!(clamp_effective_ttl("SOL/USD", Some(5_000)), 5_000);
        // Default when None
        assert_eq!(clamp_effective_ttl("SOL/USD", None), 1_000);

        // Standard tier: BTC/USD base resolution is 60,000ms
        assert_eq!(base_resolution_ms("BTC/USD"), 60_000);
        // Requesting 1,000ms on 60s feed makes no sense, so clamped to 60,000ms
        assert_eq!(clamp_effective_ttl("BTC/USD", Some(1_000)), 60_000);
        assert_eq!(clamp_effective_ttl("BTC/USD", Some(120_000)), 120_000);
        assert_eq!(clamp_effective_ttl("BTC/USD", None), 60_000);
    }

    #[test]
    fn test_user_config_overlay() {
        let cfg = Config::parse(
            r#"
            [poll_secs]
            binance = 10

            [[assets]]
            symbol = "doge/usd"
            source = "binance"
            id = "dogeusdt"
            fallback = { source = "hyperliquid", id = "DOGE" }
            "#,
        )
        .unwrap();
        // Overridden key changes, siblings keep defaults
        assert_eq!(cfg.poll_secs.binance, 10);
        assert_eq!(cfg.poll_secs.jupiter, 60);
        assert_eq!(cfg.retention.ticks_hours, 24);
        // Assets list is replaced, symbol/id normalized
        assert_eq!(cfg.assets.len(), 1);
        assert_eq!(cfg.assets[0].symbol, "DOGE/USD");
        assert_eq!(cfg.ids_for(Source::Binance), vec!["DOGEUSDT"]);
        assert_eq!(cfg.ids_for(Source::Hyperliquid), vec!["DOGE"]);
        assert!(cfg.ids_for(Source::Jupiter).is_empty());
    }

    #[test]
    fn test_invalid_configs_rejected() {
        assert!(Config::parse("assets = []").is_err());
        assert!(Config::parse("[poll_secs]\nbinance = 0").is_err());
        assert!(Config::parse("[poll_secs]\nbinanse = 5").is_err()); // typo
        assert!(Config::parse(
            "[[assets]]\nsymbol='A'\nsource='binance'\nid='X'\n[[assets]]\nsymbol='a'\nsource='yahoo'\nid='Y'"
        )
        .is_err()); // duplicate after normalization
        assert!(Config::parse("[[assets]]\nsymbol='A'\nsource='kraken'\nid='X'").is_err());
    }

    #[test]
    fn test_default_assets_and_sources() {
        let cfg = get();
        assert_eq!(cfg.assets.len(), 11);
        assert_eq!(cfg.ids_for(Source::BinanceWs), vec!["SOLUSDT"]);
        assert_eq!(cfg.ids_for(Source::Binance), vec!["BTCUSDT", "ETHUSDT", "ZECUSDT", "PAXGUSDT", "HYPEUSDT"]);
        assert_eq!(cfg.ids_for(Source::Hyperliquid), vec!["SOL", "BTC", "ETH", "ZEC", "HYPE"]);
        assert_eq!(cfg.ids_for(Source::Jupiter).len(), 6); // PAXG backup + 5 xStocks
        assert_eq!(cfg.ids_for(Source::Yahoo), vec!["SPY", "NVDA", "GOOGL", "QQQ", "TSLA"]);
    }
}
