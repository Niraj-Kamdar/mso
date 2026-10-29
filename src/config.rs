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
    Uniswap,
}

impl Source {
    pub const ALL: [Source; 6] = [
        Source::BinanceWs,
        Source::Binance,
        Source::Hyperliquid,
        Source::Jupiter,
        Source::Yahoo,
        Source::Uniswap,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Source::BinanceWs => "binance-ws",
            Source::Binance => "binance",
            Source::Hyperliquid => "hyperliquid",
            Source::Jupiter => "jupiter",
            Source::Yahoo => "yahoo",
            Source::Uniswap => "uniswap",
        }
    }
}

/// Uniswap V3 TWAP look-back window (the `secondsAgo` passed to `observe()`).
/// Presets are named by how quickly the price reacts; `Custom` is in seconds.
///
/// TOML: `twap = "hot"` | `"warm"` | `"cool"` | `"cold"` | `{ custom = 120 }`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TwapWindow {
    /// 1 minute
    Hot,
    /// 10 minutes
    Warm,
    /// 30 minutes
    Cool,
    /// 1 hour
    Cold,
    /// User-defined number of seconds (1 ..= MAX_TWAP_SECS)
    Custom(u32),
}

/// Upper bound for a custom TWAP window: 7 days. Anything longer is almost
/// certainly a typo, and no mainnet pool keeps that much observation history.
pub const MAX_TWAP_SECS: u32 = 7 * 86_400;

impl TwapWindow {
    /// Named windows, shortest first (used by tests and the live smoke test).
    #[cfg_attr(not(test), allow(dead_code))]
    pub const PRESETS: [TwapWindow; 4] = [
        TwapWindow::Hot,
        TwapWindow::Warm,
        TwapWindow::Cool,
        TwapWindow::Cold,
    ];

    /// Window length in seconds.
    pub fn secs(self) -> u32 {
        match self {
            TwapWindow::Hot => 60,
            TwapWindow::Warm => 600,
            TwapWindow::Cool => 1_800,
            TwapWindow::Cold => 3_600,
            TwapWindow::Custom(s) => s,
        }
    }

    pub fn label(self) -> String {
        match self {
            TwapWindow::Hot => "hot".into(),
            TwapWindow::Warm => "warm".into(),
            TwapWindow::Cool => "cool".into(),
            TwapWindow::Cold => "cold".into(),
            TwapWindow::Custom(s) => format!("custom({}s)", s),
        }
    }

    fn validate(self) -> Result<(), String> {
        match self {
            TwapWindow::Custom(0) => Err("custom twap window must be >= 1 second".into()),
            TwapWindow::Custom(s) if s > MAX_TWAP_SECS => {
                Err(format!("custom twap window must be <= {} seconds", MAX_TWAP_SECS))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fallback {
    pub source: Source,
    pub id: String,
    /// Uniswap fallbacks only: overrides `[feeds.uniswap].twap` for this pool.
    pub twap: Option<TwapWindow>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Asset {
    pub symbol: String,
    pub source: Source,
    pub id: String,
    pub fallback: Option<Fallback>,
    /// Uniswap primaries only: overrides `[feeds.uniswap].twap` for this pool.
    /// (A Uniswap *fallback* carries its own `twap` inside `fallback = { ... }`.)
    pub twap: Option<TwapWindow>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollSecs {
    pub binance: u64,
    pub hyperliquid: u64,
    pub jupiter: u64,
    pub yahoo: u64,
    pub uniswap: u64,
}

/// Source-specific settings that don't fit the generic asset/poll model.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Feeds {
    pub uniswap: UniswapFeed,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniswapFeed {
    /// Default TWAP window for every Uniswap pool (assets may override).
    pub twap: TwapWindow,
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
    pub feeds: Feeds,
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
        if [p.binance, p.hyperliquid, p.jupiter, p.yahoo, p.uniswap].contains(&0) {
            return Err("poll_secs values must be >= 1".into());
        }
        self.feeds.uniswap.twap.validate().map_err(|e| format!("feeds.uniswap: {}", e))?;
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
            // Uniswap ids are V3 pool contract addresses, stored in EIP-55 form so
            // the same pool always compares equal however it was written.
            if a.source == Source::Uniswap {
                a.id = normalize_eth_address(&a.id).map_err(|e| format!("asset '{}': {}", a.symbol, e))?;
            }
            if let Some(f) = &mut a.fallback {
                if f.source == Source::Uniswap {
                    f.id = normalize_eth_address(&f.id)
                        .map_err(|e| format!("asset '{}' fallback: {}", a.symbol, e))?;
                }
            }
            if let Some(w) = a.twap {
                if a.source != Source::Uniswap {
                    return Err(format!(
                        "asset '{}': 'twap' is only valid when source = \"uniswap\" \
                         (for a uniswap fallback, put it inside `fallback = {{ ... }}`)",
                        a.symbol
                    ));
                }
                w.validate().map_err(|e| format!("asset '{}': {}", a.symbol, e))?;
            }
            if let Some(f) = &a.fallback {
                if let Some(w) = f.twap {
                    if f.source != Source::Uniswap {
                        return Err(format!(
                            "asset '{}' fallback: 'twap' is only valid when source = \"uniswap\"",
                            a.symbol
                        ));
                    }
                    w.validate().map_err(|e| format!("asset '{}' fallback: {}", a.symbol, e))?;
                }
            }
        }
        // One pool = one TWAP window. Two legs sharing a pool must agree on it,
        // since the feed fetches each pool once.
        let mut windows: std::collections::HashMap<&str, TwapWindow> = Default::default();
        for a in &self.assets {
            for (src, id, twap) in uniswap_legs(a) {
                if src != Source::Uniswap {
                    continue;
                }
                let w = twap.unwrap_or(self.feeds.uniswap.twap);
                match windows.insert(id, w) {
                    Some(prev) if prev != w => {
                        return Err(format!(
                            "uniswap pool {} is configured with conflicting twap windows ({} vs {})",
                            id,
                            prev.label(),
                            w.label()
                        ));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// TWAP window for a Uniswap pool id: the override on whichever leg (primary
    /// or fallback) names that pool, else the feed default.
    pub fn twap_window(&self, pool: &str) -> TwapWindow {
        self.assets
            .iter()
            .flat_map(uniswap_legs)
            .find_map(|(src, id, twap)| (src == Source::Uniswap && id == pool).then_some(twap).flatten())
            .unwrap_or(self.feeds.uniswap.twap)
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
            Source::Uniswap => self.poll_secs.uniswap,
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

/// Both legs of an asset as `(source, id, twap override)`.
fn uniswap_legs(a: &Asset) -> impl Iterator<Item = (Source, &str, Option<TwapWindow>)> {
    std::iter::once((a.source, a.id.as_str(), a.twap))
        .chain(a.fallback.iter().map(|f| (f.source, f.id.as_str(), f.twap)))
}

/// Parses an EVM address (via alloy) and returns its canonical EIP-55 form.
/// Mixed-case input must carry a valid checksum, which catches most typos;
/// all-lowercase / all-uppercase input has no checksum and is accepted.
pub fn normalize_eth_address(raw: &str) -> Result<String, String> {
    use alloy::primitives::Address;
    let raw = raw.trim();
    let body = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .ok_or_else(|| format!("'{}' is not a 0x-prefixed Ethereum address", raw))?;
    let mixed_case = body.contains(|c: char| c.is_ascii_uppercase()) && body.contains(|c: char| c.is_ascii_lowercase());
    let addr = if mixed_case {
        Address::parse_checksummed(format!("0x{}", body), None)
            .map_err(|e| format!("'{}' has an invalid EIP-55 checksum ({})", raw, e))?
    } else {
        body.parse::<Address>()
            .map_err(|e| format!("'{}' is not a valid Ethereum address ({})", raw, e))?
    };
    Ok(addr.to_checksum(None))
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
        // Uniswap is opt-in: the built-in config has no pools but sane feed defaults
        assert!(cfg.ids_for(Source::Uniswap).is_empty());
        assert_eq!(cfg.poll_secs.uniswap, 60);
        assert_eq!(cfg.feeds.uniswap.twap, TwapWindow::Warm);
        assert_eq!(cfg.poll_interval(Source::Uniswap), Duration::from_secs(60));
        assert!(Source::ALL.contains(&Source::Uniswap));
        assert_eq!(Source::Uniswap.label(), "uniswap");
    }

    // WETH/USDT 0.05% pool: EIP-55 (the normalized form) and lowercase.
    const WETH_USDT: &str = "0x11b815efB8f581194ae79006d24E0d814B7697F6";
    const WETH_USDT_LOWER: &str = "0x11b815efb8f581194ae79006d24e0d814b7697f6";
    // WETH/USDT 0.30% pool, written lowercase in configs below.
    const WETH_USDT_03: &str = "0x4e68Ccd3E89f51C3074ca5072bbAC773960dFa36";
    const WETH_USDT_03_LOWER: &str = "0x4e68ccd3e89f51c3074ca5072bbac773960dfa36";
    // USDC/WETH 0.05% pool, written uppercase in configs below.
    const USDC_WETH: &str = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640";

    #[test]
    fn test_twap_window_presets_and_custom() {
        assert_eq!(TwapWindow::Hot.secs(), 60);
        assert_eq!(TwapWindow::Warm.secs(), 600);
        assert_eq!(TwapWindow::Cool.secs(), 1_800);
        assert_eq!(TwapWindow::Cold.secs(), 3_600);
        assert_eq!(TwapWindow::Custom(45).secs(), 45);
        assert_eq!(TwapWindow::PRESETS, [TwapWindow::Hot, TwapWindow::Warm, TwapWindow::Cool, TwapWindow::Cold]);
        // Presets are strictly increasing
        assert!(TwapWindow::PRESETS.windows(2).all(|w| w[0].secs() < w[1].secs()));
        assert_eq!(TwapWindow::Custom(90).label(), "custom(90s)");
        assert_eq!(TwapWindow::Cold.label(), "cold");
    }

    #[test]
    fn test_uniswap_feed_config_from_toml() {
        // Every preset spelled as in the docs
        for (text, want) in [
            ("hot", TwapWindow::Hot),
            ("warm", TwapWindow::Warm),
            ("cool", TwapWindow::Cool),
            ("cold", TwapWindow::Cold),
        ] {
            let cfg = Config::parse(&format!("[feeds.uniswap]\ntwap = \"{}\"", text)).unwrap();
            assert_eq!(cfg.feeds.uniswap.twap, want, "{}", text);
        }
        // Custom window as an inline table, plus the poll interval
        let cfg = Config::parse(
            r#"
            [poll_secs]
            uniswap = 15

            [feeds.uniswap]
            twap = { custom = 120 }
            "#,
        )
        .unwrap();
        assert_eq!(cfg.feeds.uniswap.twap, TwapWindow::Custom(120));
        assert_eq!(cfg.feeds.uniswap.twap.secs(), 120);
        assert_eq!(cfg.poll_interval(Source::Uniswap), Duration::from_secs(15));
        // Other feeds untouched by the overlay
        assert_eq!(cfg.poll_secs.binance, 60);
    }

    #[test]
    fn test_uniswap_assets_use_pool_address_ids() {
        let cfg = Config::parse(&format!(
            r#"
            [feeds.uniswap]
            twap = "cool"

            [[assets]]
            symbol = "weth/usdt"
            source = "uniswap"
            id = "{}"
            fallback = {{ source = "binance", id = "ethusdt" }}

            [[assets]]
            symbol = "USDC/WETH"
            source = "uniswap"
            id = "0X88E6A0C2DDD26FEEB64F039A2C41296FCB3F5640"
            twap = "hot"

            [[assets]]
            symbol = "ETH/USD"
            source = "binance"
            id = "ETHUSDT"
            fallback = {{ source = "uniswap", id = "{}", twap = {{ custom = 300 }} }}
            "#,
            WETH_USDT_LOWER, WETH_USDT_03_LOWER
        ))
        .unwrap();

        // Ids are validated and normalized to EIP-55, whatever case they were written in
        assert_eq!(cfg.ids_for(Source::Uniswap), vec![WETH_USDT, USDC_WETH, WETH_USDT_03]);
        assert_eq!(cfg.asset("WETH/USDT").unwrap().id, WETH_USDT);
        let fb = cfg.asset("ETH/USD").unwrap().fallback.as_ref().unwrap();
        assert_eq!(fb.id, WETH_USDT_03);
        assert_eq!(fb.twap, Some(TwapWindow::Custom(300)));
        assert_eq!(cfg.asset("ETH/USD").unwrap().twap, None);

        // Window resolution: the override on the leg naming the pool beats the feed default
        assert_eq!(cfg.twap_window(USDC_WETH), TwapWindow::Hot);
        assert_eq!(cfg.twap_window(WETH_USDT_03), TwapWindow::Custom(300));
        assert_eq!(cfg.twap_window(WETH_USDT), TwapWindow::Cool);
        assert_eq!(cfg.twap_window("0x0000000000000000000000000000000000000000"), TwapWindow::Cool);
        assert_eq!(base_resolution_ms("SOL/USD"), 1_000); // unrelated symbols unaffected

        // A pool shared by a primary and a fallback leg is fetched once with the agreed window
        let cfg = Config::parse(&format!(
            "[[assets]]\nsymbol='A/B'\nsource='uniswap'\nid='{0}'\ntwap='cold'\n\
             [[assets]]\nsymbol='C/D'\nsource='binance'\nid='X'\nfallback={{source='uniswap',id='{0}',twap='cold'}}",
            WETH_USDT
        ))
        .unwrap();
        assert_eq!(cfg.twap_window(WETH_USDT), TwapWindow::Cold);
        assert_eq!(cfg.ids_for(Source::Uniswap).len(), 1);
    }

    #[test]
    fn test_invalid_uniswap_configs_rejected() {
        let asset = |id: &str, extra: &str| {
            format!("[[assets]]\nsymbol='WETH/USDT'\nsource='uniswap'\nid='{}'\n{}", id, extra)
        };
        let err = |toml: &str| Config::parse(toml).unwrap_err();

        // Pool id must be an EVM address
        assert!(err(&asset("ETHUSDT", "")).contains("0x-prefixed"));
        assert!(err(&asset("0x11b815ef", "")).contains("not a valid Ethereum address"));
        assert!(err(&asset("0x11b815efb8f581194ae79006d24e0d814b7697f6aa", "")).contains("not a valid Ethereum address"));
        assert!(err(&asset("0xggb815efb8f581194ae79006d24e0d814b7697f6", "")).contains("not a valid Ethereum address"));
        // Mixed case with a wrong checksum (one letter flipped) = probable typo
        assert!(err(&asset("0x11b815efb8f581194ae79006d24E0d814B7697F6", "")).contains("EIP-55"));
        // ...but all-lowercase / all-uppercase / correct checksum are fine
        assert!(Config::parse(&asset(WETH_USDT_LOWER, "")).is_ok());
        assert!(Config::parse(&asset(&WETH_USDT_LOWER.to_uppercase(), "")).is_ok());
        assert!(Config::parse(&asset(WETH_USDT, "")).is_ok());
        // Fallback ids are validated too
        assert!(err("[[assets]]\nsymbol='A'\nsource='binance'\nid='X'\nfallback={source='uniswap',id='nope'}")
            .contains("fallback"));

        // twap windows
        assert!(err("[feeds.uniswap]\ntwap = 'lukewarm'").contains("unknown variant"));
        assert!(!err("[feeds.uniswap]\ntwap = 600").is_empty());
        assert!(err("[feeds.uniswap]\ntwap = { custom = 0 }").contains(">= 1"));
        assert!(err(&format!("[feeds.uniswap]\ntwap = {{ custom = {} }}", MAX_TWAP_SECS + 1)).contains("<="));
        assert!(!err("[feeds.uniswap]\ntwap = { custom = -5 }").is_empty());
        assert!(err("[feeds.uniswap]\ntwap = { minutes = 5 }").contains("unknown variant"));
        assert!(err("[feeds.uniswap]\nwindow = 'hot'").contains("unknown field"));
        assert!(err("[feeds.sushiswap]\ntwap = 'hot'").contains("unknown field"));
        assert!(err("[poll_secs]\nuniswap = 0").contains("poll_secs"));
        assert!(err(&asset(WETH_USDT, "twap = { custom = 0 }")).contains("WETH/USDT"));

        // twap override must sit on the leg that uses uniswap
        assert!(err("[[assets]]\nsymbol='ETH/USD'\nsource='binance'\nid='ETHUSDT'\ntwap='hot'")
            .contains("only valid when source"));
        // ...so a top-level twap with a uniswap *fallback* is rejected with a pointer to the fix
        let misplaced = format!(
            "[[assets]]\nsymbol='ETH/USD'\nsource='binance'\nid='ETHUSDT'\n\
             fallback={{source='uniswap',id='{}'}}\ntwap='hot'",
            WETH_USDT
        );
        assert!(err(&misplaced).contains("inside `fallback"));
        // ...and a twap inside a non-uniswap fallback is rejected too
        let misplaced = format!(
            "[[assets]]\nsymbol='WETH/USDT'\nsource='uniswap'\nid='{}'\n\
             fallback={{source='binance',id='ETHUSDT',twap='hot'}}",
            WETH_USDT
        );
        assert!(err(&misplaced).contains("fallback: 'twap' is only valid"));
        // Fallback windows are range-checked like primary ones
        let bad = format!(
            "[[assets]]\nsymbol='ETH/USD'\nsource='binance'\nid='ETHUSDT'\n\
             fallback={{source='uniswap',id='{}',twap={{custom=0}}}}",
            WETH_USDT
        );
        assert!(err(&bad).contains("fallback: custom twap window must be >= 1"));

        // One pool cannot be read with two different windows
        let conflict = format!(
            "[[assets]]\nsymbol='A/B'\nsource='uniswap'\nid='{0}'\ntwap='hot'\n\
             [[assets]]\nsymbol='C/D'\nsource='uniswap'\nid='{0}'\ntwap='cold'",
            WETH_USDT
        );
        assert!(err(&conflict).contains("conflicting twap"));
        // Implicit default vs explicit different override also conflicts
        let conflict = format!(
            "[[assets]]\nsymbol='A/B'\nsource='uniswap'\nid='{0}'\n\
             [[assets]]\nsymbol='C/D'\nsource='uniswap'\nid='{0}'\ntwap='cold'",
            WETH_USDT
        );
        assert!(err(&conflict).contains("conflicting twap"));
        // Primary leg vs fallback leg on the same pool must agree as well
        let conflict = format!(
            "[[assets]]\nsymbol='A/B'\nsource='uniswap'\nid='{0}'\ntwap='hot'\n\
             [[assets]]\nsymbol='C/D'\nsource='binance'\nid='X'\nfallback={{source='uniswap',id='{0}',twap='cold'}}",
            WETH_USDT
        );
        assert!(err(&conflict).contains("conflicting twap"));
        // Same explicit window on both is fine
        let same = format!(
            "[[assets]]\nsymbol='A/B'\nsource='uniswap'\nid='{0}'\ntwap='cold'\n\
             [[assets]]\nsymbol='C/D'\nsource='uniswap'\nid='{0}'\ntwap='cold'",
            WETH_USDT
        );
        assert!(Config::parse(&same).is_ok());
    }

    #[test]
    fn test_eth_address_normalization() {
        // Any accepted spelling normalizes to the EIP-55 form
        assert_eq!(normalize_eth_address(WETH_USDT).unwrap(), WETH_USDT);
        assert_eq!(normalize_eth_address(&format!("  {}  ", WETH_USDT_LOWER)).unwrap(), WETH_USDT);
        assert_eq!(normalize_eth_address(&WETH_USDT_LOWER.to_uppercase()).unwrap(), WETH_USDT);
        // WETH / USDT mainnet contracts (checksums cross-checked with an independent keccak)
        assert_eq!(
            normalize_eth_address("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2").unwrap(),
            "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        );
        assert_eq!(
            normalize_eth_address("0xdac17f958d2ee523a2206206994597c13d831ec7").unwrap(),
            "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        );
        // Failure cases
        assert!(normalize_eth_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc3").unwrap_err().contains("EIP-55"));
        assert!(normalize_eth_address("").is_err());
        assert!(normalize_eth_address("0x").is_err());
        assert!(normalize_eth_address("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2").unwrap_err().contains("0x-prefixed"));
    }

    #[test]
    fn test_production_mso_toml_valid() {
        let content = std::fs::read_to_string("mso.toml").unwrap();
        let cfg = Config::parse(&content).unwrap();
        assert!(cfg.assets.len() >= 11);
        assert_eq!(cfg.poll_secs.jupiter, 2);
        assert_eq!(cfg.poll_secs.binance, 5);
        assert_eq!(cfg.poll_secs.yahoo, 5);
        assert_eq!(cfg.poll_secs.uniswap, 30);
        // All assets have fallback configured
        for a in &cfg.assets {
            assert!(a.fallback.is_some());
        }
    }
}
