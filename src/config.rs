pub const SUPPORTED_SYMBOLS: &[&str] = &[
    "SOL/USD",
    "BTC/USD",
    "ETH/USD",
    "HYPE/USD",
    "ZEC/USD",
    "PAXG/USD",
    "SPY/USD",
    "NVDA/USD",
    "GOOG/USD",
    "QQQ/USD",
    "TSLA/USD",
];

pub const FAST_TIER_SYMBOLS: &[&str] = &["SOL/USD"];

// Bounds
pub const MIN_WINDOW_MS: u64 = 10_000;         // 10 seconds
pub const MAX_WINDOW_MS: u64 = 86_400_000;     // 24 hours
pub const DEFAULT_WINDOW_MS: u64 = 300_000;    // 5 minutes

pub const MIN_LIMIT: i32 = 1;
pub const MAX_LIMIT: i32 = 300;
pub const DEFAULT_LIMIT: i32 = 60;

pub const MAX_ACTIVE_KEYS: usize = 100;

// Retention windows in milliseconds
pub const RETENTION_TICKS_MS: i64 = 86_400_000;         // 24 hours
pub const RETENTION_CANDLES_1M_MS: i64 = 7 * 86_400_000; // 7 days
pub const RETENTION_CANDLES_1H_MS: i64 = 90 * 86_400_000; // 90 days

pub fn is_supported_symbol(symbol: &str) -> bool {
    SUPPORTED_SYMBOLS.contains(&symbol)
}

pub fn base_resolution_ms(symbol: &str) -> u64 {
    if FAST_TIER_SYMBOLS.contains(&symbol) {
        1_000 // 1 second for SOL
    } else {
        60_000 // 60 seconds for everything else
    }
}

pub fn clamp_effective_ttl(symbol: &str, requested_ttl_ms: Option<u64>) -> u64 {
    let base = base_resolution_ms(symbol);
    match requested_ttl_ms {
        Some(req) => std::cmp::max(req, base),
        None => base,
    }
}
