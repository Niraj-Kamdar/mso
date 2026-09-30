use crate::config::{
    is_supported_symbol, DEFAULT_LIMIT, DEFAULT_WINDOW_MS, MAX_LIMIT, MAX_WINDOW_MS, MIN_LIMIT,
    MIN_WINDOW_MS,
};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Unsupported symbol: '{0}'. Supported: SOL/USD, BTC/USD, ETH/USD, HYPE/USD, ZEC/USD, PAXG/USD, SPY/USD, NVDA/USD, GOOG/USD, QQQ/USD, TSLA/USD")]
    InvalidSymbol(String),
    #[error("Invalid interval: '{0}'. Supported: 1s, 1m, 1h, 1d")]
    InvalidInterval(String),
}

pub fn validate_symbol(symbol: &str) -> Result<String, ValidationError> {
    let clean = symbol.trim().to_uppercase();
    if is_supported_symbol(&clean) {
        Ok(clean)
    } else {
        Err(ValidationError::InvalidSymbol(symbol.to_string()))
    }
}

pub fn clamp_window_ms(window_ms: Option<u64>) -> u64 {
    match window_ms {
        Some(w) => w.clamp(MIN_WINDOW_MS, MAX_WINDOW_MS),
        None => DEFAULT_WINDOW_MS,
    }
}

pub fn clamp_limit(limit: Option<i32>) -> i32 {
    match limit {
        Some(l) => l.clamp(MIN_LIMIT, MAX_LIMIT),
        None => DEFAULT_LIMIT,
    }
}

pub fn validate_interval(interval: &str) -> Result<String, ValidationError> {
    match interval.trim().to_lowercase().as_str() {
        "1s" => Ok("ticks".to_string()),
        "1m" => Ok("candles_1m".to_string()),
        "1h" => Ok("candles_1h".to_string()),
        "1d" => Ok("candles_1d".to_string()),
        _ => Err(ValidationError::InvalidInterval(interval.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_validation() {
        assert_eq!(validate_symbol("sol/usd").unwrap(), "SOL/USD");
        assert_eq!(validate_symbol("  BTC/USD  ").unwrap(), "BTC/USD");
        assert!(validate_symbol("DOGE/USD").is_err());
        assert!(validate_symbol("'; DROP TABLE ticks;--").is_err());
    }

    #[test]
    fn test_window_clamping() {
        assert_eq!(clamp_window_ms(Some(500)), MIN_WINDOW_MS); // Clamped up to 10s
        assert_eq!(clamp_window_ms(Some(999_999_999)), MAX_WINDOW_MS); // Clamped down to 24h
        assert_eq!(clamp_window_ms(None), DEFAULT_WINDOW_MS);
    }

    #[test]
    fn test_limit_clamping() {
        assert_eq!(clamp_limit(Some(-5)), MIN_LIMIT); // Clamped to 1
        assert_eq!(clamp_limit(Some(1000)), MAX_LIMIT); // Clamped to 300
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
    }

    #[test]
    fn test_interval_validation() {
        assert_eq!(validate_interval("1s").unwrap(), "ticks");
        assert_eq!(validate_interval("1m").unwrap(), "candles_1m");
        assert_eq!(validate_interval("1h").unwrap(), "candles_1h");
        assert_eq!(validate_interval("1d").unwrap(), "candles_1d");
        assert!(validate_interval("5m").is_err());
    }
}
