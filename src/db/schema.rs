use rusqlite::{Connection, Result};

pub fn init_schema(conn: &Connection) -> Result<()> {
    // High-performance Pragmas for NVMe
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA cache_size = -32000; -- 32 MB cache
        PRAGMA temp_store = MEMORY;

        -- 1. Raw Ticks (24-hour retention)
        CREATE TABLE IF NOT EXISTS ticks (
            symbol TEXT NOT NULL,
            t INTEGER NOT NULL,          -- Unix epoch in milliseconds
            price REAL NOT NULL,
            source TEXT NOT NULL,
            PRIMARY KEY (symbol, t)
        );
        CREATE INDEX IF NOT EXISTS idx_ticks_query ON ticks(symbol, t DESC);

        -- 2. 1-Minute OHLC Candles (7-day retention)
        CREATE TABLE IF NOT EXISTS candles_1m (
            symbol TEXT NOT NULL,
            t INTEGER NOT NULL,          -- Epoch minute aligned (t - t % 60000)
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            twap REAL NOT NULL,
            tick_count INTEGER NOT NULL,
            PRIMARY KEY (symbol, t)
        );
        CREATE INDEX IF NOT EXISTS idx_candles_1m_query ON candles_1m(symbol, t DESC);

        -- 3. 1-Hour OHLC Candles (90-day retention)
        CREATE TABLE IF NOT EXISTS candles_1h (
            symbol TEXT NOT NULL,
            t INTEGER NOT NULL,          -- Epoch hour aligned (t - t % 3600000)
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            twap REAL NOT NULL,
            tick_count INTEGER NOT NULL,
            PRIMARY KEY (symbol, t)
        );
        CREATE INDEX IF NOT EXISTS idx_candles_1h_query ON candles_1h(symbol, t DESC);

        -- 4. 1-Day OHLC Candles (Permanent)
        CREATE TABLE IF NOT EXISTS candles_1d (
            symbol TEXT NOT NULL,
            t INTEGER NOT NULL,          -- Epoch day aligned (t - t % 86400000)
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            twap REAL NOT NULL,
            tick_count INTEGER NOT NULL,
            PRIMARY KEY (symbol, t)
        );
        CREATE INDEX IF NOT EXISTS idx_candles_1d_query ON candles_1d(symbol, t DESC);

        -- 5. Time-Scoped API Keys
        CREATE TABLE IF NOT EXISTS api_keys (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            key_hash TEXT NOT NULL UNIQUE,
            created_at INTEGER NOT NULL,
            expires_at INTEGER,          -- NULL = unlimited/permanent
            is_revoked INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_api_keys_lookup ON api_keys(key_hash);
        "#,
    )?;
    Ok(())
}
