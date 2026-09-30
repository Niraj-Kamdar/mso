use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickRecord {
    pub symbol: String,
    pub t: i64,
    pub price: f64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleRecord {
    pub timestamp: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub twap: f64,
    pub tick_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsRecord {
    pub high: f64,
    pub low: f64,
    pub twap: f64,
    pub tick_count: i32,
    pub open_price: f64,
    pub current_price: f64,
    pub return_pct: f64,
    pub since_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub id: String,
    pub app_name: String,
    pub key_hash: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub is_revoked: bool,
}

pub fn insert_tick(conn: &Connection, symbol: &str, t: i64, price: f64, source: &str) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO ticks (symbol, t, price, source) VALUES (?1, ?2, ?3, ?4)",
        params![symbol, t, price, source],
    )?;
    Ok(())
}

pub fn get_latest_tick(conn: &Connection, symbol: &str) -> Result<Option<TickRecord>> {
    let mut stmt = conn.prepare(
        "SELECT symbol, t, price, source FROM ticks WHERE symbol = ?1 ORDER BY t DESC LIMIT 1",
    )?;
    let mut rows = stmt.query(params![symbol])?;

    if let Some(row) = rows.next()? {
        Ok(Some(TickRecord {
            symbol: row.get(0)?,
            t: row.get(1)?,
            price: row.get(2)?,
            source: row.get(3)?,
        }))
    } else {
        Ok(None)
    }
}

pub fn get_recent_ticks(conn: &Connection, symbol: &str, limit: i32) -> Result<Vec<TickRecord>> {
    let mut stmt = conn.prepare(
        "SELECT symbol, t, price, source FROM ticks WHERE symbol = ?1 ORDER BY t DESC LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![symbol, limit], |row| {
        Ok(TickRecord {
            symbol: row.get(0)?,
            t: row.get(1)?,
            price: row.get(2)?,
            source: row.get(3)?,
        })
    })?;

    let mut list = Vec::new();
    for r in rows {
        list.push(r?);
    }
    // Reverse to chronological order (oldest -> newest)
    list.reverse();
    Ok(list)
}

pub fn get_candles(conn: &Connection, table: &str, symbol: &str, limit: i32) -> Result<Vec<CandleRecord>> {
    let sql = format!(
        "SELECT t, open, high, low, close, twap, tick_count FROM {} WHERE symbol = ?1 ORDER BY t DESC LIMIT ?2",
        table
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![symbol, limit], |row| {
        Ok(CandleRecord {
            timestamp: row.get(0)?,
            open: row.get(1)?,
            high: row.get(2)?,
            low: row.get(3)?,
            close: row.get(4)?,
            twap: row.get(5)?,
            tick_count: row.get(6)?,
        })
    })?;

    let mut list = Vec::new();
    for r in rows {
        list.push(r?);
    }
    list.reverse();
    Ok(list)
}

pub fn get_stats(conn: &Connection, symbol: &str, window_ms: u64) -> Result<Option<StatsRecord>> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let since = now - (window_ms as i64);

    let mut stmt = conn.prepare(
        r#"
        SELECT 
            MAX(price) AS high,
            MIN(price) AS low,
            AVG(price) AS twap,
            COUNT(*) AS tick_count,
            (SELECT price FROM ticks WHERE symbol = ?1 AND t >= ?2 ORDER BY t ASC LIMIT 1) AS open_price,
            (SELECT price FROM ticks WHERE symbol = ?1 ORDER BY t DESC LIMIT 1) AS current_price
        FROM ticks
        WHERE symbol = ?1 AND t >= ?2
        "#,
    )?;

    let mut rows = stmt.query(params![symbol, since])?;

    if let Some(row) = rows.next()? {
        let count: i32 = row.get(3)?;
        if count == 0 {
            return Ok(None);
        }
        let high: f64 = row.get(0)?;
        let low: f64 = row.get(1)?;
        let twap: f64 = row.get(2)?;
        let open_price: Option<f64> = row.get(4)?;
        let current_price: Option<f64> = row.get(5)?;

        let op = open_price.unwrap_or(0.0);
        let cp = current_price.unwrap_or(0.0);
        let return_pct = if op > 0.0 { ((cp - op) / op) * 100.0 } else { 0.0 };

        Ok(Some(StatsRecord {
            high,
            low,
            twap,
            tick_count: count,
            open_price: op,
            current_price: cp,
            return_pct,
            since_timestamp: since,
        }))
    } else {
        Ok(None)
    }
}

pub fn count_active_keys(conn: &Connection) -> Result<usize> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM api_keys WHERE is_revoked = 0 AND (expires_at IS NULL OR expires_at > ?1)",
    )?;
    let count: i64 = stmt.query_row(params![now], |r| r.get(0))?;
    Ok(count as usize)
}

pub fn insert_api_key(
    conn: &Connection,
    id: &str,
    app_name: &str,
    key_hash: &str,
    created_at: i64,
    expires_at: Option<i64>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO api_keys (id, app_name, key_hash, created_at, expires_at, is_revoked) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
        params![id, app_name, key_hash, created_at, expires_at],
    )?;
    Ok(())
}

pub fn revoke_api_key(conn: &Connection, id: &str) -> Result<bool> {
    let affected = conn.execute(
        "UPDATE api_keys SET is_revoked = 1 WHERE id = ?1",
        params![id],
    )?;
    Ok(affected > 0)
}

pub fn list_all_keys(conn: &Connection) -> Result<Vec<ApiKeyRecord>> {
    let mut stmt = conn.prepare(
        "SELECT id, app_name, key_hash, created_at, expires_at, is_revoked FROM api_keys ORDER BY created_at DESC",
    )?;
    let rows = stmt.query_map([], |row| {
        let is_revoked_int: i32 = row.get(5)?;
        Ok(ApiKeyRecord {
            id: row.get(0)?,
            app_name: row.get(1)?,
            key_hash: row.get(2)?,
            created_at: row.get(3)?,
            expires_at: row.get(4)?,
            is_revoked: is_revoked_int != 0,
        })
    })?;

    let mut list = Vec::new();
    for r in rows {
        list.push(r?);
    }
    Ok(list)
}

pub fn run_downsampling_rollups(conn: &Connection) -> Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let one_day_ago = now - crate::config::RETENTION_TICKS_MS;
    let seven_days_ago = now - crate::config::RETENTION_CANDLES_1M_MS;
    let ninety_days_ago = now - crate::config::RETENTION_CANDLES_1H_MS;

    // 1. Rollup raw ticks into completed 1-minute candles
    conn.execute_batch(
        r#"
        INSERT OR REPLACE INTO candles_1m (symbol, t, open, high, low, close, twap, tick_count)
        SELECT 
            symbol,
            (t / 60000) * 60000 AS minute_t,
            (SELECT price FROM ticks t_in WHERE t_in.symbol = ticks.symbol AND t_in.t >= (ticks.t / 60000) * 60000 ORDER BY t_in.t ASC LIMIT 1) AS open,
            MAX(price) AS high,
            MIN(price) AS low,
            (SELECT price FROM ticks t_in WHERE t_in.symbol = ticks.symbol AND t_in.t < ((ticks.t / 60000) + 1) * 60000 ORDER BY t_in.t DESC LIMIT 1) AS close,
            AVG(price) AS twap,
            COUNT(*) AS tick_count
        FROM ticks
        WHERE t < (strftime('%s', 'now') * 1000) - 60000
        GROUP BY symbol, minute_t;

        -- 2. Rollup completed 1-minute candles into 1-hour candles
        INSERT OR REPLACE INTO candles_1h (symbol, t, open, high, low, close, twap, tick_count)
        SELECT 
            symbol,
            (t / 3600000) * 3600000 AS hour_t,
            (SELECT open FROM candles_1m c_in WHERE c_in.symbol = candles_1m.symbol AND c_in.t >= (candles_1m.t / 3600000) * 3600000 ORDER BY c_in.t ASC LIMIT 1),
            MAX(high),
            MIN(low),
            (SELECT close FROM candles_1m c_in WHERE c_in.symbol = candles_1m.symbol AND c_in.t < ((candles_1m.t / 3600000) + 1) * 3600000 ORDER BY c_in.t DESC LIMIT 1),
            AVG(twap),
            SUM(tick_count)
        FROM candles_1m
        WHERE t < (strftime('%s', 'now') * 1000) - 3600000
        GROUP BY symbol, hour_t;

        -- 3. Rollup completed 1-hour candles into 1-day candles
        INSERT OR REPLACE INTO candles_1d (symbol, t, open, high, low, close, twap, tick_count)
        SELECT 
            symbol,
            (t / 86400000) * 86400000 AS day_t,
            (SELECT open FROM candles_1h c_in WHERE c_in.symbol = candles_1h.symbol AND c_in.t >= (candles_1h.t / 86400000) * 86400000 ORDER BY c_in.t ASC LIMIT 1),
            MAX(high),
            MIN(low),
            (SELECT close FROM candles_1h c_in WHERE c_in.symbol = candles_1h.symbol AND c_in.t < ((candles_1h.t / 86400000) + 1) * 86400000 ORDER BY c_in.t DESC LIMIT 1),
            AVG(twap),
            SUM(tick_count)
        FROM candles_1h
        WHERE t < (strftime('%s', 'now') * 1000) - 86400000
        GROUP BY symbol, day_t;
        "#,
    )?;

    // 4. Cascade Prune Older Partitions
    conn.execute("DELETE FROM ticks WHERE t < ?1", params![one_day_ago])?;
    conn.execute("DELETE FROM candles_1m WHERE t < ?1", params![seven_days_ago])?;
    conn.execute("DELETE FROM candles_1h WHERE t < ?1", params![ninety_days_ago])?;
    // Note: candles_1d is never pruned (permanent time-series archive)

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::init_schema;

    #[test]
    fn test_tick_and_stats_queries() {
        let conn = Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Insert ticks
        insert_tick(&conn, "SOL/USD", now - 30_000, 150.0, "binance-ws").unwrap();
        insert_tick(&conn, "SOL/USD", now - 20_000, 155.0, "binance-ws").unwrap();
        insert_tick(&conn, "SOL/USD", now - 10_000, 145.0, "binance-ws").unwrap();
        insert_tick(&conn, "SOL/USD", now, 160.0, "binance-ws").unwrap();

        // Test latest tick
        let latest = get_latest_tick(&conn, "SOL/USD").unwrap().unwrap();
        assert_eq!(latest.symbol, "SOL/USD");
        assert_eq!(latest.price, 160.0);
        assert_eq!(latest.t, now);

        // Test recent ticks (returned in chronological order oldest -> newest)
        let recent = get_recent_ticks(&conn, "SOL/USD", 2).unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].price, 145.0);
        assert_eq!(recent[1].price, 160.0);

        // Test stats
        let stats = get_stats(&conn, "SOL/USD", 60_000).unwrap().unwrap();
        assert_eq!(stats.tick_count, 4);
        assert_eq!(stats.high, 160.0);
        assert_eq!(stats.low, 145.0);
        assert_eq!(stats.open_price, 150.0);
        assert_eq!(stats.current_price, 160.0);
        // (160 - 150) / 150 * 100 = 6.666...%
        assert!((stats.return_pct - 6.6666).abs() < 0.01);
    }
}
