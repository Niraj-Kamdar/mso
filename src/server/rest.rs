use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use serde::Deserialize;
use std::sync::Arc;

use crate::auth::AuthManager;
use crate::config::clamp_effective_ttl;
use crate::feed::FeedCoordinator;
use crate::server::validator::{clamp_limit, clamp_window_ms, validate_interval, validate_symbol};

#[derive(Clone)]
pub struct AppState {
    pub feed: FeedCoordinator,
    pub auth: AuthManager,
    pub start_time: std::time::Instant,
}

#[derive(Deserialize)]
pub struct PriceQuery {
    pub symbol: Option<String>,
    pub max_age_ms: Option<u64>,
    pub key: Option<String>,
}

#[derive(Deserialize)]
pub struct TicksQuery {
    pub symbol: Option<String>,
    pub limit: Option<i32>,
    pub key: Option<String>,
}

#[derive(Deserialize)]
pub struct CandlesQuery {
    pub symbol: Option<String>,
    pub interval: Option<String>,
    pub limit: Option<i32>,
    pub key: Option<String>,
}

#[derive(Deserialize)]
pub struct StatsQuery {
    pub symbol: Option<String>,
    pub window_ms: Option<u64>,
    pub key: Option<String>,
}

fn authenticate_request(headers: &HeaderMap, query_key: Option<&str>, auth: &AuthManager) -> Result<(), (StatusCode, Json<serde_json::Value>)> {
    let key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.trim_start_matches("Bearer ").trim())
        })
        .or(query_key);

    match key {
        Some(k) => match auth.authenticate(k) {
            Ok(_) => Ok(()),
            Err(e) => Err((
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": e.to_string() })),
            )),
        },
        None => Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Missing x-api-key header or ?key= parameter" })),
        )),
    }
}

pub fn create_rest_router(feed: FeedCoordinator, auth: AuthManager) -> Router {
    let state = Arc::new(AppState {
        feed,
        auth,
        start_time: std::time::Instant::now(),
    });

    Router::new()
        .route("/health", get(health_handler))
        .route("/api/v1/price", get(get_price_handler))
        .route("/api/v1/ticks", get(get_ticks_handler))
        .route("/api/v1/candles", get(get_candles_handler))
        .route("/api/v1/stats", get(get_stats_handler))
        .route("/", get(dashboard_handler))
        .with_state(state)
}

async fn health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let uptime = state.start_time.elapsed().as_secs();
    Json(serde_json::json!({
        "status": "ok",
        "service": "mso",
        "version": "0.1.0",
        "uptime_seconds": uptime
    }))
}

async fn get_price_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<PriceQuery>,
) -> impl IntoResponse {
    if let Err(auth_err) = authenticate_request(&headers, query.key.as_deref(), &state.auth) {
        return auth_err.into_response();
    }

    let raw_symbol = query.symbol.unwrap_or_else(|| "SOL/USD".to_string());
    let symbol = match validate_symbol(&raw_symbol) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let effective_ttl = clamp_effective_ttl(&symbol, query.max_age_ms);

    // 1. Check L1 in-memory hot cache
    if let Some(entry) = state.feed.l1_cache.get(&symbol) {
        let age = (now - entry.cached_at) as u64;
        if age <= effective_ttl {
            return Json(serde_json::json!({
                "symbol": entry.symbol,
                "price": entry.price,
                "timestamp": entry.timestamp,
                "source": entry.source,
                "is_stale": false,
                "cached": true
            })).into_response();
        }
    }

    // 2. Check SQLite fallback
    let db_res = state.feed.db.with_conn(|conn| crate::db::queries::get_latest_tick(conn, &symbol));
    match db_res {
        Ok(Some(r)) => {
            let age = (now - r.t) as u64;
            let is_stale = age > (effective_ttl * 2);
            Json(serde_json::json!({
                "symbol": r.symbol,
                "price": r.price,
                "timestamp": r.t,
                "source": r.source,
                "is_stale": is_stale,
                "cached": false
            })).into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": format!("No price data available for {}", symbol) }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn get_ticks_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<TicksQuery>,
) -> impl IntoResponse {
    if let Err(auth_err) = authenticate_request(&headers, query.key.as_deref(), &state.auth) {
        return auth_err.into_response();
    }

    let raw_symbol = query.symbol.unwrap_or_else(|| "SOL/USD".to_string());
    let symbol = match validate_symbol(&raw_symbol) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    };

    let limit = clamp_limit(query.limit);

    let rows = state.feed.db.with_conn(|conn| crate::db::queries::get_recent_ticks(conn, &symbol, limit));
    match rows {
        Ok(samples) => Json(serde_json::json!({
            "symbol": symbol,
            "samples": samples.into_iter().map(|s| serde_json::json!({ "t": s.t, "p": s.price })).collect::<Vec<_>>()
        })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn get_candles_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<CandlesQuery>,
) -> impl IntoResponse {
    if let Err(auth_err) = authenticate_request(&headers, query.key.as_deref(), &state.auth) {
        return auth_err.into_response();
    }

    let raw_symbol = query.symbol.unwrap_or_else(|| "SOL/USD".to_string());
    let symbol = match validate_symbol(&raw_symbol) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    };

    let raw_interval = query.interval.unwrap_or_else(|| "1m".to_string());
    let table = match validate_interval(&raw_interval) {
        Ok(t) => t,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    };

    let limit = clamp_limit(query.limit);

    let rows = state.feed.db.with_conn(|conn| crate::db::queries::get_candles(conn, &table, &symbol, limit));
    match rows {
        Ok(candles) => Json(serde_json::json!({
            "symbol": symbol,
            "interval": raw_interval,
            "candles": candles
        })).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn get_stats_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<StatsQuery>,
) -> impl IntoResponse {
    if let Err(auth_err) = authenticate_request(&headers, query.key.as_deref(), &state.auth) {
        return auth_err.into_response();
    }

    let raw_symbol = query.symbol.unwrap_or_else(|| "SOL/USD".to_string());
    let symbol = match validate_symbol(&raw_symbol) {
        Ok(s) => s,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    };

    let window = clamp_window_ms(query.window_ms);

    let res = state.feed.db.with_conn(|conn| crate::db::queries::get_stats(conn, &symbol, window));
    match res {
        Ok(Some(s)) => Json(serde_json::json!({
            "symbol": symbol,
            "window_ms": window,
            "high": s.high,
            "low": s.low,
            "twap": s.twap,
            "tick_count": s.tick_count,
            "open_price": s.open_price,
            "current_price": s.current_price,
            "return_pct": s.return_pct,
            "since_timestamp": s.since_timestamp
        })).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": format!("No stats available for {} in window {}ms", symbol, window) }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": e.to_string() }))).into_response(),
    }
}

async fn dashboard_handler() -> Html<&'static str> {
    Html(r#"
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <title>Metasquare Oracle (mso)</title>
      <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, monospace; background: #080a0f; color: #16f08e; margin: 0; padding: 2rem; }
        .container { max-width: 800px; margin: 0 auto; }
        h1 { color: #fff; font-size: 1.8rem; border-bottom: 1px solid #1f2430; padding-bottom: 0.8rem; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1rem; margin-top: 1.5rem; }
        .card { background: #0f131a; border: 1px solid #1f2430; border-radius: 8px; padding: 1.2rem; }
        .card h3 { margin: 0 0 0.5rem 0; color: #8a919e; font-size: 0.9rem; text-transform: uppercase; }
        .val { font-size: 1.6rem; font-weight: bold; color: #ffc83d; }
        .sub { font-size: 0.75rem; color: #6b7280; margin-top: 0.4rem; }
        .badge { background: #1f3b2e; color: #16f08e; padding: 2px 6px; border-radius: 4px; font-size: 0.7rem; }
        .footer { margin-top: 2rem; font-size: 0.8rem; color: #4b5563; }
      </style>
    </head>
    <body>
      <div class="container">
        <h1>🟢 Metasquare Oracle (mso) Node</h1>
        <p style="color:#9ca3af;">Self-hosted on Raspberry Pi with NVMe storage. Ingesting Fast Tier (SOL 1s) and Standard Tier (60s Batch).</p>
        <div class="grid">
          <div class="card">
            <h3>SOL/USD <span class="badge">1s LIVE</span></h3>
            <div id="sol-price" class="val">Connecting...</div>
            <div id="sol-meta" class="sub">Binance WS Stream</div>
          </div>
          <div class="card">
            <h3>BTC/USD <span class="badge">1m Batch</span></h3>
            <div id="btc-price" class="val">Loading...</div>
            <div class="sub">Binance Multi-Ticker</div>
          </div>
          <div class="card">
            <h3>HYPE/USD <span class="badge">1m Poller</span></h3>
            <div id="hype-price" class="val">Loading...</div>
            <div class="sub">Hyperliquid L1 DEX</div>
          </div>
          <div class="card">
            <h3>PAXG/USD (Gold) <span class="badge">1m Poller</span></h3>
            <div id="paxg-price" class="val">Loading...</div>
            <div class="sub">Tokenized Gold (Binance/Jup)</div>
          </div>
        </div>
        <div class="footer">
          Endpoints: <code>/api/v1/price</code>, <code>/api/v1/ticks</code>, <code>/api/v1/candles</code>, <code>/api/v1/stats</code>, <code>/health</code><br>
          Protected with Cloudflare WAF + in-memory API key validation.
        </div>
      </div>
    </body>
    </html>
    "#)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use tower::ServiceExt;
    use crate::db::DbManager;

    #[tokio::test]
    async fn test_rest_endpoints() {
        let db = DbManager::in_memory().unwrap();
        let auth = AuthManager::new(db.clone()).unwrap();
        let feed = FeedCoordinator::new(db.clone());

        // Create an API key
        let (_id, secret) = auth.create_key("integration-test-app", None).unwrap();

        // Record a tick into L1 cache and DB
        feed.record_tick("SOL/USD", 150.50, 1700000000000, "test-feed");

        let app = create_rest_router(feed, auth);

        // 1. Test /health (public, no auth)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // 2. Test /api/v1/price without API key -> 401 Unauthorized
        let unauth = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/price?symbol=SOL/USD")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

        // 3. Test /api/v1/price with valid API key header -> 200 OK
        let authed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/price?symbol=SOL/USD")
                    .header("x-api-key", &secret)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authed.status(), StatusCode::OK);

        // 4. Test /api/v1/price with query parameter ?key=... -> 200 OK
        let authed_query = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/price?symbol=SOL/USD&key={}", secret))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authed_query.status(), StatusCode::OK);

        // 5. Test unsupported symbol -> 400 Bad Request
        let bad_sym = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/price?symbol=UNKNOWN/USD&key={}", secret))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_sym.status(), StatusCode::BAD_REQUEST);

        // 6. Test /api/v1/ticks -> 200 OK
        let ticks = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/ticks?symbol=SOL/USD&limit=10&key={}", secret))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ticks.status(), StatusCode::OK);
    }
}
