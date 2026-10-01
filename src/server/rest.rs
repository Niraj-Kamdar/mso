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
        .route("/openapi.json", get(openapi_handler))
        .route("/docs", get(scalar_docs_handler))
        .route("/", get(scalar_docs_handler))
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

async fn scalar_docs_handler() -> Html<&'static str> {
    Html(r#"
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Metasquare Oracle (mso) - Interactive API Reference</title>
        <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🟢</text></svg>">
        <style>
          body { margin: 0; background-color: #0b0d13; }
        </style>
      </head>
      <body>
        <script
          id="api-reference"
          data-url="/openapi.json"
          data-configuration='{
            "theme": "purple",
            "darkMode": true,
            "defaultHttpClient": {
              "targetKey": "shell",
              "clientKey": "curl"
            },
            "servers": [
              { "url": "https://oracle-api.metasquare.tech", "description": "Production Node (Raspberry Pi 5)" },
              { "url": "http://localhost:4000", "description": "Local Development Node" }
            ],
            "metaData": {
              "title": "Metasquare Oracle (mso) API",
              "description": "High-performance multi-tier oracle microservice"
            }
          }'
        ></script>
        <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
      </body>
    </html>
    "#)
}

async fn openapi_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Metasquare Oracle (mso) API",
            "version": "0.1.0",
            "description": "High-performance, multi-tier, self-hosted oracle microservice running on Raspberry Pi 5 with NVMe SSD. Ingests Fast Tier (SOL/USD 1s) and Standard Tier (60s Batch) feeds with SQLite WAL and time-series pyramid rollups."
        },
        "servers": [
            { "url": "https://oracle-api.metasquare.tech", "description": "Production Node (Raspberry Pi 5)" },
            { "url": "http://localhost:4000", "description": "Local Development Node" }
        ],
        "tags": [
            { "name": "Market Data", "description": "Real-time spot prices, raw chronological ticks, and historical OHLC candles" },
            { "name": "Analytics", "description": "Rolling window TWAP, high/low, and return percentage statistics" },
            { "name": "System", "description": "Service health and uptime monitoring" }
        ],
        "components": {
            "securitySchemes": {
                "ApiKeyHeader": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "x-api-key",
                    "description": "API Key passed in HTTP Header"
                },
                "BearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": "Bearer token format: 'Authorization: Bearer <API_KEY>'"
                },
                "ApiKeyQuery": {
                    "type": "apiKey",
                    "in": "query",
                    "name": "key",
                    "description": "API Key passed as URL query parameter '?key=<API_KEY>'"
                }
            },
            "schemas": {
                "PriceResponse": {
                    "type": "object",
                    "properties": {
                        "symbol": { "type": "string", "example": "SOL/USD" },
                        "price": { "type": "number", "example": 152.34 },
                        "timestamp": { "type": "integer", "format": "int64", "example": 1700000000000i64 },
                        "source": { "type": "string", "example": "binance-ws" },
                        "cached": { "type": "boolean", "example": true },
                        "is_stale": { "type": "boolean", "example": false }
                    },
                    "required": ["symbol", "price", "timestamp", "source", "cached", "is_stale"]
                },
                "PriceSample": {
                    "type": "object",
                    "properties": {
                        "t": { "type": "integer", "format": "int64", "example": 1700000000000i64 },
                        "p": { "type": "number", "example": 152.34 }
                    },
                    "required": ["t", "p"]
                },
                "TicksResponse": {
                    "type": "object",
                    "properties": {
                        "symbol": { "type": "string", "example": "SOL/USD" },
                        "samples": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/PriceSample" }
                        }
                    },
                    "required": ["symbol", "samples"]
                },
                "Candle": {
                    "type": "object",
                    "properties": {
                        "timestamp": { "type": "integer", "format": "int64", "example": 1700000000000i64 },
                        "open": { "type": "number", "example": 150.0 },
                        "high": { "type": "number", "example": 155.0 },
                        "low": { "type": "number", "example": 149.5 },
                        "close": { "type": "number", "example": 154.2 },
                        "twap": { "type": "number", "example": 152.8 },
                        "tick_count": { "type": "integer", "example": 60 }
                    },
                    "required": ["timestamp", "open", "high", "low", "close", "twap", "tick_count"]
                },
                "CandlesResponse": {
                    "type": "object",
                    "properties": {
                        "symbol": { "type": "string", "example": "SOL/USD" },
                        "interval": { "type": "string", "example": "1m" },
                        "candles": {
                            "type": "array",
                            "items": { "$ref": "#/components/schemas/Candle" }
                        }
                    },
                    "required": ["symbol", "interval", "candles"]
                },
                "StatsResponse": {
                    "type": "object",
                    "properties": {
                        "symbol": { "type": "string", "example": "SOL/USD" },
                        "window_ms": { "type": "integer", "format": "int64", "example": 60000 },
                        "high": { "type": "number", "example": 155.0 },
                        "low": { "type": "number", "example": 149.5 },
                        "twap": { "type": "number", "example": 152.8 },
                        "tick_count": { "type": "integer", "example": 60 },
                        "open_price": { "type": "number", "example": 150.0 },
                        "current_price": { "type": "number", "example": 154.2 },
                        "return_pct": { "type": "number", "example": 2.8 },
                        "since_timestamp": { "type": "integer", "format": "int64", "example": 1699999940000i64 }
                    },
                    "required": ["symbol", "window_ms", "high", "low", "twap", "tick_count", "open_price", "current_price", "return_pct", "since_timestamp"]
                },
                "HealthResponse": {
                    "type": "object",
                    "properties": {
                        "status": { "type": "string", "example": "ok" },
                        "service": { "type": "string", "example": "mso" },
                        "version": { "type": "string", "example": "0.1.0" },
                        "uptime_seconds": { "type": "integer", "example": 3600 }
                    },
                    "required": ["status", "service", "version", "uptime_seconds"]
                },
                "ErrorResponse": {
                    "type": "object",
                    "properties": {
                        "error": { "type": "string", "example": "Invalid or unrecognized API key" }
                    },
                    "required": ["error"]
                }
            }
        },
        "paths": {
            "/health": {
                "get": {
                    "tags": ["System"],
                    "summary": "Health and uptime check",
                    "description": "Public health check endpoint returning node status and elapsed uptime in seconds. Used for monitoring and container health checks.",
                    "responses": {
                        "200": {
                            "description": "Node healthy",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/HealthResponse" }
                                }
                            }
                        }
                    }
                }
            },
            "/api/v1/price": {
                "get": {
                    "tags": ["Market Data"],
                    "summary": "Get latest spot price",
                    "description": "Returns the latest real-time or cached spot price for a supported ticker. Sub-millisecond response served from L1 in-memory cache with fallback to SQLite WAL.",
                    "security": [
                        { "ApiKeyHeader": [] },
                        { "BearerAuth": [] },
                        { "ApiKeyQuery": [] }
                    ],
                    "parameters": [
                        {
                            "name": "symbol",
                            "in": "query",
                            "required": false,
                            "schema": {
                                "type": "string",
                                "enum": crate::config::get().assets.iter().map(|a| a.symbol.as_str()).collect::<Vec<_>>(),
                                "default": "SOL/USD"
                            },
                            "description": "Supported market symbol"
                        },
                        {
                            "name": "max_age_ms",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "integer", "format": "int64" },
                            "description": "Requested max cache age in milliseconds (clamped to feed resolution: min 1,000ms for SOL, 60,000ms for standard)"
                        },
                        {
                            "name": "key",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "string" },
                            "description": "API key alternative to x-api-key header"
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Latest price tick",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/PriceResponse" }
                                }
                            }
                        },
                        "400": {
                            "description": "Invalid or unsupported symbol",
                            "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ErrorResponse" } } }
                        },
                        "401": {
                            "description": "Missing or unauthorized API key",
                            "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ErrorResponse" } } }
                        },
                        "404": {
                            "description": "No price data recorded yet",
                            "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ErrorResponse" } } }
                        }
                    }
                }
            },
            "/api/v1/ticks": {
                "get": {
                    "tags": ["Market Data"],
                    "summary": "Get recent raw ticks",
                    "description": "Returns chronologically sorted raw price ticks `[{t, p}]` suitable for lightweight SVG sparklines and live chart plots.",
                    "security": [
                        { "ApiKeyHeader": [] },
                        { "BearerAuth": [] },
                        { "ApiKeyQuery": [] }
                    ],
                    "parameters": [
                        {
                            "name": "symbol",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "string", "default": "SOL/USD" },
                            "description": "Supported market symbol"
                        },
                        {
                            "name": "limit",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "integer", "minimum": 1, "maximum": 300, "default": 60 },
                            "description": "Number of samples (clamped between 1 and 300)"
                        },
                        {
                            "name": "key",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Chronological tick samples",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/TicksResponse" }
                                }
                            }
                        },
                        "400": { "description": "Invalid parameters" },
                        "401": { "description": "Unauthorized" }
                    }
                }
            },
            "/api/v1/candles": {
                "get": {
                    "tags": ["Market Data"],
                    "summary": "Get historical OHLC & TWAP candles",
                    "description": "Retrieve historical candle bars aggregated by the continuous time-series rollup worker.",
                    "security": [
                        { "ApiKeyHeader": [] },
                        { "BearerAuth": [] },
                        { "ApiKeyQuery": [] }
                    ],
                    "parameters": [
                        {
                            "name": "symbol",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "string", "default": "SOL/USD" }
                        },
                        {
                            "name": "interval",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "string", "enum": ["1s", "1m", "1h", "1d"], "default": "1m" },
                            "description": "Candle bar interval"
                        },
                        {
                            "name": "limit",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "integer", "minimum": 1, "maximum": 300, "default": 60 }
                        },
                        {
                            "name": "key",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Candle array in chronological order",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/CandlesResponse" }
                                }
                            }
                        },
                        "400": { "description": "Invalid parameters or interval" },
                        "401": { "description": "Unauthorized" }
                    }
                }
            },
            "/api/v1/stats": {
                "get": {
                    "tags": ["Analytics"],
                    "summary": "Get rolling window statistics",
                    "description": "Computes high, low, TWAP, open price, current price, return percentage, and tick count over a strictly bounded rolling time window.",
                    "security": [
                        { "ApiKeyHeader": [] },
                        { "BearerAuth": [] },
                        { "ApiKeyQuery": [] }
                    ],
                    "parameters": [
                        {
                            "name": "symbol",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "string", "default": "SOL/USD" }
                        },
                        {
                            "name": "window_ms",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "integer", "format": "int64", "minimum": 10000, "maximum": 86400000, "default": 300000 },
                            "description": "Time window in milliseconds (clamped between 10,000ms [10s] and 86,400,000ms [24h])"
                        },
                        {
                            "name": "key",
                            "in": "query",
                            "required": false,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Window statistics",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/StatsResponse" }
                                }
                            }
                        },
                        "400": { "description": "Invalid parameters" },
                        "401": { "description": "Unauthorized" }
                    }
                }
            }
        }
    }))
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

        // 7. Test / (Scalar Interactive Docs) -> 200 OK
        let scalar = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scalar.status(), StatusCode::OK);

        // 8. Test /openapi.json -> 200 OK
        let openapi = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(openapi.status(), StatusCode::OK);
    }
}
