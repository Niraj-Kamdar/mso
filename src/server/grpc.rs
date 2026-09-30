pub mod proto {
    tonic::include_proto!("oracle.v1");
}

use std::pin::Pin;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use proto::oracle_service_server::OracleService;
pub use proto::oracle_service_server::OracleServiceServer;
use proto::*;

use crate::auth::AuthManager;
use crate::config::clamp_effective_ttl;
use crate::feed::FeedCoordinator;
use crate::server::validator::{clamp_limit, clamp_window_ms, validate_symbol};

pub struct GrpcOracleService {
    feed: FeedCoordinator,
    auth: AuthManager,
}

impl GrpcOracleService {
    pub fn new(feed: FeedCoordinator, auth: AuthManager) -> Self {
        Self { feed, auth }
    }

    fn check_auth<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let metadata = req.metadata();
        let api_key = metadata
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .or_else(|| {
                metadata
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.trim_start_matches("Bearer ").trim())
            });

        match api_key {
            Some(key) => self
                .auth
                .authenticate(key)
                .map(|_| ())
                .map_err(|e| Status::unauthenticated(e.to_string())),
            None => Err(Status::unauthenticated("Missing x-api-key metadata header")),
        }
    }
}

#[tonic::async_trait]
impl OracleService for GrpcOracleService {
    type StreamPricesStream =
        Pin<Box<dyn Stream<Item = Result<PriceTick, Status>> + Send + 'static>>;

    async fn get_price(
        &self,
        request: Request<PriceRequest>,
    ) -> Result<Response<PriceResponse>, Status> {
        self.check_auth(&request)?;
        let inner = request.into_inner();
        let symbol = validate_symbol(&inner.symbol).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let effective_ttl = clamp_effective_ttl(&symbol, inner.max_age_ms);

        // 1. Check L1 In-Memory Cache
        if let Some(entry) = self.feed.l1_cache.get(&symbol) {
            let age = (now - entry.cached_at) as u64;
            if age <= effective_ttl {
                return Ok(Response::new(PriceResponse {
                    symbol: entry.symbol.clone(),
                    price: entry.price,
                    timestamp: entry.timestamp,
                    source: entry.source.clone(),
                    is_stale: false,
                }));
            }
        }

        // 2. Fallback to SQLite latest tick
        let db_res = self
            .feed
            .db
            .with_conn(|conn| crate::db::queries::get_latest_tick(conn, &symbol))
            .map_err(|e| Status::internal(e.to_string()))?;

        match db_res {
            Some(record) => {
                let age = (now - record.t) as u64;
                let is_stale = age > (effective_ttl * 2);

                Ok(Response::new(PriceResponse {
                    symbol: record.symbol,
                    price: record.price,
                    timestamp: record.t,
                    source: record.source,
                    is_stale,
                }))
            }
            None => Err(Status::not_found(format!(
                "No price data available yet for symbol: {}",
                symbol
            ))),
        }
    }

    async fn stream_prices(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamPricesStream>, Status> {
        self.check_auth(&request)?;
        let inner = request.into_inner();
        let target_symbol =
            validate_symbol(&inner.symbol).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let mut rx = self.feed.broadcast_tx.subscribe();

        let stream = async_stream::try_stream! {
            while let Ok(event) = rx.recv().await {
                if event.symbol == target_symbol {
                    yield PriceTick {
                        symbol: event.symbol,
                        price: event.price,
                        timestamp: event.timestamp,
                    };
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_candles(
        &self,
        request: Request<CandleRequest>,
    ) -> Result<Response<CandleResponse>, Status> {
        self.check_auth(&request)?;
        let inner = request.into_inner();
        let symbol = validate_symbol(&inner.symbol).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let limit = clamp_limit(Some(inner.limit));
        let (table_name, interval_code) = match inner.interval {
            1 => ("ticks", inner.interval),
            2 => ("candles_1m", inner.interval),
            3 => ("candles_1h", inner.interval),
            4 => ("candles_1d", inner.interval),
            _ => ("candles_1m", 2),
        };

        let records = self
            .feed
            .db
            .with_conn(|conn| crate::db::queries::get_candles(conn, table_name, &symbol, limit))
            .map_err(|e| Status::internal(e.to_string()))?;

        let candles = records
            .into_iter()
            .map(|r| Candle {
                timestamp: r.timestamp,
                open: r.open,
                high: r.high,
                low: r.low,
                close: r.close,
                twap: r.twap,
                tick_count: r.tick_count,
            })
            .collect();

        Ok(Response::new(CandleResponse {
            symbol,
            interval: interval_code,
            candles,
        }))
    }

    async fn get_stats(
        &self,
        request: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        self.check_auth(&request)?;
        let inner = request.into_inner();
        let symbol = validate_symbol(&inner.symbol).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let window = clamp_window_ms(Some(inner.window_ms));

        let stats = self
            .feed
            .db
            .with_conn(|conn| crate::db::queries::get_stats(conn, &symbol, window))
            .map_err(|e| Status::internal(e.to_string()))?;

        match stats {
            Some(s) => Ok(Response::new(StatsResponse {
                symbol,
                high: s.high,
                low: s.low,
                twap: s.twap,
                tick_count: s.tick_count,
                open_price: s.open_price,
                current_price: s.current_price,
                return_pct: s.return_pct,
                since_timestamp: s.since_timestamp,
            })),
            None => Err(Status::not_found(format!(
                "No statistics available for symbol {} in window {}ms",
                symbol, window
            ))),
        }
    }

    async fn get_ticks(
        &self,
        request: Request<TicksRequest>,
    ) -> Result<Response<TicksResponse>, Status> {
        self.check_auth(&request)?;
        let inner = request.into_inner();
        let symbol = validate_symbol(&inner.symbol).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let limit = clamp_limit(Some(inner.limit));

        let rows = self
            .feed
            .db
            .with_conn(|conn| crate::db::queries::get_recent_ticks(conn, &symbol, limit))
            .map_err(|e| Status::internal(e.to_string()))?;

        let samples = rows
            .into_iter()
            .map(|r| PriceTick {
                symbol: r.symbol,
                price: r.price,
                timestamp: r.t,
            })
            .collect();

        Ok(Response::new(TicksResponse { symbol, samples }))
    }
}
