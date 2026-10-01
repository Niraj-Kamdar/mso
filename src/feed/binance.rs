use futures_util::StreamExt;
use serde::Deserialize;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

use super::{now_ms, FeedCoordinator};
use crate::config::Source;

#[derive(Deserialize)]
struct BinanceTickerMessage {
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "c")]
    close_price: Option<String>,
    #[serde(rename = "E")]
    event_time: Option<i64>,
}

// Combined-stream envelope: {"stream": "...", "data": {...}}
#[derive(Deserialize)]
struct CombinedMessage {
    data: BinanceTickerMessage,
}

#[derive(Deserialize)]
struct BinanceBatchItem {
    symbol: String,
    price: String,
}

/// Streams 1s tickers for `pairs` (e.g. SOLUSDT) over one combined WebSocket.
pub async fn start_ws(coord: FeedCoordinator, pairs: Vec<String>) {
    let streams: Vec<String> = pairs.iter().map(|p| format!("{}@ticker", p.to_lowercase())).collect();
    let url = format!("wss://stream.binance.com:9443/stream?streams={}", streams.join("/"));

    loop {
        info!("[Binance] Connecting to ticker stream for {}...", pairs.join(", "));
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                info!("[Binance] Connected to ticker stream!");
                let (_, mut read) = ws_stream.split();

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                            if let Ok(CombinedMessage { data: ticker }) = serde_json::from_str(&text) {
                                if let (Some(price_str), Some(sym)) = (ticker.close_price, ticker.symbol) {
                                    if let Ok(price) = price_str.parse::<f64>() {
                                        let ts = ticker.event_time.unwrap_or_else(now_ms);
                                        coord.ingest(Source::BinanceWs, &sym, price, ts);
                                    }
                                }
                            }
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Ping(data)) => {
                            // Tungstenite automatically responds to pings, but we log trace
                            tracing::trace!("[Binance] Ping received: {:?}", data);
                        }
                        Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                            warn!("[Binance] Server sent close frame.");
                            break;
                        }
                        Err(e) => {
                            warn!("[Binance] WebSocket read error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!("[Binance] Failed to connect to WebSocket: {}. Retrying in 2s...", e);
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
}

pub async fn fetch_batch(client: &reqwest::Client, pairs: &[String]) -> Result<Vec<(String, f64)>, String> {
    let symbols = serde_json::to_string(pairs).unwrap();
    let resp = client
        .get("https://api.binance.com/api/v3/ticker/price")
        .query(&[("symbols", symbols)])
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("status {}", resp.status()));
    }
    let items: Vec<BinanceBatchItem> = resp.json().await.map_err(|e| e.to_string())?;
    Ok(items
        .into_iter()
        .filter_map(|i| Some((i.symbol, i.price.parse().ok()?)))
        .collect())
}
