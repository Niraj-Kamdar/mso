use futures_util::StreamExt;
use serde::Deserialize;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

use super::FeedCoordinator;

#[derive(Deserialize)]
struct BinanceTickerMessage {
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "c")]
    close_price: Option<String>,
    #[serde(rename = "E")]
    event_time: Option<i64>,
}

#[derive(Deserialize)]
struct BinanceBatchItem {
    symbol: String,
    price: String,
}

pub async fn start_sol_ws(coord: FeedCoordinator) {
    let url = "wss://stream.binance.com:9443/ws/solusdt@ticker";

    loop {
        info!("[Binance] Connecting to SOL/USDT 1s WebSocket stream...");
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                info!("[Binance] Connected to SOL/USDT stream!");
                let (_, mut read) = ws_stream.split();

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                            if let Ok(ticker) = serde_json::from_str::<BinanceTickerMessage>(&text) {
                                if let (Some(price_str), Some(sym)) = (ticker.close_price, ticker.symbol) {
                                    if sym == "SOLUSDT" {
                                        if let Ok(price) = price_str.parse::<f64>() {
                                            let now = ticker.event_time.unwrap_or_else(|| {
                                                std::time::SystemTime::now()
                                                    .duration_since(std::time::UNIX_EPOCH)
                                                    .unwrap()
                                                    .as_millis() as i64
                                            });
                                            coord.record_tick("SOL/USD", price, now, "binance-ws");
                                        }
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

pub async fn start_batch_poller(coord: FeedCoordinator) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let symbols_query = urlencoding::encode("[\"BTCUSDT\",\"ETHUSDT\",\"ZECUSDT\",\"PAXGUSDT\"]");
    let url = format!(
        "https://api.binance.com/api/v3/ticker/price?symbols={}",
        symbols_query
    );

    let mut interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        interval.tick().await;

        match client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if let Ok(items) = resp.json::<Vec<BinanceBatchItem>>().await {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64;

                        for item in items {
                            if let Ok(price) = item.price.parse::<f64>() {
                                let standard_sym = match item.symbol.as_str() {
                                    "BTCUSDT" => "BTC/USD",
                                    "ETHUSDT" => "ETH/USD",
                                    "ZECUSDT" => "ZEC/USD",
                                    "PAXGUSDT" => "PAXG/USD",
                                    _ => continue,
                                };
                                coord.record_tick(standard_sym, price, now, "binance-batch");
                            }
                        }
                    }
                } else {
                    warn!("[Binance] Batch poll returned status {}", resp.status());
                }
            }
            Err(e) => {
                warn!("[Binance] Batch poll request error: {}", e);
            }
        }
    }
}

mod urlencoding {
    pub fn encode(s: &str) -> String {
        s.replace('[', "%5B")
            .replace(']', "%5D")
            .replace('"', "%22")
    }
}
