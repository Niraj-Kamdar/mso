use std::collections::HashMap;
use tokio::time::{interval, Duration};
use tracing::warn;

use super::FeedCoordinator;

pub async fn start_hype_poller(coord: FeedCoordinator) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let url = "https://api.hyperliquid.xyz/info";
    let mut ticker = interval(Duration::from_secs(60));

    loop {
        ticker.tick().await;

        let payload = serde_json::json!({ "type": "allMids" });

        match client.post(url).json(&payload).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    if let Ok(mids) = resp.json::<HashMap<String, String>>().await {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64;

                        // 1. Ingest HYPE
                        if let Some(price_str) = mids.get("HYPE") {
                            if let Ok(price) = price_str.parse::<f64>() {
                                coord.record_tick("HYPE/USD", price, now, "hyperliquid");
                            }
                        }

                        // 2. Backup check for SOL if L1 cache hasn't updated in > 5s
                        if let Some(entry) = coord.l1_cache.get("SOL/USD") {
                            if now - entry.timestamp > 5000 {
                                if let Some(sol_str) = mids.get("SOL") {
                                    if let Ok(sol_price) = sol_str.parse::<f64>() {
                                        coord.record_tick("SOL/USD", sol_price, now, "hyperliquid-backup");
                                    }
                                }
                            }
                        }
                    }
                } else {
                    warn!("[Hyperliquid] allMids returned status {}", resp.status());
                }
            }
            Err(e) => {
                warn!("[Hyperliquid] Request error: {}", e);
            }
        }
    }
}
