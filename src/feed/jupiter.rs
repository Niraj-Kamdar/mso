use serde::Deserialize;
use std::collections::HashMap;
use tokio::time::{interval, Duration};

use super::FeedCoordinator;

#[derive(Deserialize)]
struct JupiterPriceData {
    price: Option<String>,
}

#[derive(Deserialize)]
struct JupiterApiResponse {
    data: Option<HashMap<String, JupiterPriceData>>,
}

// Mint mappings on Solana for Backed xStocks & PAXG
// Defaults can be overridden or fallback to Yahoo
struct XStockMapping {
    standard_sym: &'static str,
    yahoo_sym: &'static str,
    solana_mint: &'static str,
}

const XSTOCK_MAPPINGS: &[XStockMapping] = &[
    XStockMapping {
        standard_sym: "SPY/USD",
        yahoo_sym: "SPY",
        solana_mint: "SPYx111111111111111111111111111111111111111", // Backed SPY
    },
    XStockMapping {
        standard_sym: "NVDA/USD",
        yahoo_sym: "NVDA",
        solana_mint: "NVDAx11111111111111111111111111111111111111", // Backed NVDA
    },
    XStockMapping {
        standard_sym: "GOOG/USD",
        yahoo_sym: "GOOG",
        solana_mint: "GOOGx11111111111111111111111111111111111111", // Backed GOOG
    },
    XStockMapping {
        standard_sym: "QQQ/USD",
        yahoo_sym: "QQQ",
        solana_mint: "QQQx111111111111111111111111111111111111111", // Backed QQQ
    },
    XStockMapping {
        standard_sym: "TSLA/USD",
        yahoo_sym: "TSLA",
        solana_mint: "TSLAx11111111111111111111111111111111111111", // Backed TSLA
    },
];

pub async fn start_xstocks_poller(coord: FeedCoordinator) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();

    let mut ticker = interval(Duration::from_secs(60));

    loop {
        ticker.tick().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // 1. Try Primary: Jupiter Price API v2 for on-chain 24/7 DEX prices
        let mint_ids: Vec<&str> = XSTOCK_MAPPINGS.iter().map(|m| m.solana_mint).collect();
        let jup_url = format!("https://api.jup.ag/price/v2?ids={}", mint_ids.join(","));

        if let Ok(resp) = client.get(&jup_url).send().await {
            if resp.status().is_success() {
                if let Ok(body) = resp.json::<JupiterApiResponse>().await {
                    if let Some(data) = body.data {
                        for mapping in XSTOCK_MAPPINGS {
                            if let Some(item) = data.get(mapping.solana_mint) {
                                if let Some(ref p_str) = item.price {
                                    if let Ok(price) = p_str.parse::<f64>() {
                                        if price > 0.0 {
                                            coord.record_tick(mapping.standard_sym, price, now, "jupiter-dex");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2. Fallback: Yahoo Finance v8 chart API if Jupiter didn't succeed or for missing assets
        for mapping in XSTOCK_MAPPINGS {
            // Check if we already have a recent tick in L1 cache from this cycle
            let is_fresh = coord
                .l1_cache
                .get(mapping.standard_sym)
                .map(|e| now - e.timestamp < 30_000)
                .unwrap_or(false);

            if !is_fresh {
                if let Some(price) = fetch_yahoo_spot(&client, mapping.yahoo_sym).await {
                    coord.record_tick(mapping.standard_sym, price, now, "yahoo-fallback");
                }
            }
        }

        // 3. PAXG Backup check on Jupiter
        if let Some(entry) = coord.l1_cache.get("PAXG/USD") {
            if now - entry.timestamp > 120_000 {
                // If Binance PAXG hasn't updated in 2 mins, query Jupiter PAXG
                let paxg_mint = "CXLBjnncvRHACoe276Bq7dBEcDZ8vj4nZ987zC7pump"; // Wrapped/Bridged PAXG
                let jup_paxg_url = format!("https://api.jup.ag/price/v2?ids={}", paxg_mint);
                if let Ok(resp) = client.get(&jup_paxg_url).send().await {
                    if let Ok(body) = resp.json::<JupiterApiResponse>().await {
                        if let Some(data) = body.data {
                            if let Some(item) = data.get(paxg_mint) {
                                if let Some(ref p_str) = item.price {
                                    if let Ok(price) = p_str.parse::<f64>() {
                                        coord.record_tick("PAXG/USD", price, now, "jupiter-backup");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn fetch_yahoo_spot(client: &reqwest::Client, ticker: &str) -> Option<f64> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1m&range=1d",
        ticker
    );

    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        return None;
    }

    let json: serde_json::Value = resp.json().await.ok()?;
    let price = json
        .get("chart")?
        .get("result")?
        .get(0)?
        .get("meta")?
        .get("regularMarketPrice")?
        .as_f64()?;

    Some(price)
}
