use serde::Deserialize;
use std::collections::HashMap;
use tokio::time::{interval, Duration};

use super::FeedCoordinator;

// Jupiter Price API v3 response: usdPrice is an f64, not a string.
// The `stockData.price` field gives the authoritative reference price from
// xStocks/Backed for regulated RWA tokens (when available).
#[derive(Deserialize)]
struct StockData {
    price: Option<f64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JupiterPriceData {
    usd_price: Option<f64>,
    stock_data: Option<StockData>,
}

// Jupiter Price API v3: response is a flat map of mint -> PriceData (no `data` wrapper)
type JupiterApiResponse = HashMap<String, JupiterPriceData>;

// Verified Solana Token-2022 mint addresses for Backed Finance xStocks.
// All confirmed live on api.jup.ag/price/v3 as of 2026-10.
struct XStockMapping {
    standard_sym: &'static str,
    yahoo_sym: &'static str,
    solana_mint: &'static str,
}

const XSTOCK_MAPPINGS: &[XStockMapping] = &[
    XStockMapping {
        standard_sym: "SPY/USD",
        yahoo_sym: "SPY",
        solana_mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W", // Backed SPYx (SP500 xStock)
    },
    XStockMapping {
        standard_sym: "NVDA/USD",
        yahoo_sym: "NVDA",
        solana_mint: "Xsc9qvGR1efVDFGLrVsmkzv3qi45LTBjeUKSPmx9qEh", // Backed NVDAx (NVIDIA xStock)
    },
    XStockMapping {
        standard_sym: "GOOG/USD",
        yahoo_sym: "GOOGL",
        solana_mint: "XsCPL9dNWBMvFtTmwcCA5v3xWPSMEBCszbQdiLLq6aN", // Backed GOOGLx (Alphabet xStock)
    },
    XStockMapping {
        standard_sym: "QQQ/USD",
        yahoo_sym: "QQQ",
        solana_mint: "Xs8S1uUs1zvS2p7iwtsG3b6fkhpvmwz4GYU3gWAmWHZ", // Backed QQQx (Nasdaq-100 xStock)
    },
    XStockMapping {
        standard_sym: "TSLA/USD",
        yahoo_sym: "TSLA",
        solana_mint: "XsDoVfqeBukxuZHWhdvWHBhgEHjGNst4MLodqsJHzoB", // Backed TSLAx (Tesla xStock)
    },
];

// Native Paxos PAXG on Solana (Token-2022, native issuance since Jun 2026)
const PAXG_MINT: &str = "5GgRAEmv8ZxF2PR5hY72Qs5x1bnQ6UK2RbTPoqJ3wSwW";

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

        // 1. Try Primary: Jupiter Price API v3 for on-chain 24/7 DEX prices.
        //    For xStocks, v3 also returns stockData.price (authoritative reference price),
        //    which we prefer over the DEX usdPrice when available.
        let mint_ids: Vec<&str> = XSTOCK_MAPPINGS.iter().map(|m| m.solana_mint).collect();
        let jup_url = format!("https://api.jup.ag/price/v3?ids={}", mint_ids.join(","));

        if let Ok(resp) = client.get(&jup_url).send().await {
            if resp.status().is_success() {
                if let Ok(body) = resp.json::<JupiterApiResponse>().await {
                    for mapping in XSTOCK_MAPPINGS {
                        if let Some(item) = body.get(mapping.solana_mint) {
                            // Prefer stockData.price (Backed oracle) > usdPrice (DEX last swap)
                            let price = item
                                .stock_data
                                .as_ref()
                                .and_then(|sd| sd.price)
                                .or(item.usd_price);

                            if let Some(p) = price {
                                if p > 0.0 {
                                    coord.record_tick(mapping.standard_sym, p, now, "jupiter-dex");
                                }
                            }
                        }
                    }
                }
            }
        }

        // 2. Fallback: Yahoo Finance v8 chart API for any symbol Jupiter didn't return.
        for mapping in XSTOCK_MAPPINGS {
            // Check if Jupiter already produced a fresh tick in this cycle (within 30s)
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

        // 3. PAXG backup check on Jupiter v3.
        //    Binance is the primary PAXG source; only hit Jupiter if Binance is stale > 2 min.
        if let Some(entry) = coord.l1_cache.get("PAXG/USD") {
            if now - entry.timestamp > 120_000 {
                let paxg_url = format!("https://api.jup.ag/price/v3?ids={}", PAXG_MINT);
                if let Ok(resp) = client.get(&paxg_url).send().await {
                    if resp.status().is_success() {
                        if let Ok(body) = resp.json::<JupiterApiResponse>().await {
                            if let Some(item) = body.get(PAXG_MINT) {
                                if let Some(p) = item.usd_price {
                                    if p > 0.0 {
                                        coord.record_tick("PAXG/USD", p, now, "jupiter-backup");
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
