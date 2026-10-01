use serde::Deserialize;
use std::collections::HashMap;

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

/// Prices for Solana mints in one Jupiter v3 call.
/// Prefers stockData.price (Backed oracle) over usdPrice (DEX last swap).
pub async fn fetch_prices(client: &reqwest::Client, mints: &[String]) -> Result<Vec<(String, f64)>, String> {
    let url = format!("https://api.jup.ag/price/v3?ids={}", mints.join(","));
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("status {}", resp.status()));
    }
    let body: JupiterApiResponse = resp.json().await.map_err(|e| e.to_string())?;
    Ok(body
        .into_iter()
        .filter_map(|(mint, item)| {
            let price = item.stock_data.and_then(|sd| sd.price).or(item.usd_price)?;
            Some((mint, price))
        })
        .collect())
}
