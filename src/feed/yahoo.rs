/// Spot prices from Yahoo Finance v8 chart API, one request per ticker.
/// Tickers that fail are skipped so one bad ticker doesn't drop the rest.
pub async fn fetch_spots(client: &reqwest::Client, tickers: &[String]) -> Result<Vec<(String, f64)>, String> {
    let mut out = Vec::new();
    for t in tickers {
        if let Some(p) = fetch_spot(client, t).await {
            out.push((t.clone(), p));
        }
    }
    Ok(out)
}

async fn fetch_spot(client: &reqwest::Client, ticker: &str) -> Option<f64> {
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
    json.get("chart")?
        .get("result")?
        .get(0)?
        .get("meta")?
        .get("regularMarketPrice")?
        .as_f64()
}
