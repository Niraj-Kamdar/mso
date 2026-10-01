use std::collections::HashMap;

/// Mid prices for the requested Hyperliquid coins (e.g. HYPE, SOL) from one `allMids` call.
pub async fn fetch_mids(client: &reqwest::Client, coins: &[String]) -> Result<Vec<(String, f64)>, String> {
    let resp = client
        .post("https://api.hyperliquid.xyz/info")
        .json(&serde_json::json!({ "type": "allMids" }))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("allMids returned status {}", resp.status()));
    }
    let mids: HashMap<String, String> = resp.json().await.map_err(|e| e.to_string())?;
    Ok(coins
        .iter()
        .filter_map(|c| Some((c.clone(), mids.get(c)?.parse().ok()?)))
        .collect())
}
