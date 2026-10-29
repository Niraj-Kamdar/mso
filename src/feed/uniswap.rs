//! Uniswap V3 TWAP feed (Ethereum mainnet).
//!
//! Each asset id is a V3 pool contract address. The price is **token0 quoted in
//! token1** (e.g. WETH/USDT 0.05% pool: token0 = WETH, token1 = USDT, so the
//! feed reports USDT per WETH), computed exactly like the periphery
//! `OracleLibrary.consult` + `getQuoteAtTick`:
//!
//! ```text
//! (tc_then, tc_now) = pool.observe([secondsAgo, 0]).tickCumulatives
//! meanTick          = floor((tc_now - tc_then) / secondsAgo)
//! price             = 1.0001 ^ meanTick * 10 ^ (decimals0 - decimals1)
//! ```
//!
//! Ethereum plumbing (ABI codegen, JSON-RPC, address handling, revert decoding)
//! is alloy's; this module only holds the Uniswap math and feed bookkeeping.
//! JSON-RPC requests are sent through the crate's shared `reqwest` client, so
//! the feed shares TLS, proxy and timeout behaviour with every other feed.

use alloy::primitives::Address;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::rpc::client::RpcClient;
use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use alloy::sol;
use alloy::sol_types::decode_revert_reason;
use alloy::transports::{utils::guess_local_url, TransportError, TransportErrorKind, TransportFut};
use futures_util::future::join_all;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll};
use tracing::warn;

sol! {
    /// The slice of `IUniswapV3Pool` (@uniswap/v3-core) the feed needs.
    #[sol(rpc)]
    interface IUniswapV3Pool {
        function token0() external view returns (address);
        function token1() external view returns (address);
        function observe(uint32[] calldata secondsAgos)
            external
            view
            returns (int56[] memory tickCumulatives, uint160[] memory secondsPerLiquidityCumulativeX128s);
    }

    /// ERC-20 metadata extension (EIP-20 optional `decimals`).
    #[sol(rpc)]
    interface IERC20Metadata {
        function decimals() external view returns (uint8);
    }
}

/// Environment variable holding the Ethereum mainnet JSON-RPC URL.
pub const ENV_RPC_URL: &str = "MSO_ETH_RPC_URL";

/// Ethereum mainnet chain id; other networks only trigger a warning since the
/// configured pool addresses are chain-specific.
pub const MAINNET_CHAIN_ID: u64 = 1;

/// `TickMath.MIN_TICK` / `MAX_TICK` from v3-core.
pub const MIN_TICK: i32 = -887_272;
pub const MAX_TICK: i32 = 887_272;

/// Mainnet RPC URL from `MSO_ETH_RPC_URL`.
pub fn rpc_url() -> Result<String, String> {
    rpc_url_from(std::env::var(ENV_RPC_URL).ok())
}

pub fn rpc_url_from(value: Option<String>) -> Result<String, String> {
    let raw = value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("{} is not set (Ethereum mainnet JSON-RPC URL, required for uniswap assets)", ENV_RPC_URL))?;
    match reqwest::Url::parse(&raw) {
        Ok(u) if matches!(u.scheme(), "http" | "https") && u.host_str().is_some() => Ok(raw),
        _ => Err(format!("{} must be an http(s) URL, got '{}'", ENV_RPC_URL, redact_url(&raw))),
    }
}

/// Scheme + host(:port) only: RPC URLs usually carry an API key in the path or query.
pub fn redact_url(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(u) => match (u.host_str(), u.port()) {
            (Some(h), Some(p)) => format!("{}://{}:{}/...", u.scheme(), h, p),
            (Some(h), None) => format!("{}://{}/...", u.scheme(), h),
            (None, _) => format!("{}:...", u.scheme()),
        },
        Err(_) => "<invalid url>".into(),
    }
}

/// Immutable pool facts read from the chain once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolMeta {
    pub token0: Address,
    pub token1: Address,
    pub decimals0: u8,
    pub decimals1: u8,
}

/// alloy transport over the crate's shared `reqwest` client: one JSON-RPC POST per
/// request packet, with errors kept as a full source chain (and without the URL,
/// which usually embeds an API key).
#[derive(Clone)]
struct SharedHttp {
    client: reqwest::Client,
    url: reqwest::Url,
}

impl tower::Service<RequestPacket> for SharedHttp {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            let resp = this
                .client
                .post(this.url)
                .json(&req)
                .send()
                .await
                .map_err(|e| TransportErrorKind::custom(e.without_url()))?;
            let status = resp.status();
            let body = resp.bytes().await.map_err(|e| TransportErrorKind::custom(e.without_url()))?;
            if !status.is_success() {
                return Err(TransportErrorKind::http_error(status.as_u16(), String::from_utf8_lossy(&body).into_owned()));
            }
            serde_json::from_slice(&body).map_err(|e| TransportError::deser_err(e, String::from_utf8_lossy(&body)))
        })
    }
}

/// A mainnet RPC connection plus per-pool metadata cache.
pub struct UniswapClient {
    provider: DynProvider,
    /// Raw and normalized URL forms, scrubbed from every error we surface.
    secrets: [String; 2],
    redacted: String,
    meta: Mutex<HashMap<Address, PoolMeta>>,
    chain_checked: AtomicBool,
}

impl UniswapClient {
    /// `http` is the shared feed client (timeouts, TLS and proxy settings included).
    pub fn new(http: reqwest::Client, rpc_url: &str) -> Result<Self, String> {
        let url = reqwest::Url::parse(rpc_url).map_err(|e| format!("invalid {}: {}", ENV_RPC_URL, e))?;
        let transport = SharedHttp { client: http, url: url.clone() };
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers() // read-only: no gas/nonce/signing fillers
            .connect_client(RpcClient::new(transport, guess_local_url(rpc_url)))
            .erased();
        Ok(Self {
            provider,
            secrets: [rpc_url.to_string(), url.to_string()],
            redacted: redact_url(rpc_url),
            meta: Mutex::default(),
            chain_checked: AtomicBool::new(false),
        })
    }

    pub fn cached_meta(&self, pool: Address) -> Option<PoolMeta> {
        self.meta.lock().unwrap().get(&pool).cloned()
    }

    /// TWAP price for every `(pool id, seconds_ago)` pair, token0 quoted in token1.
    /// Ids are echoed back unchanged. Pools that fail are logged and skipped; `Err`
    /// only when nothing succeeded (e.g. the RPC endpoint is down).
    pub async fn fetch_twaps(&self, pools: &[(String, u32)]) -> Result<Vec<(String, f64)>, String> {
        if pools.is_empty() {
            return Ok(Vec::new());
        }
        self.check_chain_once().await;

        let results = join_all(pools.iter().map(|(id, secs)| async move {
            let res = match id.parse::<Address>() {
                Ok(pool) => self.fetch_twap(pool, *secs).await,
                Err(e) => Err(format!("invalid pool address: {}", e)),
            };
            (id, res)
        }))
        .await;

        let mut out = Vec::with_capacity(pools.len());
        let mut first_err: Option<String> = None;
        for (id, res) in results {
            match res {
                Ok(price) => out.push((id.clone(), price)),
                Err(e) => {
                    warn!("[uniswap] {}: {}", id, e);
                    first_err.get_or_insert(format!("{}: {}", id, e));
                }
            }
        }
        match (out.is_empty(), first_err) {
            (true, Some(e)) => Err(e),
            _ => Ok(out),
        }
    }

    /// One pool's TWAP over the last `seconds_ago` seconds (token0 in token1 units).
    pub async fn fetch_twap(&self, pool: Address, seconds_ago: u32) -> Result<f64, String> {
        if seconds_ago == 0 {
            return Err("twap window must be > 0 seconds".into());
        }
        let meta = self.pool_meta(pool).await?;
        let obs = IUniswapV3Pool::new(pool, &self.provider)
            .observe(vec![seconds_ago, 0])
            .call()
            .await
            .map_err(|e| self.contract_err(&format!("observe([{}, 0])", seconds_ago), e))?;
        let [then, now] = obs.tickCumulatives[..] else {
            return Err(format!("observe() returned {} tickCumulatives, expected 2", obs.tickCumulatives.len()));
        };
        let tick = mean_tick(then.as_i64(), now.as_i64(), seconds_ago)?;
        let price = tick_to_price(tick, meta.decimals0, meta.decimals1);
        if !(price.is_finite() && price > 0.0) {
            return Err(format!("computed non-positive price {} from tick {}", price, tick));
        }
        Ok(price)
    }

    /// token0/token1 and their ERC-20 decimals, cached after first use.
    pub async fn pool_meta(&self, pool: Address) -> Result<PoolMeta, String> {
        if let Some(m) = self.cached_meta(pool) {
            return Ok(m);
        }
        let contract = IUniswapV3Pool::new(pool, &self.provider);
        let token0 = contract.token0().call().await.map_err(|e| self.contract_err("token0()", e))?;
        let token1 = contract.token1().call().await.map_err(|e| self.contract_err("token1()", e))?;
        let decimals = |token: Address| async move {
            IERC20Metadata::new(token, &self.provider).decimals().call().await
        };
        let decimals0 = decimals(token0).await.map_err(|e| self.contract_err("token0 decimals()", e))?;
        let decimals1 = decimals(token1).await.map_err(|e| self.contract_err("token1 decimals()", e))?;
        let meta = PoolMeta { token0, token1, decimals0, decimals1 };
        self.meta.lock().unwrap().insert(pool, meta.clone());
        Ok(meta)
    }

    async fn check_chain_once(&self) {
        if self.chain_checked.swap(true, Ordering::SeqCst) {
            return;
        }
        match self.provider.get_chain_id().await {
            Ok(MAINNET_CHAIN_ID) => {}
            Ok(id) => warn!(
                "[uniswap] {} points at chain id {}, not Ethereum mainnet ({}); pool addresses are chain-specific",
                self.redacted, id, MAINNET_CHAIN_ID
            ),
            // Not fatal: the eth_calls will surface a real outage. Retry next poll.
            Err(e) => {
                warn!("[uniswap] eth_chainId failed: {}", self.scrub(&e));
                self.chain_checked.store(false, Ordering::SeqCst);
            }
        }
    }

    /// alloy error -> message with the decoded revert reason, an operator hint
    /// for Uniswap's short revert codes, and no RPC secrets.
    fn contract_err(&self, call: &str, e: alloy::contract::Error) -> String {
        let reason = e.as_revert_data().and_then(|d| decode_revert_reason(&d));
        let hint = match reason.as_deref() {
            Some(r) if r.contains("OLD") => {
                " [pool has too little observation history for this twap window: shorten the window or call increaseObservationCardinalityNext on the pool]"
            }
            _ => "",
        };
        // alloy renders it as e.g. "revert: OLD"
        let reason = reason.map(|r| format!(" ({})", r)).unwrap_or_default();
        format!("{} failed: {}{}{}", call, self.scrub(&e), reason, hint)
    }

    /// Full error chain (the root cause lives several `source()` hops down), with
    /// the RPC URL replaced in case anything upstream echoed it.
    fn scrub(&self, e: &dyn std::error::Error) -> String {
        self.secrets
            .iter()
            .fold(error_chain(e), |msg, secret| msg.replace(secret.as_str(), &self.redacted))
    }
}

/// `"outer: inner: root cause"`, skipping layers that only repeat their source.
pub fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(src) = cur {
        let msg = src.to_string();
        if !out.contains(&msg) {
            out.push_str(": ");
            out.push_str(&msg);
        }
        cur = src.source();
    }
    out
}

/// Arithmetic mean tick over the window, floored toward negative infinity
/// exactly like `OracleLibrary.consult`.
pub fn mean_tick(tick_cumulative_then: i64, tick_cumulative_now: i64, seconds_ago: u32) -> Result<i32, String> {
    if seconds_ago == 0 {
        return Err("twap window must be > 0 seconds".into());
    }
    let delta = tick_cumulative_now
        .checked_sub(tick_cumulative_then)
        .ok_or("tickCumulative delta overflow")?;
    let tick = delta.div_euclid(seconds_ago as i64); // positive divisor: euclid == floor
    if tick < MIN_TICK as i64 || tick > MAX_TICK as i64 {
        return Err(format!("mean tick {} outside [{}, {}]", tick, MIN_TICK, MAX_TICK));
    }
    Ok(tick as i32)
}

/// Price of one whole token0 in whole token1 units at `tick`
/// (`1.0001^tick` is token1-per-token0 in raw on-chain units).
pub fn tick_to_price(tick: i32, decimals0: u8, decimals1: u8) -> f64 {
    1.0001f64.powi(tick) * 10f64.powi(decimals0 as i32 - decimals1 as i32)
}

// ---------------------------------------------------------------------------
// Test support: an in-process JSON-RPC server that behaves like a V3 pool.
// Calls are decoded and results encoded with the same alloy bindings.
// ---------------------------------------------------------------------------

#[cfg(test)]
pub mod mock {
    use super::*;
    use alloy::primitives::{address, aliases::I56, Bytes, U160};
    use alloy::sol_types::{Revert, SolCall, SolError, SolInterface};
    use axum::{extract::State, routing::post, Json, Router};
    use serde_json::{json, Value};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use IUniswapV3Pool::IUniswapV3PoolCalls as PoolCall;

    #[derive(Clone)]
    pub struct MockPool {
        pub token0: Address,
        pub token1: Address,
        /// Constant tick the pool has been at forever, so any TWAP equals it.
        pub tick: i64,
        /// Make `observe()` revert with this `Error(string)` reason.
        pub revert: Option<&'static str>,
    }

    #[derive(Default)]
    pub struct MockState {
        pub pools: HashMap<Address, MockPool>,
        pub decimals: HashMap<Address, u8>,
        pub chain_id: u64,
        pub mode: Mutex<Mode>,
        /// (to, function name) of every eth_call
        pub calls: Mutex<Vec<(Address, &'static str)>>,
        pub observe_args: Mutex<Vec<Vec<u32>>>,
        pub requests: AtomicUsize,
    }

    /// Respond normally, with HTTP 500, or with a non-JSON body.
    #[derive(Clone, Copy, PartialEq, Eq, Default)]
    pub enum Mode {
        #[default]
        Ok,
        Http500,
        NotJson,
    }

    pub const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
    pub const USDT: Address = address!("dAC17F958D2ee523a2206206994597C13D831ec7");
    pub const WETH_USDT_POOL: Address = address!("11b815efB8f581194ae79006d24E0d814B7697F6");

    /// State with a WETH/USDT pool pinned at `tick` (decimals 18 / 6).
    pub fn weth_usdt_state(tick: i64) -> MockState {
        let mut st = MockState { chain_id: 1, ..Default::default() };
        st.pools.insert(WETH_USDT_POOL, MockPool { token0: WETH, token1: USDT, tick, revert: None });
        st.decimals.insert(WETH, 18);
        st.decimals.insert(USDT, 6);
        st
    }

    /// Spawns the server; returns its URL.
    pub async fn serve(state: Arc<MockState>) -> String {
        let app = Router::new().route("/", post(handle)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{}", addr)
    }

    async fn handle(State(st): State<Arc<MockState>>, Json(req): Json<Value>) -> axum::response::Response {
        use axum::response::IntoResponse;
        st.requests.fetch_add(1, Ordering::SeqCst);
        match *st.mode.lock().unwrap() {
            Mode::Http500 => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response(),
            Mode::NotJson => return "<html>not json</html>".into_response(),
            Mode::Ok => {}
        }
        let id = req["id"].clone();
        let ok = |result: Value| Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })).into_response();
        let fail = |error: Value| Json(json!({ "jsonrpc": "2.0", "id": id, "error": error })).into_response();

        match req["method"].as_str() {
            Some("eth_chainId") => ok(json!(format!("0x{:x}", st.chain_id))),
            Some("eth_call") => {
                let tx = &req["params"][0];
                let to: Address = tx["to"].as_str().unwrap().parse().unwrap();
                let input = tx.get("input").or_else(|| tx.get("data")).and_then(Value::as_str).unwrap();
                let data: Bytes = input.parse().unwrap();
                let pool = st.pools.get(&to);

                let (name, ret): (&'static str, Vec<u8>) = match (pool, PoolCall::abi_decode(&data)) {
                    (Some(p), Ok(PoolCall::token0(_))) => {
                        ("token0", IUniswapV3Pool::token0Call::abi_encode_returns(&p.token0))
                    }
                    (Some(p), Ok(PoolCall::token1(_))) => {
                        ("token1", IUniswapV3Pool::token1Call::abi_encode_returns(&p.token1))
                    }
                    (Some(p), Ok(PoolCall::observe(call))) => {
                        st.observe_args.lock().unwrap().push(call.secondsAgos.clone());
                        if let Some(reason) = p.revert {
                            let data = Bytes::from(Revert::from(reason).abi_encode());
                            return fail(json!({ "code": 3, "message": "execution reverted", "data": data }));
                        }
                        ("observe", encode_observe(p.tick, &call.secondsAgos))
                    }
                    _ => match (st.decimals.get(&to), IERC20Metadata::decimalsCall::abi_decode(&data)) {
                        (Some(d), Ok(_)) => ("decimals", IERC20Metadata::decimalsCall::abi_encode_returns(d)),
                        _ => ("<none>", Vec::new()), // no contract here -> "0x"
                    },
                };
                st.calls.lock().unwrap().push((to, name));
                ok(json!(Bytes::from(ret)))
            }
            _ => fail(json!({ "code": -32601, "message": "method not found" })),
        }
    }

    /// tickCumulative grows by `tick` every second; "now" is an arbitrary base.
    pub fn encode_observe(tick: i64, seconds_agos: &[u32]) -> Vec<u8> {
        const NOW: i64 = 1_700_000_000;
        IUniswapV3Pool::observeCall::abi_encode_returns(&IUniswapV3Pool::observeReturn {
            tickCumulatives: seconds_agos
                .iter()
                .map(|&a| I56::try_from(tick * (NOW - a as i64)).unwrap())
                .collect(),
            secondsPerLiquidityCumulativeX128s: vec![U160::ZERO; seconds_agos.len()],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::hex;
    use alloy::sol_types::SolCall;

    #[test]
    fn test_interface_matches_known_selectors() {
        // Guards the sol! signatures against typos: cross-checked with an independent keccak256.
        assert_eq!(IUniswapV3Pool::observeCall::SELECTOR, hex!("883bdbfd"));
        assert_eq!(IUniswapV3Pool::token0Call::SELECTOR, hex!("0dfe1681"));
        assert_eq!(IUniswapV3Pool::token1Call::SELECTOR, hex!("d21220a7"));
        assert_eq!(IERC20Metadata::decimalsCall::SELECTOR, hex!("313ce567"));
        // ...and the calldata alloy produces for observe([600, 0])
        let data = IUniswapV3Pool::observeCall { secondsAgos: vec![600, 0] }.abi_encode();
        assert_eq!(
            hex::encode(data),
            "883bdbfd\
             0000000000000000000000000000000000000000000000000000000000000020\
             0000000000000000000000000000000000000000000000000000000000000002\
             0000000000000000000000000000000000000000000000000000000000000258\
             0000000000000000000000000000000000000000000000000000000000000000"
        );
    }

    #[test]
    fn test_observe_return_decodes_negative_tick_cumulatives() {
        // Negative ticks (every ETH/stable pool) exercise int56 sign extension in alloy's decoder.
        let ret = mock::encode_observe(-198_000, &[600, 0]);
        let obs = IUniswapV3Pool::observeCall::abi_decode_returns(&ret).unwrap();
        let [then, now] = obs.tickCumulatives[..] else { panic!("expected 2") };
        assert_eq!(mean_tick(then.as_i64(), now.as_i64(), 600).unwrap(), -198_000);
        // Truncated payloads are rejected by the decoder
        assert!(IUniswapV3Pool::observeCall::abi_decode_returns(&ret[..ret.len() - 32]).is_err());
    }

    #[test]
    fn test_mean_tick_floors_toward_negative_infinity() {
        // Exact division
        assert_eq!(mean_tick(0, 6_000, 60).unwrap(), 100);
        assert_eq!(mean_tick(0, -6_000, 60).unwrap(), -100);
        // Inexact: positive truncates, negative floors (matches OracleLibrary)
        assert_eq!(mean_tick(0, 6_059, 60).unwrap(), 100);
        assert_eq!(mean_tick(0, -6_001, 60).unwrap(), -101);
        assert_eq!(mean_tick(100, 99, 60).unwrap(), -1);
        // Failure cases
        assert!(mean_tick(0, 1, 0).is_err());
        assert!(mean_tick(0, (MAX_TICK as i64 + 1) * 60, 60).is_err());
        assert!(mean_tick(0, (MIN_TICK as i64 - 1) * 60, 60).is_err());
        assert!(mean_tick(i64::MIN, i64::MAX, 60).is_err());
    }

    #[test]
    fn test_tick_to_price_applies_decimals() {
        // Tick 0 = 1 raw token1 per raw token0
        assert_eq!(tick_to_price(0, 18, 18), 1.0);
        assert!((tick_to_price(0, 18, 6) - 1e12).abs() < 1e-3);
        assert!((tick_to_price(0, 6, 18) - 1e-12).abs() < 1e-24);
        // WETH/USDT-like tick: 1.0001^-198000 * 1e12 ~= 2519.99
        let p = tick_to_price(-198_000, 18, 6);
        assert!((p - 2519.9921).abs() < 0.01, "{}", p);
        // Inverse ticks give inverse prices
        let a = tick_to_price(50_000, 18, 18);
        let b = tick_to_price(-50_000, 18, 18);
        assert!((a * b - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_rpc_url_parsing_and_redaction() {
        assert!(rpc_url_from(None).unwrap_err().contains(ENV_RPC_URL));
        assert!(rpc_url_from(Some("   ".into())).is_err());
        assert!(rpc_url_from(Some("wss://mainnet.example".into())).is_err());
        assert!(rpc_url_from(Some("mainnet.example".into())).is_err());
        assert!(rpc_url_from(Some("https://".into())).is_err());
        assert_eq!(rpc_url_from(Some(" https://eth.example/v3/KEY ".into())).unwrap(), "https://eth.example/v3/KEY");

        assert_eq!(redact_url("https://eth.example/v3/SECRET"), "https://eth.example/...");
        assert_eq!(redact_url("http://127.0.0.1:8545"), "http://127.0.0.1:8545/...");
        assert_eq!(redact_url("https://x.io?apikey=SECRET"), "https://x.io/...");
        assert_eq!(redact_url("https://user:SECRET@x.io/"), "https://x.io/...");
        assert_eq!(redact_url("garbage"), "<invalid url>");
    }

    #[test]
    fn test_client_rejects_bad_url() {
        let http = reqwest::Client::new();
        assert!(UniswapClient::new(http.clone(), "not a url").is_err());
        assert!(UniswapClient::new(http, "http://127.0.0.1:8545").is_ok());
    }

    #[test]
    fn test_error_chain_includes_root_cause_once() {
        #[derive(Debug)]
        struct Layer(&'static str, Option<Box<Layer>>);
        impl std::fmt::Display for Layer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Layer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.1.as_deref().map(|l| l as &dyn std::error::Error)
            }
        }
        let root = Layer("Connection refused (os error 111)", None);
        let mid = Layer("client error (Connect)", Some(Box::new(root)));
        let top = Layer("error sending request", Some(Box::new(mid)));
        assert_eq!(error_chain(&top), "error sending request: client error (Connect): Connection refused (os error 111)");
        // A transparent wrapper that just repeats its source is not duplicated
        let inner = Layer("HTTP error 405", None);
        let transparent = Layer("HTTP error 405", Some(Box::new(inner)));
        assert_eq!(error_chain(&transparent), "HTTP error 405");
    }
}




