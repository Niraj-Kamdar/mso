# Push Price Feeds Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let external processes push prices to `POST /api/v1/ticks` for `type = "push"` sources, with write-scoped API keys.

**Architecture:** A `SourceRef { Builtin(Source), Push(String) }` replaces the closed `Source` enum on assets/fallbacks. `FeedCoordinator` carries a `&'static Config` (so tests can inject a push config) and `ingest()` accepts `impl Into<SourceRef>`. The endpoint validates, then calls `ingest()`. Keys get a nullable `write_sources` column.

**Tech Stack:** Rust 2021, axum 0.7, rusqlite 0.31, serde/toml 0.9, clap 4. Tests: `cargo test`.

**Spec:** `docs/superpowers/specs/2026-10-03-push-feeds-design.md` (three small deviations are recorded in Task 5).

## Global Constraints

- Only `type = "push"` is accepted under `[sources.<name>]`; `every` minimum is 1s (`"60s"`, `"5m"`).
- `fallback_after_ms = max(2 × every, 5s)` for pushed assets (existing rule, unchanged).
- Batch limit: 1000 ticks; body limit 256 KB (`413`). `time` at most 5 minutes in the future and not older than tick retention.
- Key with no write scope = read-only, exactly as today. A key can never be scoped to a built-in source.
- Existing built-in sources, `[poll_secs]` and default config are unchanged.
- Commits: conventional, descriptive scope (e.g. `feat(config):`), no AI mentions, no `Co-Authored-By`, `git add` specific files only. The working tree has unrelated edits (`src/feed/jupiter.rs`, `mso.toml`): never stage them.
- Baseline: `cargo test` passes 16 tests before starting.

## Review Focus

- Out-of-order batch (`time` descending): ticks are applied oldest-first so the cache ends on the newest. (Task 4 test)
- Same `id` twice with the same explicit `time`: both count as accepted, last one wins (`INSERT OR REPLACE`). (Task 4 test)
- Empty `ticks: []`: `200 {"accepted":0,"rejected":[]}`. (Task 4 test)
- `source` is case-sensitive (`"NAV"` is not `"nav"`) and a push name containing `,` or spaces is rejected at config load. (Tasks 1, 4)
- Garbage or wrong-content-type bodies give a 4xx, never a panic. (Task 4 test)

---

### Task 1: Config — push sources and `SourceRef`

**Files:**
- Modify: `src/config.rs`

**Interfaces:**
- Produces:
  - `Source::from_label(&str) -> Option<Source>`
  - `enum SourceRef { Builtin(Source), Push(String) }` with `label(&self) -> &str`, `is(&self, Source) -> bool`, `From<Source>`
  - `Config.sources: BTreeMap<String, PushSource>`; `PushSource.every: Duration`
  - `Config::resolution(&self, &SourceRef) -> Duration`, `Config::is_push_source(&self, &str) -> bool`, `Config::knows(&self, &SourceRef, &str) -> bool`, `Config::base_resolution_ms(&self, &str) -> u64`, `Config::fallback_after_ms(&self, &str) -> u64`
  - `Asset.source` and `Fallback.source` are `SourceRef`. Free fns `base_resolution_ms` / `fallback_after_ms` keep their signatures and delegate to `get()`.

- [ ] **Step 1: Write the failing tests**

Add inside `mod tests` in `src/config.rs`:

```rust
    const PUSH_CFG: &str = r#"
        [sources.my-service]
        type = "push"
        every = "30s"

        [[assets]]
        symbol = "fund-a/nav"
        source = "my-service"
        id = "fund-a"
        fallback = { source = "binance", id = "xyz" }
    "#;

    #[test]
    fn test_push_source_config() {
        let cfg = Config::parse(PUSH_CFG).unwrap();
        let nav = SourceRef::Push("my-service".into());
        assert_eq!(cfg.sources["my-service"].every, Duration::from_secs(30));
        assert_eq!(cfg.assets[0].source, nav);
        assert_eq!(cfg.base_resolution_ms("FUND-A/NAV"), 30_000);
        assert_eq!(cfg.fallback_after_ms("FUND-A/NAV"), 60_000);
        assert!(cfg.is_push_source("my-service"));
        assert!(!cfg.is_push_source("binance"));
        assert!(cfg.knows(&nav, "fund-a"));
        assert!(!cfg.knows(&nav, "nope"));
        assert!(cfg.knows(&SourceRef::Builtin(Source::Binance), "XYZ")); // fallback leg, uppercased
        assert_eq!(cfg.ids_for(Source::Binance), vec!["XYZ"]);
        // 5m and 1s-minimum parsing
        let c = Config::parse(&PUSH_CFG.replace("30s", "5m")).unwrap();
        assert_eq!(c.sources["my-service"].every, Duration::from_secs(300));
    }

    #[test]
    fn test_push_source_config_rejected() {
        let asset = "[[assets]]\nsymbol='A'\nsource='my'\nid='x'\n";
        let src = |extra: &str| format!("[sources.my]\ntype='push'\n{extra}\n{asset}");
        assert!(Config::parse(&src("every='5s'")).is_ok());
        assert!(Config::parse(&src("every='0s'")).is_err()); // below minimum
        assert!(Config::parse(&src("every='5x'")).is_err()); // bad unit
        assert!(Config::parse(&src("")).is_err()); // every required
        assert!(Config::parse(&src("every='5s'\nevry='1s'")).is_err()); // typo
        assert!(Config::parse(&format!("[sources.my]\ntype='poll'\nevery='5s'\n{asset}")).is_err());
        assert!(Config::parse(asset).is_err()); // undeclared source
        assert!(Config::parse(
            "[sources.binance]\ntype='push'\nevery='5s'\n[[assets]]\nsymbol='A'\nsource='binance'\nid='X'"
        )
        .is_err()); // collides with built-in label
        assert!(Config::parse(&format!("[sources.'a,b']\ntype='push'\nevery='5s'\n{}", asset.replace("'my'", "'a,b'"))).is_err());
        assert!(Config::parse(&format!("[sources.'a b']\ntype='push'\nevery='5s'\n{}", asset.replace("'my'", "'a b'"))).is_err());
        // undeclared push name as a fallback
        assert!(Config::parse(&src("every='5s'").replace("id='x'", "id='x'\nfallback={source='ghost',id='y'}")).is_err());
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test config::`
Expected: FAIL — compile errors (`SourceRef`, `sources`, `knows` not defined).

- [ ] **Step 3: Implement**

In `src/config.rs`:

1. Imports: replace the top `use serde::Deserialize;` with
```rust
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;
```

2. After `impl Source { ... }` add:
```rust
impl Source {
    pub fn from_label(s: &str) -> Option<Source> {
        Source::ALL.into_iter().find(|b| b.label() == s)
    }
}

/// Where an asset's price comes from: a compiled-in poller, or a named push source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRef {
    Builtin(Source),
    Push(String),
}

impl SourceRef {
    pub fn label(&self) -> &str {
        match self {
            SourceRef::Builtin(s) => s.label(),
            SourceRef::Push(name) => name,
        }
    }

    pub fn is(&self, source: Source) -> bool {
        *self == SourceRef::Builtin(source)
    }

    fn is_binance(&self) -> bool {
        matches!(self, SourceRef::Builtin(Source::Binance | Source::BinanceWs))
    }
}

impl From<Source> for SourceRef {
    fn from(s: Source) -> Self {
        SourceRef::Builtin(s)
    }
}

impl<'de> Deserialize<'de> for SourceRef {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(Source::from_label(&s).map_or(SourceRef::Push(s), SourceRef::Builtin))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PushKind {
    Push,
}

/// `[sources.<name>]` with `type = "push"`: no poller, assets change only when written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushSource {
    #[serde(rename = "type")]
    #[allow(dead_code)]
    kind: PushKind,
    /// Expected update cadence; drives staleness and fallback like a poll interval.
    #[serde(deserialize_with = "de_every")]
    pub every: Duration,
}

fn de_every<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let s = String::deserialize(d)?;
    parse_every(&s).map_err(serde::de::Error::custom)
}

/// "60s" or "5m"; at least one second.
fn parse_every(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, mult) = if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else {
        return Err(format!("invalid duration '{s}' (use e.g. \"60s\" or \"5m\")"));
    };
    let n: u64 = num.trim().parse().map_err(|_| format!("invalid duration '{s}'"))?;
    match n.saturating_mul(mult) {
        0 => Err("every must be >= 1s".into()),
        secs => Ok(Duration::from_secs(secs)),
    }
}
```

3. Change `Fallback.source` and `Asset.source` from `Source` to `SourceRef`.

4. In `Config` add (after `poll_secs`): 
```rust
    #[serde(default)]
    pub sources: BTreeMap<String, PushSource>,
```

5. In `validate()`, right after the retention check, add:
```rust
        for name in self.sources.keys() {
            if Source::from_label(name).is_some() {
                return Err(format!("source '{name}' collides with a built-in source"));
            }
            if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
                return Err(format!("source name '{name}' may only use letters, digits, '-' and '_'"));
            }
        }
```
and replace the body of the asset loop's binance handling / add reference checks so the loop reads:
```rust
        for a in &mut self.assets {
            a.symbol = a.symbol.trim().to_uppercase();
            if a.symbol.is_empty() || a.id.trim().is_empty() {
                return Err("assets need a non-empty symbol and id".into());
            }
            if !seen.insert(a.symbol.clone()) {
                return Err(format!("duplicate asset symbol '{}'", a.symbol));
            }
            let legs = std::iter::once(&mut a.source).chain(a.fallback.iter_mut().map(|f| &mut f.source));
            for s in legs {
                if let SourceRef::Push(name) = s {
                    if !self.sources.contains_key(name.as_str()) {
                        return Err(format!("unknown source '{name}' (not built-in, not in [sources])"));
                    }
                }
            }
            // Binance reports pairs uppercase; other sources' ids are case-sensitive.
            if a.source.is_binance() {
                a.id = a.id.to_uppercase();
            }
            if let Some(f) = &mut a.fallback {
                if f.source.is_binance() {
                    f.id = f.id.to_uppercase();
                }
            }
        }
```
(The `legs` borrow of `a.source` and `a.fallback` are disjoint fields, so this compiles; if the borrow checker objects, split into two `if` blocks checking `a.source` then `a.fallback`.)

6. Replace `ids_for` body's leg comparison and add the new methods:
```rust
    pub fn resolution(&self, source: &SourceRef) -> Duration {
        match source {
            SourceRef::Builtin(s) => self.poll_interval(*s),
            SourceRef::Push(name) => self.sources[name].every, // validated at load
        }
    }

    pub fn is_push_source(&self, name: &str) -> bool {
        self.sources.contains_key(name)
    }

    /// True if `(source, id)` is the primary or a fallback leg of some asset.
    pub fn knows(&self, source: &SourceRef, id: &str) -> bool {
        self.assets.iter().any(|a| {
            (a.source == *source && a.id == id)
                || a.fallback.as_ref().is_some_and(|f| f.source == *source && f.id == id)
        })
    }

    pub fn base_resolution_ms(&self, symbol: &str) -> u64 {
        match self.asset(symbol) {
            Some(a) => self.resolution(&a.source).as_millis() as u64,
            None => 60_000,
        }
    }

    /// Age past which an asset's fallback source may overwrite its price.
    pub fn fallback_after_ms(&self, symbol: &str) -> u64 {
        std::cmp::max(2 * self.base_resolution_ms(symbol), 5_000)
    }
```
In `ids_for`, change the loop to
```rust
            let legs = std::iter::once((&a.source, &a.id))
                .chain(a.fallback.iter().map(|f| (&f.source, &f.id)));
            for (s, id) in legs {
                if s.is(source) && !ids.contains(id) {
                    ids.push(id.clone());
                }
            }
```

7. Replace the free `base_resolution_ms` and `fallback_after_ms` bodies:
```rust
pub fn base_resolution_ms(symbol: &str) -> u64 {
    get().base_resolution_ms(symbol)
}

/// Age past which an asset's fallback source may overwrite its price.
pub fn fallback_after_ms(symbol: &str) -> u64 {
    get().fallback_after_ms(symbol)
}
```
(Keep the doc comment only once.)

- [ ] **Step 4: Run to verify pass**

Run: `cargo test config::`
Expected: all config tests PASS (including the pre-existing `test_invalid_configs_rejected`, where `source='kraken'` is now rejected by the unknown-source check). The crate may fail to compile elsewhere until Task 2 (`feed/mod.rs` compares `Source` with `SourceRef`); if so, do Task 2 before running, then run both tasks' tests together.

- [ ] **Step 5: Commit**

```bash
git add src/config.rs
git commit -m "feat(config): add push sources and SourceRef for assets"
```

---

### Task 2: Feed — config-carrying coordinator and `ingest` for any source

**Files:**
- Modify: `src/feed/mod.rs`

**Interfaces:**
- Consumes: Task 1 (`SourceRef`, `Config::fallback_after_ms`).
- Produces: `FeedCoordinator.cfg: &'static Config`; `FeedCoordinator::with_config(db, cfg)`; `FeedCoordinator::ingest(&self, source: impl Into<SourceRef>, id: &str, price: f64, timestamp: i64)` (existing call sites passing `Source` keep compiling).

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `src/feed/mod.rs` (and `use crate::config::Config;` is covered by `use super::*` once the file imports it in Step 3):

```rust
    fn push_cfg() -> &'static Config {
        Box::leak(Box::new(
            Config::parse(
                r#"
                [sources.nav]
                type = "push"
                every = "10s"

                [[assets]]
                symbol = "FUND/NAV"
                source = "nav"
                id = "fund"
                fallback = { source = "binance", id = "FUNDUSDT" }

                [[assets]]
                symbol = "SOL/USD"
                source = "binance-ws"
                id = "SOLUSDT"
                fallback = { source = "nav", id = "sol" }
                "#,
            )
            .unwrap(),
        ))
    }

    #[tokio::test]
    async fn test_push_source_routing_staleness_and_fallback() {
        let feed = FeedCoordinator::with_config(DbManager::in_memory().unwrap(), push_cfg());
        let now = now_ms();
        let nav = || SourceRef::Push("nav".into());
        let src = |s: &str| feed.l1_cache.get(s).map(|e| e.source.clone());
        let price = |s: &str| feed.l1_cache.get(s).unwrap().price;

        // A pushed tick is the primary price for its asset, labelled with the source name
        feed.ingest(nav(), "fund", 12.5, now);
        assert_eq!(src("FUND/NAV").as_deref(), Some("nav"));

        // Binance fallback is blocked while the pushed price is fresh
        feed.ingest(Source::Binance, "FUNDUSDT", 13.0, now);
        assert_eq!(price("FUND/NAV"), 12.5);

        // Stale threshold is max(2 x 10s, 5s) = 20s: a 21s-old push lets the fallback write
        feed.ingest(nav(), "fund", 12.6, now - 21_000);
        feed.ingest(Source::Binance, "FUNDUSDT", 13.0, now);
        assert_eq!(src("FUND/NAV").as_deref(), Some("binance-fallback"));

        // A push source can be the fallback for a polled asset
        feed.ingest(nav(), "sol", 99.0, now);
        assert_eq!(src("SOL/USD").as_deref(), Some("nav-fallback"));
        feed.ingest(Source::BinanceWs, "SOLUSDT", 100.0, now);
        feed.ingest(nav(), "sol", 98.0, now);
        assert_eq!(price("SOL/USD"), 100.0);

        // Unknown ids and bad prices are ignored; push ids are never polled
        feed.ingest(nav(), "ghost", 1.0, now);
        feed.ingest(nav(), "fund", f64::NAN, now);
        assert_eq!(price("FUND/NAV"), 13.0);
        for s in Source::ALL {
            assert!(!feed.cfg.ids_for(s).iter().any(|id| id == "fund" || id == "sol"));
        }
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test feed::`
Expected: FAIL — compile errors (`with_config`, `cfg` missing; `Source`/`SourceRef` mismatches).

- [ ] **Step 3: Implement**

In `src/feed/mod.rs`:

1. Import: `use crate::config::{self, Config, Source, SourceRef};`
2. Struct + constructors:
```rust
#[derive(Clone)]
pub struct FeedCoordinator {
    pub l1_cache: Arc<DashMap<String, CachedPrice>>,
    pub broadcast_tx: broadcast::Sender<PriceTickEvent>,
    pub db: DbManager,
    pub cfg: &'static Config,
}

impl FeedCoordinator {
    pub fn new(db: DbManager) -> Self {
        Self::with_config(db, config::get())
    }

    pub fn with_config(db: DbManager, cfg: &'static Config) -> Self {
        let (broadcast_tx, _) = broadcast::channel(1024);
        Self {
            l1_cache: Arc::new(DashMap::new()),
            broadcast_tx,
            db,
            cfg,
        }
    }
```
3. `ingest`:
```rust
    pub fn ingest(&self, source: impl Into<SourceRef>, id: &str, price: f64, timestamp: i64) {
        if !(price.is_finite() && price > 0.0) {
            return;
        }
        let source = source.into();
        let now = now_ms();
        for a in &self.cfg.assets {
            if a.source == source && a.id == id {
                self.record_tick(&a.symbol, price, timestamp, source.label());
            } else if a.fallback.as_ref().is_some_and(|f| f.source == source && f.id == id)
                && self.is_stale(&a.symbol, now)
            {
                self.record_tick(&a.symbol, price, timestamp, &format!("{}-fallback", source.label()));
            }
        }
    }
```
4. `is_stale`: `self.cfg.fallback_after_ms(symbol) as i64`.
5. `ids_due`: iterate `&self.cfg.assets`; use `a.source.is(source)` and `Some(f) if f.source.is(source) && self.is_stale(..)`.
6. `start_all`: `let cfg = self.cfg;`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test`
Expected: all tests PASS (16 existing + 2 config + 1 feed = 19).

- [ ] **Step 5: Commit**

```bash
git add src/feed/mod.rs
git commit -m "feat(feed): route ticks from push sources through ingest"
```

---

### Task 3: Keys — write scope (DB column, auth, CLI)

**Files:**
- Modify: `src/db/schema.rs`, `src/db/queries.rs`, `src/auth.rs`, `src/main.rs`

**Interfaces:**
- Produces:
  - `ApiKeyRecord.write_sources: Vec<String>`; `insert_api_key(conn, id, app_name, key_hash, created_at, expires_at, write_sources: &[String])`
  - `ApiKeyMetadata.write_sources: Vec<String>`; `ApiKeyMetadata::can_write(&self, &str) -> bool`
  - `AuthManager::create_key(&self, app_name: &str, ttl_ms: Option<i64>, write_sources: &[String])`
  - CLI: `mso key create --app X --write SRC [--write SRC2] [--config FILE]`; `keys` alias for `key`.

- [ ] **Step 1: Write the failing tests**

In `src/auth.rs` `mod tests` add:
```rust
    #[test]
    fn test_write_scope() {
        let auth = AuthManager::new(DbManager::in_memory().unwrap()).unwrap();
        let (_, ro) = auth.create_key("reader", None, &[]).unwrap();
        let (_, rw) = auth.create_key("pusher", None, &["nav".to_string(), "idx".to_string()]).unwrap();

        assert!(!auth.authenticate(&ro).unwrap().can_write("nav"));
        let m = auth.authenticate(&rw).unwrap();
        assert!(m.can_write("nav") && m.can_write("idx"));
        assert!(!m.can_write("binance"));
        assert!(!m.can_write("NAV")); // names are case-sensitive

        // Scope is persisted, not just cached
        auth.reload_cache().unwrap();
        assert!(auth.authenticate(&rw).unwrap().can_write("idx"));
        assert!(!auth.authenticate(&ro).unwrap().can_write("nav"));
    }
```
In `src/db/schema.rs` add at the bottom:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrates_legacy_api_keys_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE api_keys (id TEXT PRIMARY KEY, app_name TEXT NOT NULL, key_hash TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL, expires_at INTEGER, is_revoked INTEGER NOT NULL DEFAULT 0);
             INSERT INTO api_keys VALUES ('k', 'app', 'h', 1, NULL, 0);",
        )
        .unwrap();
        init_schema(&conn).unwrap();
        init_schema(&conn).unwrap(); // idempotent
        let keys = crate::db::queries::list_all_keys(&conn).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].write_sources.is_empty()); // legacy key stays read-only
    }
}
```
Update the existing calls to the new 3-arg signature in `auth.rs` tests (`create_key("test-app", Some(60_000), &[])`, `create_key(&format!("app_{}", i), None, &[])`, `create_key("overflow", None, &[])`) and in `src/server/rest.rs` test (`create_key("integration-test-app", None, &[])`).

- [ ] **Step 2: Run to verify failure**

Run: `cargo test auth:: db::`
Expected: FAIL — compile errors (`write_sources`, `can_write`, 3-arg `create_key`).

- [ ] **Step 3: Implement**

`src/db/schema.rs` — after the `execute_batch(...)?;` call and before `Ok(())`:
```rust
    // Added after v0.1: nullable comma-separated push sources this key may write to.
    let has_scope = conn
        .prepare("SELECT 1 FROM pragma_table_info('api_keys') WHERE name = 'write_sources'")?
        .exists([])?;
    if !has_scope {
        conn.execute("ALTER TABLE api_keys ADD COLUMN write_sources TEXT", [])?;
    }
```

`src/db/queries.rs`:
- `ApiKeyRecord`: add `pub write_sources: Vec<String>,`.
- `insert_api_key`: add param `write_sources: &[String]`, and
```rust
    let scope = (!write_sources.is_empty()).then(|| write_sources.join(","));
    conn.execute(
        "INSERT INTO api_keys (id, app_name, key_hash, created_at, expires_at, is_revoked, write_sources) VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
        params![id, app_name, key_hash, created_at, expires_at, scope],
    )?;
```
- `list_all_keys`: select `..., is_revoked, write_sources`, and in the row mapper:
```rust
        let scope: Option<String> = row.get(6)?;
        Ok(ApiKeyRecord {
            ...,
            is_revoked: is_revoked_int != 0,
            write_sources: scope
                .map(|s| s.split(',').filter(|x| !x.is_empty()).map(String::from).collect())
                .unwrap_or_default(),
        })
```
(Fix any other `ApiKeyRecord { .. }` / `insert_api_key(..)` call sites the compiler reports.)

`src/auth.rs`:
- `ApiKeyMetadata`: add `pub write_sources: Vec<String>,` and
```rust
impl ApiKeyMetadata {
    pub fn can_write(&self, source: &str) -> bool {
        self.write_sources.iter().any(|s| s == source)
    }
}
```
- `reload_cache`: set `write_sources: k.write_sources`.
- `create_key(&self, app_name: &str, ttl_ms: Option<i64>, write_sources: &[String])`: pass `write_sources` to `insert_api_key` and set `write_sources: write_sources.to_vec()` in the cache entry.

`src/main.rs`:
- `Commands::Key` variant: add `#[command(alias = "keys")]` above it.
- `KeyCommands::Create`: add fields
```rust
        #[arg(long = "write", value_name = "SOURCE", help = "Allow pushing to this push source (repeatable)")]
        write: Vec<String>,

        #[arg(long, env = "MSO_CONFIG", help = "TOML config used to validate --write names")]
        config: Option<String>,
```
- Handler: destructure `write, config`, and before creating the db manager:
```rust
                if !write.is_empty() {
                    config::init(config::Config::load(config.as_deref())?);
                    for w in &write {
                        if !config::get().is_push_source(w) {
                            eprintln!("\n❌ '{}' is not a push source in the config\n", w);
                            std::process::exit(1);
                        }
                    }
                }
```
  call `auth_mgr.create_key(&app, ttl_ms, &write)`, and after the "Expires In" lines print `if !write.is_empty() { println!("Write scope: {}", write.join(", ")); }`.
- Bootstrap call: `auth_mgr.create_key("pnl-backend-bootstrap", None, &[])`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test`
Expected: all PASS (20 tests).

Manual CLI check:
```bash
cat > $TMPDIR/p.toml <<'EOF'
[sources.nav]
type = "push"
every = "60s"
[[assets]]
symbol = "FUND/NAV"
source = "nav"
id = "fund"
EOF
cargo run -q -- key create --app t --write nav --config $TMPDIR/p.toml --db $TMPDIR/k.db   # prints "Write scope: nav"
cargo run -q -- keys create --app t --write binance --config $TMPDIR/p.toml --db $TMPDIR/k.db # exits 1: not a push source
```

- [ ] **Step 5: Commit**

```bash
git add src/db/schema.rs src/db/queries.rs src/auth.rs src/main.rs src/server/rest.rs
git commit -m "feat(auth): scope API keys to push sources for writes"
```

---

### Task 4: `POST /api/v1/ticks` and OpenAPI

**Files:**
- Modify: `src/server/rest.rs`

**Interfaces:**
- Consumes: `ApiKeyMetadata::can_write`, `FeedCoordinator::{cfg, ingest}`, `Config::{is_push_source, knows, retention}`, `SourceRef`, `feed::now_ms`.

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `src/server/rest.rs`:

```rust
    fn push_app() -> (Router, FeedCoordinator, String, String) {
        let cfg: &'static crate::config::Config = Box::leak(Box::new(
            crate::config::Config::parse(
                r#"
                [sources.nav]
                type = "push"
                every = "60s"

                [sources.other]
                type = "push"
                every = "60s"

                [[assets]]
                symbol = "FUND/NAV"
                source = "nav"
                id = "fund"

                [[assets]]
                symbol = "FUND-B/NAV"
                source = "nav"
                id = "fund-b"
                "#,
            )
            .unwrap(),
        ));
        let db = DbManager::in_memory().unwrap();
        let auth = AuthManager::new(db.clone()).unwrap();
        let feed = FeedCoordinator::with_config(db, cfg);
        let (_, rw) = auth.create_key("pusher", None, &["nav".to_string()]).unwrap();
        let (_, ro) = auth.create_key("reader", None, &[]).unwrap();
        (create_rest_router(feed.clone(), auth), feed, rw, ro)
    }

    async fn post(app: &Router, key: Option<&str>, content_type: &str, body: String) -> (StatusCode, serde_json::Value) {
        let mut req = Request::builder().method("POST").uri("/api/v1/ticks").header("content-type", content_type);
        if let Some(k) = key {
            req = req.header("x-api-key", k);
        }
        let resp = app.clone().oneshot(req.body(axum::body::Body::from(body)).unwrap()).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
    }

    async fn post_json(app: &Router, key: Option<&str>, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
        post(app, key, "application/json", body.to_string()).await
    }

    #[tokio::test]
    async fn test_push_ticks_request_level_errors() {
        let (app, _feed, rw, ro) = push_app();
        let empty = |source: &str| serde_json::json!({ "source": source, "ticks": [] });

        assert_eq!(post_json(&app, None, empty("nav")).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(post_json(&app, Some("bogus"), empty("nav")).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(post_json(&app, Some(&ro), empty("nav")).await.0, StatusCode::FORBIDDEN); // read-only key
        assert_eq!(post_json(&app, Some(&rw), empty("other")).await.0, StatusCode::FORBIDDEN); // push source, no scope
        assert_eq!(post_json(&app, Some(&rw), empty("binance")).await.0, StatusCode::BAD_REQUEST); // built-in
        assert_eq!(post_json(&app, Some(&rw), empty("ghost")).await.0, StatusCode::BAD_REQUEST); // undeclared
        assert_eq!(post_json(&app, Some(&rw), empty("NAV")).await.0, StatusCode::BAD_REQUEST); // case-sensitive

        // Empty batch is fine
        let (st, body) = post_json(&app, Some(&rw), empty("nav")).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "accepted": 0, "rejected": [] }));

        // 1001 ticks -> 413; body over 256 KB -> 413
        let many: Vec<_> = (0..1001).map(|_| serde_json::json!({ "id": "fund", "price": 1.0 })).collect();
        let (st, _) = post_json(&app, Some(&rw), serde_json::json!({ "source": "nav", "ticks": many })).await;
        assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE);
        let big = serde_json::json!({ "source": "nav", "ticks": [], "pad": "x".repeat(300_000) });
        assert_eq!(post_json(&app, Some(&rw), big).await.0, StatusCode::PAYLOAD_TOO_LARGE);

        // Garbage and wrong content type are client errors, not panics
        assert!(post(&app, Some(&rw), "application/json", "not json".into()).await.0.is_client_error());
        assert!(post(&app, Some(&rw), "application/json", r#"{"source":"nav"}"#.into()).await.0.is_client_error());
        assert!(post(&app, Some(&rw), "text/plain", r#"{"source":"nav","ticks":[]}"#.into()).await.0.is_client_error());
    }

    #[tokio::test]
    async fn test_push_ticks_mixed_batch_applies_valid_and_reports_invalid() {
        let (app, feed, rw, _ro) = push_app();
        let now = crate::feed::now_ms();
        let (st, body) = post_json(
            &app,
            Some(&rw),
            serde_json::json!({ "source": "nav", "ticks": [
                { "id": "fund", "price": 12.5, "time": now },
                { "id": "fund-b", "price": 98.1 },
                { "id": "ghost", "price": 1.0 },
                { "id": "fund", "price": 0.0 },
                { "id": "fund", "price": -1.0 },
                { "id": "fund", "price": 1.0, "time": now + 6 * 60_000 },
                { "id": "fund", "price": 1.0, "time": now - 25 * 3_600_000 },
            ]}),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(body["accepted"], 2);
        let errs: Vec<(&str, &str)> = body["rejected"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| (r["id"].as_str().unwrap(), r["error"].as_str().unwrap()))
            .collect();
        assert_eq!(
            errs,
            vec![
                ("ghost", "unknown id for source nav"),
                ("fund", "invalid price"),
                ("fund", "invalid price"),
                ("fund", "time too far in the future"),
                ("fund", "time older than retention"),
            ]
        );

        // Valid ticks went through ingest(): cache, source label and SQLite
        let cached = feed.l1_cache.get("FUND/NAV").unwrap().clone();
        assert_eq!((cached.price, cached.source.as_str(), cached.timestamp), (12.5, "nav", now));
        assert_eq!(feed.l1_cache.get("FUND-B/NAV").unwrap().price, 98.1);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let stored = feed
            .db
            .with_conn(|c| crate::db::queries::get_recent_ticks(c, "FUND/NAV", 10))
            .unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!((stored[0].price, stored[0].t), (12.5, now));
    }

    #[tokio::test]
    async fn test_push_ticks_ordering_and_duplicates() {
        let (app, feed, rw, _ro) = push_app();
        let now = crate::feed::now_ms();

        // Newest first in the payload: the cache must still end on the newest tick
        let (st, body) = post_json(
            &app,
            Some(&rw),
            serde_json::json!({ "source": "nav", "ticks": [
                { "id": "fund", "price": 3.0, "time": now },
                { "id": "fund", "price": 2.0, "time": now - 1_000 },
                { "id": "fund", "price": 1.0, "time": now - 2_000 },
            ]}),
        )
        .await;
        assert_eq!((st, body["accepted"].as_i64()), (StatusCode::OK, Some(3)));
        assert_eq!(feed.l1_cache.get("FUND/NAV").unwrap().price, 3.0);

        // Same id + same time twice: both accepted, the later one wins
        let (st, body) = post_json(
            &app,
            Some(&rw),
            serde_json::json!({ "source": "nav", "ticks": [
                { "id": "fund-b", "price": 1.0, "time": now },
                { "id": "fund-b", "price": 2.0, "time": now },
            ]}),
        )
        .await;
        assert_eq!((st, body["accepted"].as_i64()), (StatusCode::OK, Some(2)));
        assert_eq!(feed.l1_cache.get("FUND-B/NAV").unwrap().price, 2.0);
    }

    #[tokio::test]
    async fn test_openapi_documents_push() {
        let (app, _feed, _rw, _ro) = push_app();
        let resp = app
            .oneshot(Request::builder().uri("/openapi.json").body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let spec: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let post = &spec["paths"]["/api/v1/ticks"]["post"];
        assert!(post["requestBody"].is_object());
        assert!(post["responses"]["403"].is_object() && post["responses"]["413"].is_object());
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test server::rest::`
Expected: FAIL — POST returns 405 / assertions fail (no handler yet).

- [ ] **Step 3: Implement**

In `src/server/rest.rs`:

1. Imports: `extract::{DefaultBodyLimit, Query, State}`; `use crate::auth::{ApiKeyMetadata, AuthManager};`; `use crate::config::{clamp_effective_ttl, SourceRef};`; `use crate::feed::{now_ms, FeedCoordinator};`.

2. Constants and request types (below `StatsQuery`):
```rust
const MAX_PUSH_BODY: usize = 256 * 1024;
const MAX_PUSH_TICKS: usize = 1000;
const MAX_FUTURE_MS: i64 = 5 * 60_000;

#[derive(Deserialize)]
pub struct PushBody {
    pub source: String,
    pub ticks: Vec<PushTick>,
}

#[derive(Deserialize)]
pub struct PushTick {
    pub id: String,
    pub price: f64,
    pub time: Option<i64>,
}
```

3. `authenticate_request`: return type `Result<ApiKeyMetadata, (StatusCode, Json<serde_json::Value>)>` and `Ok(m) => Ok(m)` (GET handlers only use `if let Err(..)`, so they are unchanged).

4. Route: `.route("/api/v1/ticks", get(get_ticks_handler).post(post_ticks_handler).layer(DefaultBodyLimit::max(MAX_PUSH_BODY)))`.

5. Handler:
```rust
async fn post_ticks_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<PushBody>,
) -> impl IntoResponse {
    // Writes never accept ?key=, so keys stay out of URLs and logs.
    let key = match authenticate_request(&headers, None, &state.auth) {
        Ok(k) => k,
        Err(e) => return e.into_response(),
    };
    let err = |status: StatusCode, msg: &str| (status, Json(serde_json::json!({ "error": msg }))).into_response();

    if body.ticks.len() > MAX_PUSH_TICKS {
        return err(StatusCode::PAYLOAD_TOO_LARGE, "too many ticks (max 1000 per request)");
    }
    let cfg = state.feed.cfg;
    if !cfg.is_push_source(&body.source) {
        return err(StatusCode::BAD_REQUEST, &format!("'{}' is not a push source", body.source));
    }
    if !key.can_write(&body.source) {
        return err(StatusCode::FORBIDDEN, &format!("API key has no write scope for source '{}'", body.source));
    }

    let now = now_ms();
    let (min_t, max_t) = (now - cfg.retention.ticks_ms(), now + MAX_FUTURE_MS);
    let src = SourceRef::Push(body.source.clone());
    let mut valid = Vec::new();
    let mut rejected = Vec::new();
    for t in body.ticks {
        let time = t.time.unwrap_or(now);
        let error = if !cfg.knows(&src, &t.id) {
            Some(format!("unknown id for source {}", body.source))
        } else if !(t.price.is_finite() && t.price > 0.0) {
            Some("invalid price".to_string())
        } else if time > max_t {
            Some("time too far in the future".to_string())
        } else if time < min_t {
            Some("time older than retention".to_string())
        } else {
            None
        };
        match error {
            Some(error) => rejected.push(serde_json::json!({ "id": t.id, "error": error })),
            None => valid.push((time, t.id, t.price)),
        }
    }

    // Oldest first, so the cache ends on the newest tick even if the payload is unordered.
    // ponytail: ordering is per request; across requests the last tick applied wins, so
    // pushers should send in time order. Add a newest-wins guard in record_tick if backfill matters.
    valid.sort_by_key(|v| v.0);
    let accepted = valid.len();
    for (time, id, price) in valid {
        state.feed.ingest(src.clone(), &id, price, time);
    }
    Json(serde_json::json!({ "accepted": accepted, "rejected": rejected })).into_response()
}
```

6. OpenAPI: in `openapi_handler`, replace the unique anchor
```
                }
            },
            "/api/v1/candles": {
```
with
```
                },
                "post": {
                    "tags": ["Market Data"],
                    "summary": "Push prices to a push source",
                    "description": "Writes a batch of prices for assets whose source is a `type = push` entry under `[sources.*]` in the config. Requires an API key created with `mso key create --write <source>`; read-only keys get 403. Valid ticks are applied and invalid ones are listed in `rejected`. Max 1000 ticks and 256 KB per request. `time` is Unix ms (default: receive time), at most 5 minutes in the future and within tick retention. Send ticks in time order across requests: the cache reflects the last tick applied.",
                    "security": [
                        { "ApiKeyHeader": [] },
                        { "BearerAuth": [] }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["source", "ticks"],
                                    "properties": {
                                        "source": { "type": "string", "example": "my-service" },
                                        "ticks": {
                                            "type": "array",
                                            "maxItems": 1000,
                                            "items": {
                                                "type": "object",
                                                "required": ["id", "price"],
                                                "properties": {
                                                    "id": { "type": "string", "example": "fund-a" },
                                                    "price": { "type": "number", "exclusiveMinimum": 0, "example": 12.3456 },
                                                    "time": { "type": "integer", "format": "int64", "example": 1791035346214i64 }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Result per batch",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "accepted": { "type": "integer", "example": 1 },
                                            "rejected": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id": { "type": "string" },
                                                        "error": { "type": "string", "example": "unknown id for source my-service" }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        "400": { "description": "source is not a push source" },
                        "401": { "description": "Unauthorized" },
                        "403": { "description": "API key has no write scope for source" },
                        "413": { "description": "More than 1000 ticks or body over 256 KB" }
                    }
                }
            },
            "/api/v1/candles": {
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test`
Expected: all PASS (24 tests), no new warnings from `cargo build`.

- [ ] **Step 5: Commit**

```bash
git add src/server/rest.rs
git commit -m "feat(api): accept pushed ticks over POST /api/v1/ticks"
```

---

### Task 5: Docs and spec alignment

**Files:**
- Modify: `README.md`, `docs/superpowers/specs/2026-10-03-push-feeds-design.md`

- [ ] **Step 1: README**

1. In **Configuration**, after the "Unknown keys, duplicate symbols..." sentence add:

````markdown
### Push sources

Prices computed by another service (a fund NAV, an index, a private system) can be pushed
over HTTP instead of polled. Declare a `push` source and point assets at it:

```toml
[sources.my-service]
type  = "push"
every = "60s"          # expected cadence; fallback_after = max(2 x every, 5s)

[[assets]]
symbol = "FUND-A/NAV"
source = "my-service"
id     = "fund-a"
```

A pushed asset gets the same cache, stream, SQLite storage, candles and `/price`, `/ticks`,
`/candles`, `/stats` as any other. It can have a `fallback`, and a push source can be the
`fallback` of a polled asset.
````

2. In **API Key Management** add under the create examples:

````markdown
# Key that may push to a push source (repeat --write for several; read-only without it):
./target/release/mso key create --app my-service --write my-service --config mso.toml
````
   plus one sentence after the block: "`--write` names must be `type = "push"` sources in the config; a key can never write to a built-in source. Existing keys stay read-only."

3. In **API Reference** add the table row
`| /api/v1/ticks | POST | JSON body | Push prices to a push source (needs a write-scoped key) |` and below the table:

````markdown
### Pushing prices

```bash
curl -X POST http://localhost:4000/api/v1/ticks \
  -H "x-api-key: $WRITE_KEY" -H "content-type: application/json" \
  -d '{"source":"my-service","ticks":[{"id":"fund-a","price":12.3456,"time":1791035346214},{"id":"fund-b","price":98.10}]}'
# {"accepted":1,"rejected":[{"id":"fund-b","error":"unknown id for source my-service"}]}
```

`time` (Unix ms) is optional (default: receive time). Valid ticks are applied even when others in the
batch are rejected. Errors: `400` source is not a push source, `403` key has no write scope for it,
`413` over 1000 ticks or 256 KB. Per tick: unknown `id`, price not finite or <= 0, `time` more than
5 minutes ahead or older than tick retention. Send ticks in time order; the cache reflects the last
tick applied. Writes take the key from a header only (not `?key=`).
````

- [ ] **Step 2: Spec alignment**

In `docs/superpowers/specs/2026-10-03-push-feeds-design.md` make these edits:
- Section 2: "`ingest()` takes a `&SourceRef`" → "`ingest()` takes `impl Into<SourceRef>` (poller call sites unchanged); `FeedCoordinator` holds `cfg: &'static Config` so tests can inject a push config."
- Section 4 check order sentence → "The 256 KB body limit is enforced by a body-limit layer before the handler (so `413` can precede `401`); then `401` bad/missing key → `413` more than 1000 ticks → `400` → `403`." Also: "Writes accept the key from headers only." and "Valid ticks are applied oldest-first."
- Section 3: "`mso key create` (alias `keys`) takes repeatable `--write` and `--config`."
- Remove the "Open checks" rate-limiter bullet and add: "No per-key rate limiter exists in the codebase, so none is added for writes."

- [ ] **Step 3: Verify**

Run: `cargo test && cargo build --release 2>&1 | tail -3`
Expected: all tests PASS; release build succeeds.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/superpowers/specs/2026-10-03-push-feeds-design.md
git commit -m "docs(feed): document push sources and write-scoped keys"
```
