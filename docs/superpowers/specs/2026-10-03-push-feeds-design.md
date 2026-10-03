# Push price feeds over HTTP (issue #1)

External processes push prices to `POST /api/v1/ticks`. A pushed tick goes through
`FeedCoordinator::ingest()`, so it gets the cache, broadcast, SQLite persistence,
candles, staleness and fallback like any built-in feed.

## Scope decisions

- Config uses `[sources.<name>]`, but only `type = "push"` is accepted for now. Built-in
  sources and `[poll_secs]` are unchanged. Issue #2 can widen the same table later.
- Out of scope: inbound WebSocket/gRPC, derived or composite symbols.

## 1. Config (`src/config.rs`)

```toml
[sources.my-service]
type  = "push"
every = "60s"

[[assets]]
symbol = "FUND-A/NAV"
source = "my-service"
id     = "fund-a"
```

- New optional `sources` table: `name -> { type = "push", every }`, `deny_unknown_fields`.
  `every` is a duration string (`"60s"`); minimum 1s.
- `Asset.source` and `Fallback.source` become `SourceRef { Builtin(Source), Push(String) }`.
  Deserialization tries built-in labels first, then treats the string as a push name.
- `validate()` rejects: undeclared push names, push names equal to a built-in label, `every` < 1s.
- `base_resolution_ms` returns `every` for push assets, so
  `fallback_after_ms = max(2 x every, 5s)` applies unchanged.
- `ids_for` / `poll_interval` still handle built-ins only.

## 2. Ingestion (`src/feed/mod.rs`)

- `ingest()` takes `&SourceRef`; pollers pass `SourceRef::Builtin(..)`.
- Stored tick `source` = push source name (`<name>-fallback` when acting as a fallback).
- `ids_due` / `start_all` consider built-ins only: push sources never spawn a poller.
- A push source can be a fallback for a polled asset (writes only when pushed and the
  primary is stale), and a pushed asset can have a fallback.

## 3. Keys (`src/auth.rs`, `src/db/`, `src/main.rs`)

- `api_keys` gains a nullable `write_sources` column (comma-separated names), added with a
  guarded `ALTER TABLE` (check `pragma table_info`) for existing databases. NULL = read-only,
  so existing keys are unchanged.
- `ApiKeyMetadata.write_sources: Vec<String>`; `create_key(app, ttl, write_sources)`.
- `mso keys create --write <source>` is repeatable and validates each name against the
  configured push sources, so a key can never be scoped to a built-in source.

## 4. Endpoint (`src/server/rest.rs`)

`POST /api/v1/ticks`, body `{ "source": str, "ticks": [{ "id", "price", "time"? }] }`,
`x-api-key` header like the GETs. 256 KB body limit.

Whole-request checks, in order: `401` bad/missing key -> `413` body > 256 KB or
> 1000 ticks -> `400` `source` is not a push source -> `403` key lacks write scope for it.

Per-tick rejections (rest of the batch still applies):

| Condition | Error text |
|---|---|
| `id` not declared for the source in `[[assets]]` | `unknown id for source <name>` |
| price not finite or <= 0 | `invalid price` |
| `time` > now + 5 min | `time too far in the future` |
| `time` older than tick retention | `time older than retention` |

Missing `time` = server receive time. Response `200`:
`{ "accepted": n, "rejected": [{ "id", "error" }] }`.
One `id` can feed several assets (fallback legs); it counts as accepted if it matches any asset leg.

`/openapi.json` and `README.md` document the endpoint, the `push` source type and the key scope.

## 5. Tests

- config: push source parses, asset can reference it, undeclared/colliding/zero-`every` rejected.
- feed: push routing + staleness + fallback, in the style of `test_ingest_routes_primary_and_fallback`.
- auth/db: scoped key, read-only default, migration of a pre-existing `api_keys` table.
- rest: one test per validation row (401/400/403/413 and per-tick), a mixed batch, and a pushed
  tick visible in `/price` and `/ticks`.

## Open checks during implementation

- Confirm there is no per-key rate limiter to extend; if one exists, apply it to POST.
- Same `(symbol, t)` behaviour follows the existing `insert_tick`.
