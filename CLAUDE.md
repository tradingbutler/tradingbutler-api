# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

The Rust backend for TradingButler. MT5 terminals connect over a **WebSocket** and push
broker info + live ticks into the `collector`; the collector writes them into per-broker Redis
**Streams**; downstream services consume those streams independently. See the root-level
`CLAUDE.md` for full system context.

## Architecture

```
MT5 terminals (rhiaqey metatrader DLL)
  └─ WebSocket  →  collector  (/ws, binary GatewayMessage frames)
                      │
                      ├─ category "broker" → HSET {ns}:brokers:{id}
                      └─ category "live"   → XADD {ns}:brokers:{id}:live  (maxlen ~1000)
                                           + HSET {ns}:brokers:{id}:snapshot  field=key
                                              │
                                       Redis Streams (per broker)
                                              ├─ json-writer    → writes rates.json + brokers.json (SSR)
                                              ├─ rate-streamer   → (planned) WS fan-out to landing page
                                              └─ admin-api       → (planned) HTTP API for admin dashboard
```

Transport is **Redis Streams** (`XADD` / `XREAD`, consumer groups), not pub/sub channels. Each
consumer service tracks its own position; consumer groups give load-balanced, crash-safe delivery
with explicit `XACK`.

## Workspace crates

| Crate | Path | Type | Role |
|---|---|---|---|
| `common` | `crates/common` | lib | `RedisService` (streams + hashes), `env::Env`, `VERSION` |
| `collector` | `crates/collector` | bin | WebSocket server; ingests broker/live messages from MT5 into Redis |
| `json-writer` | `crates/json-writer` | bin | Consumes live streams, writes `rates.json` + `brokers.json` snapshots |
| `rate-streamer` | `crates/rate-streamer` | bin | **Stub** — `start()` is a no-op. Planned WS fan-out to the landing page |
| `admin-api` | `crates/admin-api` | bin | axum HTTP API backing the `admin/` dashboard. `GET /health`, broker CRUD (see below) |
| `rhiaqey-sdk-rs` | `sdk` | lib | Vendored fork of the rhiaqey SDK — `GatewayMessage`, `MessageValue`, gateway/channel/producer/settings traits |
| `rhiaqey-metatrader` | `metatrader` | cdylib + rlib | The **MT5-side gateway DLL** (`gw_*` FFI). Replaces the old `.mq5` EA |

There is no longer a `core` or `server` crate — those names are gone. The roster of brokers/symbols
is no longer hardcoded in Rust; brokers register themselves at runtime over the WebSocket.

When adding a new crate: add it to `members` in the root `Cargo.toml`, declare shared deps in
`[workspace.dependencies]`, and reference them with `{ workspace = true }`.

## Message protocol (MT5 → collector)

The MT5 terminal connects to `collector`'s `/ws` and sends binary frames. A frame is either the
literal bytes `ping` (ignored) or a JSON-serialized `rhiaqey_sdk_rs::gateway::GatewayMessage`.
Field names on the wire are short (serde renames): `key`, `val`, `tms`, `tag`, `cat`, `siz`,
`uid`, `cid`. The collector dispatches on `cat` (category):

- **`broker`** — `val` is a `Broker { id, nm→name, ak→api_key }`. The collector stores it at
  `{ns}:brokers:{id}` (hash: `id`, `name`, `api_key`) and remembers it for the connection. It also
  reads that broker's `symbol_map` hash field (JSON `{alias: canonical}`, e.g.
  `{"BITCOIN":"BTCUSD"}`) and attaches it to the in-memory `Broker` (`symbol_map` is
  `#[serde(skip)]` — never on the wire) for use by `live` messages on this connection.
  The DLL **SHA-512–hashes** the API key before sending; `api_key` on the wire is the hex digest.
- **`live`** — must arrive after a `broker` message on the same connection. `val` is an `MT5Event`
  (symbol/timeframe/info/tick/diffs). Before storing, the collector normalizes the message `key`
  (broker's own symbol string, e.g. `BITCOIN`) through `Broker::canonical_symbol` — if this broker
  has a `symbol_map` entry for it, the canonical code (e.g. `BTCUSD`) is stored instead; if the
  broker has a non-empty `symbol_map` but no entry for this particular key, it's stored as-is and a
  warning is logged; if the broker has no `symbol_map` at all, it's stored as-is with no warning
  (normalization is opt-in per broker). The collector, in one pipeline, `XADD`s to
  `{ns}:brokers:{id}:live` (trimmed to ~1000 entries) and `HSET`s the latest value into
  `{ns}:brokers:{id}:snapshot` keyed by the (possibly normalized) symbol.
- Other categories (e.g. `historical:*`) are logged and ignored by the collector today.

## Redis key layout

`{ns}` is the `REDIS_NAMESPACE` env var (see below); `RedisService` prepends it automatically, so
every crate builds/consumes **bare** keys (`brokers:{id}`, `brokers:{id}:live`, `brokers:{id}:snapshot`) and never
hardcodes the namespace itself.

| Key                          | Type | Written by | Contents |
|------------------------------|---|---|---|
| `{ns}:brokers:{id}`          | hash | collector, admin-api | `id`, `name`, `api_key` (sha512 hex), `allowed_ips`, `open_account_url`, `logo`, `symbol_map` (JSON `{alias: canonical}`) |
| `{ns}:brokers:{id}:live`     | stream | collector | fields `conn_id`, `key`, `data` (JSON tick); maxlen ~1000 |
| `{ns}:brokers:{id}:snapshot` | hash | collector | field = symbol/key → latest tick JSON |

The old `latest:{symbol}` / `baseline:{symbol}` keys and the `prices` pub/sub channel no longer exist.

## json-writer behavior

- A discovery loop scans `brokers:*` every **30s**. For each broker it (a) refreshes
  `brokers.json` (all broker hashes, **with `api_key` stripped**) and (b) the first time it sees a
  broker, ensures a `json-writer` consumer group on `{id}:live` (`NewOnly`) and spawns
  a reader task.
- Each reader uses `group_reader` (consumer `json-writer-{id}`); on every entry it re-reads the
  whole `brokers:{id}:snapshot` hash, forwards it to the writer task, and `XACK`s.
- The writer task accumulates `broker_id → symbol → value` and writes `rates.json` **atomically**
  (write to `*.tmp`, then rename). `brokers.json` is written the same way.
- Output paths come from `JSON_SNAPSHOT_FILE` (`rates.json`) and `BROKERS_SNAPSHOT_FILE`
  (`brokers.json`).

## admin-api endpoints

The `admin/` Angular dashboard (top-level project, see its README) provisions brokers through these.
All errors are JSON `{ "error": "…" }`.

- `GET /api/brokers` — list as `[{ id, name, has_key, allowed_ips }]`. Never returns the key/hash;
  `has_key` is false when the key was revoked or never set.
- `POST /api/brokers` `{ id, name, allowed_ips? }` — create a broker. Generates a random plaintext
  key, stores `{ns}:brokers:{id}` with `api_key` = its **SHA-512 hex digest** (same hashing
  the MT5 DLL applies, so a terminal authenticating with the plaintext key matches), and returns
  `{ id, name, api_key }` **once** — the plaintext is never persisted. `409` if the id exists, `400`
  on empty fields, an id containing whitespace/`:`, or an invalid IP/CIDR.
- `POST /api/brokers/{id}/key` — regenerate: issue a fresh key (invalidating the old one), returns
  `{ id, name, api_key }` once. `404` if unknown.
- `DELETE /api/brokers/{id}/key` — revoke: clear `api_key` (empty hash matches nothing) without
  deleting the broker. `204`, `404` if unknown.
- `PUT /api/brokers/{id}/allowed-ips` `{ allowed_ips }` — replace the IP whitelist (empty = no
  restriction). Entries are validated (IP or CIDR), trimmed and de-duplicated. Returns the updated
  broker. `404` if unknown.
- `PUT /api/brokers/{id}/symbol-map` `{ symbol_map }` (object, alias → canonical code, e.g.
  `{"BITCOIN":"BTCUSD"}`) — replace this broker's symbol normalization table. Empty map disables
  normalization for this broker (ticks stored under their raw broker-reported symbol). Keys/values
  are trimmed; `400` on an empty alias or canonical value. Returns the updated broker. `404` if
  unknown. Consumed by the collector when handling this broker's `live` messages (see above).
- `DELETE /api/brokers/{id}` — delete the broker and its `:live` stream + `:snapshot` hash. `204`,
  `404` if unknown.

This makes admin the source of truth for the broker roster, replacing the dev practice of MT5
terminals self-registering arbitrary brokers. **Note:** `allowed_ips` is stored but **not yet
enforced** — wiring it into the `collector` (reject broker/live messages whose client IP is outside
the whitelist) is a follow-up.

## RedisService (`crates/common/src/service.rs`)

`RedisService` is `Clone` (shared `ConnectionManager` that multiplexes + auto-reconnects). Blocking
reads (`XREAD BLOCK`) use a **dedicated** connection with the response timeout disabled, because
redis 1.3's 500ms default would abort `BLOCK` early.

Every key-bearing method takes and returns **bare** keys (no namespace) — `RedisService` prepends
`REDIS_NAMESPACE` (see below) internally via its private `key()` helper, and `keys()` strips it
back off returned matches. Callers never build namespaced strings themselves, with one exception:
`pipeline()` hands the closure a raw `redis::Pipeline`, which bypasses `RedisService` entirely, so
code using it must namespace its own keys with the public `RedisService::key()` before passing them
to `pipe.xadd_maxlen` / `pipe.hset` / `pipe.del` (see `collector`'s live-message handler and
`admin-api`'s `delete_broker` for examples).

```rust
let mut svc = RedisService::new(&env.redis_url, &env.redis_namespace).await?;

// producer — key is bare; svc namespaces it to `{REDIS_NAMESPACE}:b1:live`
svc.xadd("b1:live", &[("key","EURUSD"),("data","{…}")], Some(1000)).await?;

// fan-out reader (every consumer gets every entry); StreamPosition::{Beginning,NewOnly,After}
let mut r = svc.stream_reader("b1:live", StreamPosition::NewOnly);

// load-balanced reader (each entry to one consumer); call ack() after processing
svc.ensure_consumer_group("…:live", "json-writer", StreamPosition::NewOnly).await?;
let mut g = svc.group_reader("…:live", "json-writer", "json-writer-b1");
```

Also exposes `pipeline`, `set`, `hset`, `del`, `hgetall`, `keys`, `key`, and raw `client()`.

## Environment (`crates/common/src/env.rs`, via `envconfig`)

| Variable | Default | Purpose                                                                                        |
|---|---|------------------------------------------------------------------------------------------------|
| `REDIS_URL` | `redis://127.0.0.1` | Valkey/Redis connection string                                                                 |
| `REDIS_NAMESPACE` | `tradingbuttler` | Prefix `RedisService` prepends to every key (`{ns}:brokers:{id}`, `{ns}:brokers:{id}:live`, …) |
| `HTTP_HOST` | `0.0.0.0` | Bind host for collector / admin-api                                                            |
| `HTTP_PORT` | `20000` | Bind port for collector / admin-api                                                            |
| `IP_SOURCE` | `ConnectInfo` | `axum-client-ip` source for resolving client IP                                                |
| `JSON_SNAPSHOT_FILE` | `rates.json` | json-writer rates output path                                                                  |
| `BROKERS_SNAPSHOT_FILE` | `brokers.json` | json-writer brokers output path                                                                |

There are no `VALKEY_URL`, `PORT`, or `BROKER{1-5}_API_KEY` variables anymore — broker auth is the
self-registered, sha512-hashed `api_key` carried in the `broker` message.

## The MT5 gateway (`metatrader/`)

`rhiaqey-metatrader` is a **`cdylib`** built into the DLL that the MQL5 side loads. The MQL5 sources
live under `metatrader/metatrader/` (`rhiaqey.mq5`, `rhiaqey.mqh`, `rhiaqey_hash.mqh`). The DLL
exposes `extern "C"` `gw_*` functions: `gw_init`, `gw_connect` (comma-separated endpoints, opens a
WS to `…/ws`), `gw_send_broker`, `gw_send_tick`, `gw_send_historical`, `gw_send_ping`,
`gw_disconnect`, `gw_get_last_error`, `gw_total_connections`. Build with the `mt5` feature for MT5
tick fields. This is the replacement for the former hand-written `ea/TradingButlerFeed.mq5`.

## Commands (Makefile from `api/`)

```bash
make dev            # cargo build --all-features
make prod           # cargo build --release --all-features
make collector      # cargo run -p collector
make json-writer    # cargo run -p json-writer
make test           # cargo test --all --all-features
make lint           # cargo fmt --all + clippy -D warnings -D dead_code
make format         # cargo fmt --all

# direct cargo
cargo build -p common
cargo test -p common <name>
```

Each binary's `main.rs` installs the rustls ring crypto provider, inits `env_logger`, loads `Env`,
then `init().await` + `start().await`.

## Stale deployment files — fix before shipping

- `api/Dockerfile` still builds and runs a `server` binary (`cargo build --release -p server`) that
  no longer exists. It needs to build the real binaries (`collector`, `json-writer`, …) and copy
  `crates/`, `sdk/`, `metatrader/` into the build context.
- root `docker-compose.yml` still defines a single `api` service with `VALKEY_URL` / `PORT` /
  `BROKER*_API_KEY`. The backend is now multiple binaries on Redis Streams with the env vars above.
