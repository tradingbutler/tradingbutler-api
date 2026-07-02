use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use common::{RedisService, env::Env};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::net::{IpAddr, SocketAddr};
use uuid::Uuid;

/// Brokers are stored as a hash at `tradingbutler:broker:{id}` with fields
/// `id`, `name`, `api_key`, `allowed_ips`.
///
/// - `api_key` is the **SHA-512 hex digest** of the plaintext key — the same
///   hashing the MT5 gateway DLL applies before it sends a `broker` message — so
///   a terminal authenticating with the plaintext key produces a matching
///   digest. An empty `api_key` means the key has been revoked.
/// - `allowed_ips` is a comma-separated list of IPs/CIDRs permitted to connect
///   for this broker. Empty means no restriction. Enforcement lives in the
///   `collector`; admin-api is the source of truth. See `api/CLAUDE.md`.
const BROKER_KEY_PREFIX: &str = "tradingbutler:broker:";

fn broker_key(id: &str) -> String {
    format!("{BROKER_KEY_PREFIX}{id}")
}

#[derive(Clone)]
pub struct AdminApi {
    redis: RedisService,
    bind: SocketAddr,
}

impl AdminApi {
    pub async fn init(env: &Env) -> anyhow::Result<Self> {
        let redis = RedisService::new(&env.redis_url).await?;
        let bind: SocketAddr = format!("{}:{}", env.http_host, env.http_port).parse()?;
        Ok(Self { redis, bind })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let router = Router::new()
            .merge(common::health::router())
            .route("/api/brokers", get(list_brokers).post(create_broker))
            .route("/api/brokers/{id}", delete(delete_broker))
            .route("/api/brokers/{id}/key", post(regenerate_key))
            .route("/api/brokers/{id}/logo", put(update_logo))
            .route(
                "/api/brokers/{id}/open-account-url",
                put(update_open_account_url),
            )
            .route("/api/brokers/{id}/allowed-ips", put(update_allowed_ips))
            .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
            .with_state(self.redis);

        let listener = tokio::net::TcpListener::bind(self.bind).await?;
        log::info!("listening on {}", self.bind);
        axum::serve(listener, router).await?;
        Ok(())
    }
}

/// Public view of a broker — never includes the API key (hash or plaintext).
#[derive(Serialize)]
struct Broker {
    id: String,
    name: String,
    has_key: bool,
    allowed_ips: Vec<String>,
    open_account_url: String,
    /// Full data URL (`data:image/...;base64,...`) or empty when no logo is set.
    logo: Option<String>,
}

async fn list_brokers(State(redis): State<RedisService>) -> Result<Json<Vec<Broker>>, ApiError> {
    let mut redis = redis;
    let keys = redis.keys(&format!("{BROKER_KEY_PREFIX}*")).await?;

    let mut brokers = Vec::with_capacity(keys.len());
    for key in keys {
        let fields = redis.hgetall(&key).await?;
        let id = fields
            .get("id")
            .cloned()
            .or_else(|| key.strip_prefix(BROKER_KEY_PREFIX).map(str::to_owned))
            .unwrap_or_default();
        let name = fields.get("name").cloned().unwrap_or_default();
        let has_key = fields.get("api_key").is_some_and(|k| !k.is_empty());
        let allowed_ips = parse_ip_list(fields.get("allowed_ips").map(String::as_str));
        let open_account_url = fields.get("open_account_url").cloned().unwrap_or_default();
        let logo = fields.get("logo").filter(|v| !v.is_empty()).cloned();
        brokers.push(Broker {
            id,
            name,
            has_key,
            allowed_ips,
            open_account_url,
            logo,
        });
    }
    brokers.sort_by(|a, b| a.id.cmp(&b.id));

    Ok(Json(brokers))
}

#[derive(Deserialize)]
struct CreateBroker {
    id: String,
    name: String,
    #[serde(default)]
    allowed_ips: Vec<String>,
    #[serde(default)]
    open_account_url: String,
    #[serde(default)]
    logo: String,
}

/// Returned **once** when a key is issued (create or regenerate) — `api_key` is
/// the plaintext key, which is never stored and cannot be retrieved again.
#[derive(Serialize)]
struct IssuedKey {
    id: String,
    name: String,
    api_key: String,
}

async fn create_broker(
    State(redis): State<RedisService>,
    Json(body): Json<CreateBroker>,
) -> Result<(StatusCode, Json<IssuedKey>), ApiError> {
    let mut redis = redis;

    let id = body.id.trim().to_owned();
    let name = body.name.trim().to_owned();

    if id.is_empty() || name.is_empty() {
        return Err(ApiError::BadRequest("id and name are required".into()));
    }
    // The id is part of the Redis key and stream names — keep it clean.
    if id.contains(|c: char| c.is_whitespace() || c == ':') {
        return Err(ApiError::BadRequest(
            "id must not contain spaces or ':'".into(),
        ));
    }
    let allowed_ips = normalize_ip_list(&body.allowed_ips)?;

    let key = broker_key(&id);
    if !redis.hgetall(&key).await?.is_empty() {
        return Err(ApiError::Conflict(format!("broker '{id}' already exists")));
    }

    let api_key = new_api_key();
    redis
        .hset(
            &key,
            &[
                ("id", id.as_str()),
                ("name", name.as_str()),
                ("api_key", sha512_hex(&api_key).as_str()),
                ("allowed_ips", allowed_ips.as_str()),
                ("open_account_url", body.open_account_url.trim()),
                ("logo", body.logo.trim()),
            ],
        )
        .await?;

    log::info!("created broker {id}");

    Ok((StatusCode::CREATED, Json(IssuedKey { id, name, api_key })))
}

/// Issue a fresh key for an existing broker, invalidating the previous one.
async fn regenerate_key(
    State(redis): State<RedisService>,
    Path(id): Path<String>,
) -> Result<Json<IssuedKey>, ApiError> {
    let mut redis = redis;
    let key = broker_key(&id);

    let fields = redis.hgetall(&key).await?;
    if fields.is_empty() {
        return Err(ApiError::NotFound(format!("broker '{id}' not found")));
    }
    let name = fields.get("name").cloned().unwrap_or_default();

    let api_key = new_api_key();
    redis
        .hset(&key, &[("api_key", sha512_hex(&api_key).as_str())])
        .await?;

    log::info!("regenerated key for broker {id}");

    Ok(Json(IssuedKey { id, name, api_key }))
}

#[derive(Deserialize)]
struct UpdateLogo {
    logo: String,
}

async fn update_logo(
    State(redis): State<RedisService>,
    Path(id): Path<String>,
    Json(body): Json<UpdateLogo>,
) -> Result<StatusCode, ApiError> {
    let mut redis = redis;
    let key = broker_key(&id);

    if redis.hgetall(&key).await?.is_empty() {
        return Err(ApiError::NotFound(format!("broker '{id}' not found")));
    }
    redis.hset(&key, &[("logo", body.logo.trim())]).await?;

    log::info!("updated logo for broker {id}");

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct UpdateOpenAccountUrl {
    open_account_url: String,
}

async fn update_open_account_url(
    State(redis): State<RedisService>,
    Path(id): Path<String>,
    Json(body): Json<UpdateOpenAccountUrl>,
) -> Result<StatusCode, ApiError> {
    let mut redis = redis;
    let key = broker_key(&id);

    if redis.hgetall(&key).await?.is_empty() {
        return Err(ApiError::NotFound(format!("broker '{id}' not found")));
    }
    redis
        .hset(&key, &[("open_account_url", body.open_account_url.trim())])
        .await?;

    log::info!("updated open_account_url for broker {id}");

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct UpdateAllowedIps {
    #[serde(default)]
    allowed_ips: Vec<String>,
}

/// Replace a broker's IP whitelist. An empty list removes all restrictions.
async fn update_allowed_ips(
    State(redis): State<RedisService>,
    Path(id): Path<String>,
    Json(body): Json<UpdateAllowedIps>,
) -> Result<Json<Broker>, ApiError> {
    let mut redis = redis;
    let key = broker_key(&id);

    let fields = redis.hgetall(&key).await?;
    if fields.is_empty() {
        return Err(ApiError::NotFound(format!("broker '{id}' not found")));
    }

    let allowed_ips = normalize_ip_list(&body.allowed_ips)?;
    redis
        .hset(&key, &[("allowed_ips", allowed_ips.as_str())])
        .await?;

    log::info!("updated allowed_ips for broker {id}");

    Ok(Json(Broker {
        name: fields.get("name").cloned().unwrap_or_default(),
        has_key: fields.get("api_key").is_some_and(|k| !k.is_empty()),
        allowed_ips: parse_ip_list(Some(allowed_ips.as_str())),
        open_account_url: fields.get("open_account_url").cloned().unwrap_or_default(),
        logo: fields.get("logo").filter(|v| !v.is_empty()).cloned(),
        id,
    }))
}

/// Remove a broker and its live stream + snapshot.
async fn delete_broker(
    State(redis): State<RedisService>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let mut redis = redis;
    let key = broker_key(&id);

    if redis.hgetall(&key).await?.is_empty() {
        return Err(ApiError::NotFound(format!("broker '{id}' not found")));
    }

    let live = format!("tradingbutler:{id}:live");
    let snapshot = format!("tradingbutler:{id}:snapshot");
    redis
        .pipeline(|pipe| {
            pipe.del(key.as_str());
            pipe.del(live.as_str());
            pipe.del(snapshot.as_str());
        })
        .await?;

    log::info!("deleted broker {id}");

    Ok(StatusCode::NO_CONTENT)
}

/// 256 bits of entropy as 64 hex chars.
fn new_api_key() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn sha512_hex(input: &str) -> String {
    let mut hasher = Sha512::new();
    hasher.update(input.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Split a stored comma-separated whitelist into individual entries.
fn parse_ip_list(stored: Option<&str>) -> Vec<String> {
    stored
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Validate, trim and de-duplicate whitelist entries, returning the canonical
/// comma-separated form to store. Each entry may itself contain commas (so the
/// client can send one string or many). Accepts bare IPs and CIDRs.
fn normalize_ip_list(entries: &[String]) -> Result<String, ApiError> {
    let mut out: Vec<String> = Vec::new();
    for raw in entries {
        for part in raw.split(',') {
            let entry = part.trim();
            if entry.is_empty() {
                continue;
            }
            validate_ip_or_cidr(entry)?;
            if !out.iter().any(|e| e == entry) {
                out.push(entry.to_owned());
            }
        }
    }
    Ok(out.join(","))
}

fn validate_ip_or_cidr(entry: &str) -> Result<(), ApiError> {
    let (ip_part, prefix_part) = match entry.split_once('/') {
        Some((ip, prefix)) => (ip, Some(prefix)),
        None => (entry, None),
    };

    let ip: IpAddr = ip_part
        .parse()
        .map_err(|_| ApiError::BadRequest(format!("invalid IP address: '{entry}'")))?;

    if let Some(prefix) = prefix_part {
        let max = if ip.is_ipv4() { 32 } else { 128 };
        let bits: u8 = prefix
            .parse()
            .map_err(|_| ApiError::BadRequest(format!("invalid CIDR prefix: '{entry}'")))?;
        if bits > max {
            return Err(ApiError::BadRequest(format!(
                "CIDR prefix out of range: '{entry}'"
            )));
        }
    }
    Ok(())
}

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    Internal(String),
}

impl From<redis::RedisError> for ApiError {
    fn from(e: redis::RedisError) -> Self {
        ApiError::Internal(e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Conflict(m) => (StatusCode::CONFLICT, m),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_ip_list, sha512_hex};

    #[test]
    fn sha512_hex_matches_known_digest() {
        // Same digest openssl/sha2 produce for "abc".
        assert_eq!(
            sha512_hex("abc"),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
             2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        );
    }

    #[test]
    fn normalize_ip_list_trims_dedupes_and_validates() {
        let out = normalize_ip_list(&[
            " 1.2.3.4 ".into(),
            "10.0.0.0/8,1.2.3.4".into(),
            "::1".into(),
        ])
        .unwrap();
        assert_eq!(out, "1.2.3.4,10.0.0.0/8,::1");

        assert!(normalize_ip_list(&[String::new()]).unwrap().is_empty());
        assert!(normalize_ip_list(&["not-an-ip".into()]).is_err());
        assert!(normalize_ip_list(&["1.2.3.4/40".into()]).is_err());
    }
}
