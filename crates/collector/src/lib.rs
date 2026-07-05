use anyhow::bail;
use axum::extract::State;
use axum::{
    Router,
    extract::WebSocketUpgrade,
    extract::ws::{Message, WebSocket},
    response::IntoResponse,
    routing::get,
};
use axum_client_ip::{ClientIp, ClientIpSource};
use bytes::Bytes;
use common::broker::Broker;
use common::{RedisService, env::Env};
use ipnet::IpNet;
use log::{debug, info, warn};
use redis::streams::StreamMaxlen;
use rhiaqey_sdk_rs::message::MessageValue;
use serde::Serialize;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub(crate) redis: Arc<RwLock<RedisService>>,
    pub(crate) analytics_connections_key: Arc<str>,
}

#[derive(Clone)]
pub struct Collector {
    bind: SocketAddr,
    ip_source: ClientIpSource,
    state: AppState,
}

impl Collector {
    pub async fn init(env: &Env) -> anyhow::Result<Self> {
        let mut redis = RedisService::new(&env.redis_url, &env.redis_namespace).await?;
        let bind: SocketAddr = format!("{}:{}", env.http_host, env.http_port).parse()?;
        let ip_source = env.ip_source.clone();

        let analytics_connections_key: Arc<str> =
            format!("analytics:collector:{}:connections", env.id).into();

        // Clear any stale entries left over from a previous run of this instance —
        // connections that never got a chance to `remove_connection` on crash/restart.
        redis.del(&analytics_connections_key).await?;

        let state = AppState {
            redis: Arc::new(RwLock::new(redis)),
            analytics_connections_key,
        };

        Ok(Self {
            bind,
            ip_source,
            state,
        })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let (shutdown_tx, mut _shutdown_rx) = broadcast::channel(1);

        let router = Router::new()
            .route("/ws", get(ws_handler))
            .merge(common::health::router())
            .layer(self.ip_source.into_extension())
            .with_state(self.state);

        let listener = tokio::net::TcpListener::bind(self.bind).await?;
        info!("listening on {}", self.bind);

        if let Err(err) = axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = _shutdown_rx.recv().await;
        })
        .await
        {
            warn!("http server error: {err}");
        }

        let _ = shutdown_tx.send(());

        Ok(())
    }
}

async fn ws_handler(
    State(state): State<AppState>,
    ClientIp(ip): ClientIp,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let conn_id = Uuid::new_v4().to_string();
    info!("new websocket connection {} from {}", conn_id, ip);
    ws.on_upgrade(move |socket| handle_socket(socket, state, conn_id, ip))
}

enum CollectedValue {
    None,
    Broker(Broker),
    CloseConnection,
}

#[derive(Serialize)]
struct ConnectionInfo<'a> {
    id: &'a str,
    ip: String,
    broker: Option<&'a str>,
    connected_at: u64,
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Record or update this connection's analytics entry (hash field = conn_id, so
/// concurrent connections never race — each task only ever writes its own field).
async fn record_connection(
    state: &AppState,
    conn_id: &str,
    ip: IpAddr,
    connected_at: u64,
    broker: Option<&str>,
) -> anyhow::Result<()> {
    let info = ConnectionInfo {
        id: conn_id,
        ip: ip.to_string(),
        broker,
        connected_at,
    };
    let json = serde_json::to_string(&info)?;
    state
        .redis
        .write()
        .await
        .hset(
            &state.analytics_connections_key,
            &[(conn_id, json.as_str())],
        )
        .await?;
    Ok(())
}

async fn remove_connection(state: &AppState, conn_id: &str) -> anyhow::Result<()> {
    state
        .redis
        .write()
        .await
        .hdel(&state.analytics_connections_key, conn_id)
        .await?;
    Ok(())
}

async fn handle_socket(mut socket: WebSocket, state: AppState, conn_id: String, client_ip: IpAddr) {
    let mut conn_broker: Option<Broker> = None;
    let connected_at = unix_timestamp();

    if let Err(err) = record_connection(&state, &conn_id, client_ip, connected_at, None).await {
        warn!("[{}] failed to record connection analytics: {err}", conn_id);
    }

    while let Some(Ok(msg)) = socket.recv().await {
        match msg {
            Message::Binary(raw) => {
                match handle_binary_message(raw, &state, &conn_id, &conn_broker, client_ip).await {
                    Ok(CollectedValue::Broker(broker)) => {
                        if let Err(err) = record_connection(
                            &state,
                            &conn_id,
                            client_ip,
                            connected_at,
                            Some(broker.id.as_str()),
                        )
                        .await
                        {
                            warn!("[{}] failed to update connection analytics: {err}", conn_id);
                        }
                        conn_broker = Some(broker);
                    }
                    Ok(CollectedValue::None) => {
                        //
                    }
                    Ok(CollectedValue::CloseConnection) => {
                        break;
                    }
                    Err(err) => {
                        let msg = err.to_string();
                        if msg.contains("broken pipe") || msg.contains("connection reset") {
                            debug!("[{}] transient connection error: {err}", conn_id);
                        } else {
                            warn!("[{}] message handling error: {err}", conn_id);
                        }
                    }
                }
            }
            Message::Close(_) => {
                warn!("[{}] websocket closed", conn_id);
                break;
            }
            _ => {
                warn!(
                    "[{}] websocket received unexpected message: {msg:?}",
                    conn_id
                );
                break;
            }
        }
    }

    if let Err(err) = remove_connection(&state, &conn_id).await {
        warn!("[{}] failed to remove connection analytics: {err}", conn_id);
    }

    info!("[{}] connection cleaned up", conn_id);
}

async fn handle_binary_message(
    raw: Bytes,
    state: &AppState,
    conn_id: &str,
    broker: &Option<Broker>,
    client_ip: IpAddr,
) -> anyhow::Result<CollectedValue> {
    if raw.len() >= 4 && raw.as_ref() == b"ping" {
        return Ok(CollectedValue::None);
    }

    let message: rhiaqey_sdk_rs::gateway::GatewayMessage = serde_json::from_slice(&raw)?;

    let MessageValue::Json(value) = message.value else {
        bail!("received non-json message: {:?}", message.value);
    };

    match message.category {
        Some(category) => match category.to_lowercase().as_str() {
            "broker" => {
                let mut broker: Broker = serde_json::from_value(value)?;
                let broker_key = format!("brokers:{}", broker.id);
                let fields = state.redis.write().await.hgetall(&broker_key).await?;
                if fields.is_empty() {
                    bail!(
                        "[{}] broker '{}' not registered — register it in the admin dashboard first",
                        conn_id,
                        broker.id
                    );
                }
                let stored_key = fields.get("api_key").map(String::as_str).unwrap_or("");
                if stored_key.is_empty() || stored_key != broker.api_key {
                    bail!("[{}] invalid api_key for broker '{}'", conn_id, broker.id);
                }
                let whitelist = fields.get("allowed_ips").map(String::as_str).unwrap_or("");
                if !whitelist.is_empty() && !ip_allowed(client_ip, whitelist) {
                    bail!(
                        "[{}] broker '{}': client IP {} not in whitelist",
                        conn_id,
                        broker.id,
                        client_ip
                    );
                }
                if let Some(raw) = fields.get("symbol_map") {
                    match serde_json::from_str::<HashMap<String, String>>(raw) {
                        Ok(map) => broker.symbol_map = map,
                        Err(err) => warn!(
                            "[{}] broker '{}': invalid symbol_map JSON, ignoring: {err}",
                            conn_id, broker.id
                        ),
                    }
                }
                info!("[{}] authenticated broker {}", conn_id, broker.id);
                return Ok(CollectedValue::Broker(broker));
            }
            "live" => {
                let Some(b) = broker else {
                    warn!(
                        "[{}] received live message before broker info, closing connection",
                        conn_id
                    );
                    return Ok(CollectedValue::CloseConnection);
                };
                let symbol = match b.canonical_symbol(&message.key) {
                    Some(canonical) => canonical.to_string(),
                    None => {
                        if !b.symbol_map.is_empty() {
                            debug!(
                                "[{}] broker '{}': no symbol mapping for '{}', storing as-is",
                                conn_id, b.id, message.key
                            );
                        }
                        message.key.clone()
                    }
                };
                let json = value.to_string();
                let stream_key = format!("brokers:{}:live", b.id);
                let snapshot_key = format!("brokers:{}:snapshot", b.id);
                let mut redis = state.redis.write().await;
                let ns_stream_key = redis.key(&stream_key);
                let ns_snapshot_key = redis.key(&snapshot_key);
                redis
                    .pipeline(|pipe| {
                        pipe.xadd_maxlen(
                            &ns_stream_key,
                            StreamMaxlen::Approx(1_000),
                            "*",
                            &[
                                ("conn_id", conn_id),
                                ("key", symbol.as_str()),
                                ("data", json.as_str()),
                            ],
                        );
                        pipe.hset(&ns_snapshot_key, symbol.as_str(), json.as_str());
                    })
                    .await?;
                debug!(
                    "[{}] published live message {} (from '{}') to stream {}",
                    conn_id, symbol, message.key, stream_key
                );
            }
            &_ => {
                warn!("[{conn_id}] unsupported gateway message category: {category:?}",);
            }
        },
        None => {
            warn!("[{}] received non-categorized message", conn_id);
        }
    }

    Ok(CollectedValue::None)
}

/// Returns true if `ip` matches any entry in the comma-separated `whitelist`.
/// Each entry may be a bare IP or a CIDR range.
fn ip_allowed(ip: IpAddr, whitelist: &str) -> bool {
    for entry in whitelist.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        if let Ok(net) = entry.parse::<IpNet>() {
            if net.contains(&ip) {
                return true;
            }
        } else if let Ok(single) = entry.parse::<IpAddr>()
            && single == ip
        {
            return true;
        }
    }
    false
}
