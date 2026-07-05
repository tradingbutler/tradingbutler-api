use axum::{
    Router,
    extract::ws::{Message, WebSocket},
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
};
use axum_client_ip::{ClientIp, ClientIpSource};
use common::{RedisService, StreamPosition, env::Env};
use futures_util::StreamExt;
use log::{debug, info, warn};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

const BROKER_KEY_PREFIX: &str = "brokers:";
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);

/// `brokers:*` also matches the derived `brokers:{id}:live` and
/// `brokers:{id}:snapshot` keys; broker ids never contain `:` (enforced by
/// admin-api), so reject anything with a further colon after the prefix.
#[inline]
fn broker_id_from_key(key: &str) -> Option<String> {
    let id = key.strip_prefix(BROKER_KEY_PREFIX)?;
    (!id.contains(':')).then(|| id.to_string())
}

/// Slow clients that fall behind by this many messages are disconnected.
const BROADCAST_CAPACITY: usize = 1024;

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<Arc<String>>,
    redis: Arc<RwLock<RedisService>>,
    analytics_connections_key: Arc<str>,
}

pub struct RateStreamer {
    redis: RedisService,
    bind: SocketAddr,
    ip_source: ClientIpSource,
    analytics_connections_key: Arc<str>,
}

impl RateStreamer {
    pub async fn init(env: &Env) -> anyhow::Result<Self> {
        let mut redis = RedisService::new(&env.redis_url, &env.redis_namespace).await?;
        let bind: SocketAddr = format!("{}:{}", env.http_host, env.http_port).parse()?;
        let ip_source = env.ip_source.clone();

        let analytics_connections_key: Arc<str> =
            format!("analytics:rate-streamer:{}:connections", env.id()).into();

        // Clear any stale entries left over from a previous run of this instance —
        // connections that never got a chance to `remove_connection` on crash/restart.
        redis.del(&analytics_connections_key).await?;

        Ok(Self {
            redis,
            bind,
            ip_source,
            analytics_connections_key,
        })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);

        let discovery_redis = self.redis.clone();
        let state = AppState {
            tx: tx.clone(),
            redis: Arc::new(RwLock::new(self.redis)),
            analytics_connections_key: self.analytics_connections_key,
        };

        tokio::spawn(discover_brokers(discovery_redis, tx));

        let router = Router::new()
            .route("/ws", get(ws_handler))
            .merge(common::health::router())
            .layer(self.ip_source.into_extension())
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(self.bind).await?;
        info!("listening on {}", self.bind);

        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await?;
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

#[derive(Serialize)]
struct ConnectionInfo<'a> {
    id: &'a str,
    ip: String,
    connected_at: u64,
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Record this connection's analytics entry (hash field = conn_id, so concurrent
/// connections never race — each task only ever writes its own field).
async fn record_connection(state: &AppState, conn_id: &str, ip: IpAddr) -> anyhow::Result<()> {
    let info = ConnectionInfo {
        id: conn_id,
        ip: ip.to_string(),
        connected_at: unix_timestamp(),
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
    info!("[{conn_id}] websocket connected");

    if let Err(err) = record_connection(&state, &conn_id, client_ip).await {
        warn!("[{conn_id}] failed to record connection analytics: {err}");
    }

    let mut rx = state.tx.subscribe();

    loop {
        tokio::select! {
            tick = rx.recv() => {
                debug!("[{conn_id}] tick received");
                match tick {
                    Ok(json) => {
                        debug!("[{conn_id}] forwarding message to websocket subscriber");
                        if let Err(err) = socket
                            .send(Message::Binary(json.as_bytes().to_vec().into()))
                            .await
                        {
                            warn!("[{conn_id}] error sending binary: {err}");
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("[{conn_id}] lagged by {n} messages, disconnecting");
                        break;
                    }
                    Err(err) => {
                        warn!("[{conn_id}] error receiving message {err}");
                        break
                    },
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => {
                        debug!("[{conn_id}] closing websocket connection");
                        break
                    },
                    _ => {
                        warn!("[{conn_id}] unexpected message received. not closed or none");
                    }
                }
            }
        }
    }

    if let Err(err) = remove_connection(&state, &conn_id).await {
        warn!("[{conn_id}] failed to remove connection analytics: {err}");
    }

    info!("[{conn_id}] websocket disconnected");
}

/// Periodically scans for registered brokers and spawns a `stream_reader` task
/// for each new one. Each rate-streamer instance reads independently of `$`
/// (NewOnly) — no consumer group, no ACK, full fan-out to every instance.
async fn discover_brokers(mut redis: RedisService, tx: broadcast::Sender<Arc<String>>) {
    let mut active: HashSet<String> = HashSet::new();
    let mut readers: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let keys = loop {
            match redis.keys(&format!("{}*", BROKER_KEY_PREFIX)).await {
                Ok(k) => break k,
                Err(e) => {
                    warn!("broker discovery scan error: {e}, retrying in 5s");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        };

        let current_ids: HashSet<String> = keys
            .iter()
            .filter_map(|key| broker_id_from_key(key))
            .collect();

        // A broker that vanished from Redis was deleted via admin-api — stop its
        // stream reader task instead of leaving it blocked forever on a stream
        // that no longer exists.
        let removed: Vec<String> = active.difference(&current_ids).cloned().collect();
        for broker_id in removed {
            active.remove(&broker_id);
            if let Some(handle) = readers.remove(&broker_id) {
                handle.abort();
            }
            info!("broker {} deleted, stopped stream reader", broker_id);
        }

        for key in keys {
            let Some(broker_id) = broker_id_from_key(&key) else {
                continue;
            };

            if active.insert(broker_id.clone()) {
                info!("discovered broker {}, starting stream reader", broker_id);

                let reader = redis.clone();
                let tx = tx.clone();
                let bid = broker_id.clone();

                let handle = tokio::spawn(async move {
                    let live_key = format!("brokers:{}:live", bid);
                    let stream = reader.stream_reader(live_key, StreamPosition::NewOnly);
                    futures_util::pin_mut!(stream);

                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(entry) => {
                                let symbol = string_field(&entry.map, "key");
                                debug!("[{}] received message for symbol {}", bid, symbol);
                                let data_str = string_field(&entry.map, "data");
                                let data: serde_json::Value = serde_json::from_str(&data_str)
                                    .unwrap_or(serde_json::Value::Null);

                                let msg = serde_json::json!({
                                    "type": "tick",
                                    "broker": bid,
                                    "symbol": symbol,
                                    "data": data,
                                });

                                // SendError just means no subscribers yet — keep streaming.
                                let _ = tx.send(Arc::new(msg.to_string()));
                            }
                            Err(e) => warn!("[{}] stream error: {e}", bid),
                        }
                    }
                });
                readers.insert(broker_id.clone(), handle);
            }
        }
    }
}

/// Extract a UTF-8 string from a redis stream entry's map field.
fn string_field(map: &std::collections::HashMap<String, redis::Value>, key: &str) -> String {
    map.get(key)
        .and_then(|v| redis::from_redis_value(v.clone()).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{broker_id_from_key, string_field};
    use std::collections::HashMap;

    #[test]
    fn extracts_bare_broker_id() {
        assert_eq!(broker_id_from_key("brokers:b1"), Some("b1".to_string()));
    }

    #[test]
    fn rejects_derived_live_and_snapshot_keys() {
        assert_eq!(broker_id_from_key("brokers:b1:live"), None);
        assert_eq!(broker_id_from_key("brokers:b1:snapshot"), None);
    }

    #[test]
    fn string_field_extracts_and_defaults_to_empty() {
        let mut map = HashMap::new();
        map.insert(
            "key".to_string(),
            redis::Value::BulkString(b"EURUSD".to_vec()),
        );
        assert_eq!(string_field(&map, "key"), "EURUSD");
        assert_eq!(string_field(&map, "missing"), "");
    }
}
