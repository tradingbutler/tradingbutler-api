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
use common::{RedisService, env::Env};
use ipnet::IpNet;
use log::{debug, info, warn};
use redis::streams::StreamMaxlen;
use common::broker::Broker;
use rhiaqey_sdk_rs::message::MessageValue;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub(crate) redis: Arc<RwLock<RedisService>>,
}

#[derive(Clone)]
pub struct Collector {
    bind: SocketAddr,
    ip_source: ClientIpSource,
    state: AppState,
}

impl Collector {
    pub async fn init(env: &Env) -> anyhow::Result<Self> {
        let redis = RedisService::new(&env.redis_url).await?;
        let bind: SocketAddr = format!("{}:{}", env.http_host, env.http_port).parse()?;
        let ip_source = env.ip_source.clone();

        let state = AppState {
            redis: Arc::new(RwLock::new(redis)),
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

async fn handle_socket(mut socket: WebSocket, state: AppState, conn_id: String, client_ip: IpAddr) {
    let mut conn_broker: Option<Broker> = None;

    while let Some(Ok(msg)) = socket.recv().await {
        match msg {
            Message::Binary(raw) => {
                match handle_binary_message(raw, &state, &conn_id, &conn_broker, client_ip).await {
                    Ok(CollectedValue::Broker(broker)) => {
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
                let broker: Broker = serde_json::from_value(value)?;
                let broker_key = format!("tradingbutler:broker:{}", broker.id);
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
                info!("[{}] authenticated broker {}", conn_id, broker.id);
                return Ok(CollectedValue::Broker(broker));
            }
            "live" => {
                let Some(b) = broker else {
                    warn!("[{}] received live message before broker info, closing connection", conn_id);
                    return Ok(CollectedValue::CloseConnection);
                };
                let json = value.to_string();
                let stream_key = format!("tradingbutler:{}:live", b.id);
                let snapshot_key = format!("tradingbutler:{}:snapshot", b.id);
                state
                    .redis
                    .write()
                    .await
                    .pipeline(|pipe| {
                        pipe.xadd_maxlen(
                            &stream_key,
                            StreamMaxlen::Approx(1_000),
                            "*",
                            &[
                                ("conn_id", conn_id),
                                ("key", message.key.as_str()),
                                ("data", json.as_str()),
                            ],
                        );
                        pipe.hset(&snapshot_key, message.key.as_str(), json.as_str());
                    })
                    .await?;
                info!(
                    "[{}] published live message {} to stream {}",
                    conn_id, message.key, stream_key
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
        } else if let Ok(single) = entry.parse::<IpAddr>() {
            if single == ip {
                return true;
            }
        }
    }
    false
}
