use axum::{
    Router,
    extract::ws::{Message, WebSocket},
    extract::{State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
};
use common::{RedisService, StreamPosition, env::Env};
use futures_util::StreamExt;
use log::{debug, info, warn};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use uuid::Uuid;

const BROKER_KEY_PREFIX: &str = "tradingbutler:broker:";
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);

/// Slow clients that fall behind by this many messages are disconnected.
const BROADCAST_CAPACITY: usize = 1024;

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<Arc<String>>,
}

pub struct RateStreamer {
    redis: RedisService,
    bind: SocketAddr,
}

impl RateStreamer {
    pub async fn init(env: &Env) -> anyhow::Result<Self> {
        let redis = RedisService::new(&env.redis_url).await?;
        let bind: SocketAddr = format!("{}:{}", env.http_host, env.http_port).parse()?;
        Ok(Self { redis, bind })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);

        let state = AppState { tx: tx.clone() };

        tokio::spawn(discover_brokers(self.redis, tx));

        let router = Router::new()
            .route("/ws", get(ws_handler))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(self.bind).await?;
        info!("listening on {}", self.bind);

        axum::serve(listener, router).await?;
        Ok(())
    }
}

async fn ws_handler(State(state): State<AppState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    let conn_id = Uuid::new_v4();
    info!("[{conn_id}] websocket connected");

    let mut rx = state.tx.subscribe();

    loop {
        tokio::select! {
            tick = rx.recv() => {
                match tick {
                    Ok(json) => {
                        debug!("[{conn_id}] forwarding message to websocket subscriber");
                        if socket
                            .send(Message::Binary(json.as_bytes().to_vec().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("[{conn_id}] lagged by {n} messages, disconnecting");
                        break;
                    }
                    Err(_) => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
        }
    }

    info!("[{conn_id}] websocket disconnected");
}

/// Periodically scans for registered brokers and spawns a `stream_reader` task
/// for each new one. Each rate-streamer instance reads independently of `$`
/// (NewOnly) — no consumer group, no ACK, full fan-out to every instance.
async fn discover_brokers(mut redis: RedisService, tx: broadcast::Sender<Arc<String>>) {
    let mut active: HashSet<String> = HashSet::new();
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

        for key in keys {
            let Some(broker_id) = key.strip_prefix(BROKER_KEY_PREFIX) else {
                continue;
            };

            let broker_id = broker_id.to_string();

            if active.insert(broker_id.clone()) {
                info!("discovered broker {}, starting stream reader", broker_id);

                let reader = redis.clone();
                let tx = tx.clone();
                let bid = broker_id.clone();

                tokio::spawn(async move {
                    let live_key = format!("tradingbutler:{}:live", bid);
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
