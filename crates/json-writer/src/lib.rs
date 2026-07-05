use common::{RedisService, StreamPosition, env::Env};
use futures_util::StreamExt;
use log::{debug, info, warn};
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;

const BROKER_KEY_PREFIX: &str = "broker:";
const CONSUMER_GROUP: &str = "json-writer";
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);
const BROKERS_WRITE_INTERVAL: Duration = Duration::from_secs(60);
const RATES_WRITE_INTERVAL: Duration = Duration::from_secs(30);

/// Sent from the discovery task to the rates-writer loop.
enum SnapshotEvent {
    /// A broker's snapshot hash changed — merge these symbol values in.
    Update(String, HashMap<String, String>),
    /// The broker's key disappeared from Redis (deleted via admin-api) —
    /// drop it from the rates state entirely.
    Removed(String),
}

pub struct JsonWriter {
    redis: RedisService,
    snapshot_path: PathBuf,
    brokers_snapshot_path: PathBuf,
    bind: SocketAddr,
}

impl JsonWriter {
    pub async fn init(env: &Env) -> anyhow::Result<Self> {
        let redis = RedisService::new(&env.redis_url, &env.redis_namespace).await?;
        let bind: SocketAddr = format!("{}:{}", env.http_host, env.http_port).parse()?;
        Ok(Self {
            redis,
            snapshot_path: env.json_snapshot_path.clone(),
            brokers_snapshot_path: env.brokers_snapshot_path.clone(),
            bind,
        })
    }

    pub async fn start(self) -> anyhow::Result<()> {
        tokio::spawn(serve_healthz(self.bind));

        let (tx, mut rx) = mpsc::unbounded_channel::<SnapshotEvent>();

        tokio::spawn(discover_brokers(
            self.redis.clone(),
            self.brokers_snapshot_path,
            tx,
        ));

        let snapshot_path = self.snapshot_path;
        let mut state: HashMap<String, HashMap<String, Value>> = HashMap::new();
        let mut write_interval = tokio::time::interval(RATES_WRITE_INTERVAL);
        write_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(SnapshotEvent::Update(broker_id, snapshot)) => {
                            debug!("[{}] received snapshot with {} symbols", broker_id, snapshot.len());
                            let broker_state = state.entry(broker_id).or_default();
                            for (symbol, json_str) in snapshot {
                                match serde_json::from_str::<Value>(&json_str) {
                                    Ok(v) => { broker_state.insert(symbol, v); }
                                    Err(e) => warn!("failed to parse snapshot value: {e}"),
                                }
                            }
                        }
                        Some(SnapshotEvent::Removed(broker_id)) => {
                            if state.remove(&broker_id).is_some() {
                                info!("[{}] broker deleted, dropped from rates state", broker_id);
                            }
                        }
                        None => break,
                    }
                }
                _ = write_interval.tick() => {
                    if state.is_empty() {
                        continue;
                    }
                    match write_json_atomic(&snapshot_path, &state).await {
                        Ok(_) => info!("rates snapshot ({} brokers) written to {:?}", state.len(), snapshot_path),
                        Err(e) => warn!("failed to write rates snapshot: {e}"),
                    }
                }
            }
        }

        Ok(())
    }
}

async fn serve_healthz(bind: SocketAddr) {
    let router = common::health::router();
    match tokio::net::TcpListener::bind(bind).await {
        Ok(listener) => {
            info!("healthz listening on {}", bind);
            if let Err(err) = axum::serve(listener, router).await {
                warn!("healthz server error: {err}");
            }
        }
        Err(err) => warn!("failed to bind healthz listener on {bind}: {err}"),
    }
}

async fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, &json).await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

async fn discover_brokers(
    mut redis: RedisService,
    brokers_snapshot_path: PathBuf,
    tx: mpsc::UnboundedSender<SnapshotEvent>,
) {
    let mut active: HashSet<String> = HashSet::new();
    let mut readers: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_brokers_write = tokio::time::Instant::now();

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
            .filter_map(|key| key.strip_prefix(BROKER_KEY_PREFIX).map(str::to_owned))
            .collect();

        // A broker that vanished from Redis was deleted via admin-api — stop its
        // reader task (its stream/snapshot are already gone) and drop it from
        // the rates state so it stops showing up in rates.json.
        let removed: Vec<String> = active.difference(&current_ids).cloned().collect();
        for broker_id in removed {
            active.remove(&broker_id);
            if let Some(handle) = readers.remove(&broker_id) {
                handle.abort();
            }
            let _ = tx.send(SnapshotEvent::Removed(broker_id.clone()));
            info!("broker {} deleted, stopped tracking", broker_id);
        }

        let mut brokers: HashMap<String, HashMap<String, String>> = HashMap::new();

        for key in keys {
            let Some(broker_id) = key.strip_prefix(BROKER_KEY_PREFIX) else {
                continue;
            };
            let broker_id = broker_id.to_string();

            match redis.hgetall(&key).await {
                Ok(mut fields) => {
                    fields.remove("api_key");
                    fields.remove("allowed_ips");
                    brokers.insert(broker_id.clone(), fields);
                }
                Err(e) => warn!("[{}] failed to read broker hash: {e}", broker_id),
            }

            if active.insert(broker_id.clone()) {
                info!("discovered broker {}, joining consumer group", broker_id);

                let stream_key = format!("{}:live", broker_id);
                let snapshot_key = format!("{}:snapshot", broker_id);
                let mut setup_redis = redis.clone();

                if let Err(e) = setup_redis
                    .ensure_consumer_group(&stream_key, CONSUMER_GROUP, StreamPosition::NewOnly)
                    .await
                {
                    warn!("[{}] failed to ensure consumer group: {e}", broker_id);
                    active.remove(&broker_id);
                    continue;
                }

                // Seed immediately from the snapshot hash so we don't miss ticks
                // that arrived before the consumer group was created.
                match setup_redis.hgetall(&snapshot_key).await {
                    Ok(snapshot) if !snapshot.is_empty() => {
                        let _ = tx.send(SnapshotEvent::Update(broker_id.clone(), snapshot));
                    }
                    Ok(_) => {}
                    Err(e) => warn!("[{}] initial snapshot read error: {e}", broker_id),
                }

                let reader_redis = redis.clone();
                let mut snapshot_redis = redis.clone();
                let tx = tx.clone();
                let bid = broker_id.clone();

                let handle = tokio::spawn(async move {
                    let live_key = format!("{}:live", bid);
                    let snapshot_key = format!("{}:snapshot", bid);
                    let stream = reader_redis.group_reader(
                        live_key.clone(),
                        CONSUMER_GROUP,
                        format!("json-writer-{}", bid),
                    );
                    futures_util::pin_mut!(stream);

                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(entry) => {
                                match snapshot_redis.hgetall(&snapshot_key).await {
                                    Ok(snapshot) => {
                                        let _ =
                                            tx.send(SnapshotEvent::Update(bid.clone(), snapshot));
                                    }
                                    Err(e) => warn!("[{}] hgetall error: {e}", bid),
                                }
                                if let Err(e) = snapshot_redis
                                    .ack(&live_key, CONSUMER_GROUP, &[&entry.id])
                                    .await
                                {
                                    warn!("[{}] ack error: {e}", bid);
                                }
                            }
                            Err(e) => {
                                // The stream/consumer group is gone, most likely because
                                // the broker was deleted; discovery will clean this up
                                // (and abort this task) on its next tick.
                                warn!("[{}] stream error: {e}", bid);
                                break;
                            }
                        }
                    }
                });
                readers.insert(broker_id.clone(), handle);
            }
        }

        if tokio::time::Instant::now() >= next_brokers_write {
            match write_json_atomic(&brokers_snapshot_path, &brokers).await {
                Ok(_) => info!(
                    "brokers snapshot ({} brokers) written to {:?}",
                    brokers.len(),
                    brokers_snapshot_path
                ),
                Err(e) => warn!("failed to write brokers snapshot: {e}"),
            }
            next_brokers_write = tokio::time::Instant::now() + BROKERS_WRITE_INTERVAL;
        }
    }
}
