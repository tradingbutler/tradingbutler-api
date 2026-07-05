use axum_client_ip::ClientIpSource;
use envconfig::{Envconfig, Error};
use std::path::PathBuf;

#[derive(Envconfig, Debug)]
pub struct Env {
    /// Docker sets this to the container's own id automatically, giving each
    /// scaled replica a distinct value with no compose-level wiring needed.
    #[envconfig(from = "HOSTNAME")]
    pub id: String,

    #[envconfig(from = "REDIS_URL", default = "redis://127.0.0.1")]
    pub redis_url: String,

    #[envconfig(from = "REDIS_NAMESPACE", default = "tradingbuttler")]
    pub redis_namespace: String,

    #[envconfig(from = "HTTP_HOST", default = "0.0.0.0")]
    pub http_host: String,

    #[envconfig(from = "HTTP_PORT", default = "20000")]
    pub http_port: u16,

    #[envconfig(from = "IP_SOURCE", default = "ConnectInfo")]
    pub ip_source: ClientIpSource,

    #[envconfig(from = "BROKERS_SNAPSHOT_FILE", default = "brokers.json")]
    pub brokers_snapshot_path: PathBuf,

    #[envconfig(from = "JSON_SNAPSHOT_FILE", default = "rates.json")]
    pub json_snapshot_path: PathBuf,
}

impl Env {
    pub fn init() -> Result<Self, Error> {
        Self::init_from_env()
    }
}
