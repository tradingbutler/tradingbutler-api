use admin_api::AdminApi;
use log::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    env_logger::init();

    info!("starting admin-api {}", common::VERSION);

    let env = common::env::Env::init()?;
    let svc = AdminApi::init(&env).await?;
    svc.start().await
}
