use log::info;
use rate_streamer::RateStreamer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    env_logger::init();

    info!("starting rate-streamer {}", common::VERSION);

    let env = common::env::Env::init()?;
    let svc = RateStreamer::init(&env).await?;
    svc.start().await
}
