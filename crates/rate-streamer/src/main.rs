use common::cli::Command;
use log::info;
use rate_streamer::RateStreamer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match common::cli::parse().command {
        Command::Version => {
            println!("{}", common::VERSION);
            return Ok(());
        }
        Command::Healthcheck => {
            let env = common::env::Env::init()?;
            return common::health::check(&env.http_host, env.http_port).await;
        }
        Command::Start => {}
    }

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    env_logger::init();

    info!("starting rate-streamer {}", common::VERSION);

    let env = common::env::Env::init()?;
    let svc = RateStreamer::init(&env).await?;
    svc.start().await
}
