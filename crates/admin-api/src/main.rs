use admin_api::AdminApi;
use common::cli::Command;
use log::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env = common::env::Env::init()?;

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    match common::cli::parse().command {
        Command::Version => {
            println!("{}", common::VERSION);
            Ok(())
        }
        Command::Healthcheck => common::health::check(&env.http_host, env.http_port).await,
        Command::Start => {
            env_logger::init();
            info!("starting rate-streamer {}", common::VERSION);
            let svc = AdminApi::init(&env).await?;
            svc.start().await
        }
    }
}
