use common::cli::Command;
use json_writer::JsonWriter;
use log::info;

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

    info!("starting json-writer {}", common::VERSION);

    let env = common::env::Env::init()?;
    let svc = JsonWriter::init(&env).await?;
    svc.start().await
}
