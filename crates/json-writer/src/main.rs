use json_writer::JsonWriter;
use log::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls crypto provider");

    env_logger::init();

    info!("starting json-writer {}", common::VERSION);

    let env = common::env::Env::init()?;
    let svc = JsonWriter::init(&env).await?;
    svc.start().await
}
