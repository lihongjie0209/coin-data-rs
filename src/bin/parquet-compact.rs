use anyhow::Result;
use clap::Parser;
use coin_data_rs::compactor::{Compactor, Options};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "coin_data=info".into()),
        )
        .init();
    Compactor::new(Options::parse()).await?.run().await
}
