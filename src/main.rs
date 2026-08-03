use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use coin_data_rs::{
    api::{ApiState, router},
    archive::Archiver,
    backfill::Backfiller,
    collector,
    config::Config,
    notify::TelegramNotifier,
    runtime::Metrics,
    writer::Writer,
};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "coin_data=info".into()),
        )
        .init();

    let config = Config::parse();
    let symbols = config.resolve_symbols().await?;
    config.validate(&symbols)?;
    tracing::info!(
        symbols = symbols.len(),
        connections = config.connection_count(symbols.len()),
        "symbol universe resolved"
    );
    let metrics = Arc::new(Metrics::default());
    let writer = Writer::start(
        config.database.clone(),
        config.queue_capacity,
        config.batch_size,
        config.flush_interval(),
        Arc::clone(&metrics),
    );
    let backfiller = Backfiller::new(config.rest_url.clone(), symbols.clone(), writer.clone());
    backfiller.clone().spawn();
    let notifier = TelegramNotifier::from_env(Arc::clone(&metrics));
    let archiver = Arc::new(Archiver::new(&config, writer.clone(), backfiller, notifier).await);
    Arc::clone(&archiver).spawn_hourly();

    for (id, streams) in config.shards(&symbols).into_iter().enumerate() {
        tokio::spawn(collector::run_shard(
            id,
            config.ws_url.clone(),
            streams,
            writer.clone(),
            Arc::clone(&metrics),
        ));
    }

    let state = ApiState {
        writer,
        metrics,
        archiver,
    };
    let listener = TcpListener::bind(&config.api_address)
        .await
        .with_context(|| format!("bind API on {}", config.api_address))?;
    tracing::info!(address = %config.api_address, "API listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown())
        .await
        .context("serve API")
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown requested");
}
