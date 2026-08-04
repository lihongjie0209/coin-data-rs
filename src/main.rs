use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use coin_data_rs::{
    api::{ApiState, DatasetState, router},
    archive::Archiver,
    collector,
    config::Config,
    futures_poll::OpenInterestPoller,
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

    let root_config = Config::parse();
    let (buffer_mb, segment_mb) = root_config.buffer_sizes()?;
    tracing::info!(buffer_mb, segment_mb, "Parquet buffering configured");
    let metrics = Arc::new(Metrics::default());
    let (writer, completed_segments) = Writer::start(
        root_config.database.clone(),
        root_config.exchange.as_str().to_owned(),
        root_config.queue_capacity,
        buffer_mb.saturating_mul(1_024 * 1_024),
        segment_mb.saturating_mul(1_024 * 1_024),
        root_config.flush_interval(),
        Arc::clone(&metrics),
    )?;
    let notifier = TelegramNotifier::from_env(
        Arc::clone(&metrics),
        root_config.exchange.as_str().to_owned(),
    );
    let archiver = Arc::new(Archiver::new(&root_config, writer.clone(), notifier).await);
    Arc::clone(&archiver).spawn(completed_segments);
    let mut datasets = BTreeMap::new();
    for config in root_config.dataset_configs() {
        let name = config.market.as_str().to_owned();
        start_dataset(config, writer.clone(), Arc::clone(&metrics)).await?;
        let state = DatasetState {
            writer: writer.clone(),
            metrics: Arc::clone(&metrics),
            archiver: Arc::clone(&archiver),
        };
        datasets.insert(name, state);
    }
    let state = ApiState {
        datasets: Arc::new(datasets),
    };
    let listener = TcpListener::bind(&root_config.api_address)
        .await
        .with_context(|| format!("bind API on {}", root_config.api_address))?;
    tracing::info!(address = %root_config.api_address, "API listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown())
        .await
        .context("serve API")
}

async fn start_dataset(config: Config, writer: Writer, metrics: Arc<Metrics>) -> Result<()> {
    let symbols = config.resolve_symbols().await?;
    config.validate(&symbols)?;
    tracing::info!(
        exchange = config.exchange.as_str(),
        market = config.market.as_str(),
        symbols = symbols.len(),
        connections = config.connection_count(symbols.len()),
        "symbol universe resolved"
    );
    if config.exchange == coin_data_rs::config::Exchange::Binance
        && config.market != coin_data_rs::config::Market::Spot
    {
        OpenInterestPoller::new(
            config.rest_url(),
            symbols.clone(),
            config.market,
            writer.clone(),
        )
        .spawn();
    }
    let mut shard_id = 0;
    let groups = config.stream_groups();
    let connection_counts = config.connection_counts(symbols.len());
    for (group, connection_count) in groups.into_iter().zip(connection_counts) {
        for streams in config.shards(&symbols, &group.streams, connection_count) {
            tokio::spawn(collector::run_shard(
                shard_id,
                group.name,
                group.url.clone(),
                streams,
                writer.clone(),
                Arc::clone(&metrics),
                config.market,
            ));
            shard_id += 1;
        }
    }

    Ok(())
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown requested");
}
