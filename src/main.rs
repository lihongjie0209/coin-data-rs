use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use coin_data_rs::{
    api::{ApiState, DatasetState, router},
    collector,
    config::Config,
    futures_poll::OpenInterestPoller,
    notify::TelegramNotifier,
    runtime::Metrics,
    stream_writer::StreamWriter,
    uploader::{Uploader, UploaderConfig},
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
    let mut datasets = BTreeMap::new();
    for config in root_config.dataset_configs() {
        let name = config.market.as_str().to_owned();
        let state = start_dataset(config).await?;
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

async fn start_dataset(config: Config) -> Result<DatasetState> {
    let symbols = config.resolve_symbols().await?;
    config.validate(&symbols)?;
    tracing::info!(
        exchange = config.exchange.as_str(),
        market = config.market.as_str(),
        symbols = symbols.len(),
        connections = config.connection_count(symbols.len()),
        "symbol universe resolved"
    );
    let metrics = Arc::new(Metrics::default());
    let directory = config.dataset_parquet_dir();
    let notifier = TelegramNotifier::from_env(
        Arc::clone(&metrics),
        format!("{}/{}", config.exchange.as_str(), config.market.as_str()),
    );
    let uploader = Uploader::start(UploaderConfig {
        directory: directory.clone(),
        bucket: config.s3_bucket.clone(),
        prefix: config.dataset_s3_prefix(),
        region: config.aws_region.clone(),
        min_retention_hours: config.min_retention_hours,
        max_retention_hours: config.max_retention_hours,
        min_free_disk_percent: config.min_free_disk_percent,
        notifier,
    })
    .await;
    let writer = StreamWriter::start(
        directory.clone(),
        config.queue_capacity,
        Arc::clone(&metrics),
        uploader.clone(),
    );
    coin_data_rs::backfill::Backfiller::new(
        config.rest_url(),
        symbols.clone(),
        writer.clone(),
        config.market,
        directory.clone(),
    )
    .spawn();
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

    Ok(DatasetState {
        writer,
        metrics,
        uploader,
        directory,
    })
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown requested");
}
