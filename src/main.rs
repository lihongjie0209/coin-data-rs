use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use coin_data_rs::{
    api::{ApiState, router},
    archive::Archiver,
    backfill::Backfiller,
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

    let config = Config::parse();
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
    let writer = Writer::start(
        config.database.clone(),
        config.queue_capacity,
        config.batch_size,
        config.flush_interval(),
        Arc::clone(&metrics),
    )?;
    let backfiller = if config.exchange == coin_data_rs::config::Exchange::Binance {
        let backfiller = Backfiller::new(
            config.rest_url(),
            symbols.clone(),
            writer.clone(),
            config.market,
        );
        backfiller.clone().spawn();
        Some(backfiller)
    } else {
        None
    };
    let notifier = TelegramNotifier::from_env(
        Arc::clone(&metrics),
        format!("{}/{}", config.exchange.as_str(), config.market.as_str()),
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
    let archiver = Arc::new(Archiver::new(&config, writer.clone(), backfiller, notifier).await);
    Arc::clone(&archiver).spawn_hourly();

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
