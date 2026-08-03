use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use anyhow::{Result, bail};
use clap::Parser;

pub const DEFAULT_SYMBOLS: &str = "ALL";
pub const DEFAULT_STREAMS: &str = "depth@100ms,aggTrade,trade,kline_1s,miniTicker,ticker,ticker_1h,ticker_4h,ticker_1d,bookTicker,avgPrice";

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, default_value = DEFAULT_SYMBOLS)]
    symbols: String,
    #[arg(long, default_value = DEFAULT_STREAMS)]
    streams: String,
    #[arg(
        long,
        default_value_t = 0,
        help = "0 automatically selects the minimum safe count"
    )]
    pub ws_connections: usize,
    #[arg(long, default_value = "wss://data-stream.binance.vision/stream")]
    pub ws_url: String,
    #[arg(long, default_value = "https://data-api.binance.vision")]
    pub rest_url: String,
    #[arg(long, default_value = "data/market.duckdb")]
    pub database: PathBuf,
    #[arg(long, default_value = "data/parquet")]
    pub parquet_dir: PathBuf,
    #[arg(long, default_value_t = 5_000)]
    pub batch_size: usize,
    #[arg(long, default_value_t = 100_000)]
    pub queue_capacity: usize,
    #[arg(long, default_value_t = 1)]
    flush_seconds: u64,
    #[arg(long, default_value = "127.0.0.1:8081")]
    pub api_address: String,
    #[arg(long, default_value = "coin-data-196920285698-ap-southeast-1-an")]
    pub s3_bucket: String,
    #[arg(long, default_value = "parquet-rust")]
    pub s3_prefix: String,
    #[arg(long, default_value = "ap-southeast-1")]
    pub aws_region: String,
    #[arg(long, default_value_t = 4)]
    pub min_retention_hours: u64,
    #[arg(long, default_value_t = 8)]
    pub max_retention_hours: u64,
    #[arg(long, default_value_t = 20)]
    pub min_free_disk_percent: u64,
}

impl Config {
    pub fn validate(&self, symbols: &[String]) -> Result<()> {
        let streams = self.streams();
        if symbols.is_empty() || streams.is_empty() {
            bail!("symbols and streams must not be empty");
        }
        NonZeroUsize::new(self.batch_size)
            .ok_or_else(|| anyhow::anyhow!("batch size must be positive"))?;
        NonZeroUsize::new(self.queue_capacity)
            .ok_or_else(|| anyhow::anyhow!("queue capacity must be positive"))?;
        if self.min_retention_hours < 4 || self.max_retention_hours < self.min_retention_hours {
            bail!("retention must keep at least 4 hours and max must be >= min");
        }
        if self.min_free_disk_percent > 100 {
            bail!("minimum free disk percent must be <= 100");
        }
        Ok(())
    }

    pub fn symbols(&self) -> Vec<String> {
        csv(&self.symbols, true)
    }

    pub async fn resolve_symbols(&self) -> Result<Vec<String>> {
        let configured = self.symbols();
        if configured.len() != 1 || configured[0] != "ALL" {
            return Ok(configured);
        }
        let response: serde_json::Value = reqwest::Client::new()
            .get(format!(
                "{}/api/v3/exchangeInfo",
                self.rest_url.trim_end_matches('/')
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let mut symbols = response
            .get("symbols")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| {
                item.get("status").and_then(serde_json::Value::as_str) == Some("TRADING")
            })
            .filter(|item| {
                item.get("isSpotTradingAllowed")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
            })
            .filter_map(|item| item.get("symbol").and_then(serde_json::Value::as_str))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        symbols.sort_unstable();
        symbols.dedup();
        if symbols.is_empty() {
            bail!("exchangeInfo returned no tradable spot symbols");
        }
        Ok(symbols)
    }

    pub fn streams(&self) -> Vec<String> {
        csv(&self.streams, false)
    }

    pub const fn flush_interval(&self) -> Duration {
        Duration::from_secs(self.flush_seconds)
    }

    pub fn connection_count(&self, symbol_count: usize) -> usize {
        let required = (symbol_count * self.streams().len()).div_ceil(1_024);
        self.ws_connections.max(required).min(symbol_count)
    }

    pub fn shards(&self, symbols: &[String]) -> Vec<Vec<String>> {
        let streams = self.streams();
        let count = self.connection_count(symbols.len());
        let mut shards = vec![Vec::new(); count];
        for (index, symbol) in symbols.iter().enumerate() {
            for stream in &streams {
                shards[index % count].push(format!("{}@{stream}", symbol.to_lowercase()));
            }
        }
        shards
    }
}

fn csv(value: &str, uppercase: bool) -> Vec<String> {
    let mut values = Vec::new();
    for item in value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let item = if uppercase {
            item.to_uppercase()
        } else {
            item.to_owned()
        };
        if !values.contains(&item) {
            values.push(item);
        }
    }
    values
}
