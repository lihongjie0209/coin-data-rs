use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use anyhow::{Result, bail};
use clap::Parser;

pub const DEFAULT_SYMBOLS: &str =
    "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,TRXUSDT,LINKUSDT,LTCUSDT";
pub const DEFAULT_STREAMS: &str = "depth@100ms,aggTrade,trade,kline_1s,miniTicker,ticker,ticker_1h,ticker_4h,ticker_1d,bookTicker,avgPrice";

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, default_value = DEFAULT_SYMBOLS)]
    symbols: String,
    #[arg(long, default_value = DEFAULT_STREAMS)]
    streams: String,
    #[arg(long, default_value_t = 4)]
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
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        let symbols = self.symbols();
        let streams = self.streams();
        if symbols.is_empty() || streams.is_empty() {
            bail!("symbols and streams must not be empty");
        }
        let connections = self.ws_connections.min(symbols.len());
        if connections == 0 || symbols.len().div_ceil(connections) * streams.len() > 1_024 {
            bail!("too many streams per connection");
        }
        NonZeroUsize::new(self.batch_size)
            .ok_or_else(|| anyhow::anyhow!("batch size must be positive"))?;
        NonZeroUsize::new(self.queue_capacity)
            .ok_or_else(|| anyhow::anyhow!("queue capacity must be positive"))?;
        Ok(())
    }

    pub fn symbols(&self) -> Vec<String> {
        csv(&self.symbols, true)
    }

    pub fn streams(&self) -> Vec<String> {
        csv(&self.streams, false)
    }

    pub const fn flush_interval(&self) -> Duration {
        Duration::from_secs(self.flush_seconds)
    }

    pub fn shards(&self) -> Vec<Vec<String>> {
        let symbols = self.symbols();
        let streams = self.streams();
        let count = self.ws_connections.min(symbols.len());
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
