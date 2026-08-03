use std::{num::NonZeroUsize, path::PathBuf, time::Duration};

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};

pub const DEFAULT_SYMBOLS: &str = "ALL";
pub const DEFAULT_STREAMS: &str = "depth@100ms,aggTrade,trade,kline_1s,miniTicker,ticker,ticker_1h,ticker_4h,ticker_1d,bookTicker,avgPrice";
pub const DEFAULT_FUTURES_STREAMS: &str =
    "depth@100ms,aggTrade,kline_1m,miniTicker,ticker,bookTicker,markPrice@1s,forceOrder";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGroup {
    pub name: &'static str,
    pub url: String,
    pub streams: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Exchange {
    Binance,
    Okx,
    Bybit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Market {
    Spot,
    Usdm,
    Coinm,
}

impl Market {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Usdm => "usdm",
            Self::Coinm => "coinm",
        }
    }
}

impl Exchange {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Okx => "okx",
            Self::Bybit => "bybit",
        }
    }

    pub const fn default_archive_minute(self) -> u32 {
        match self {
            Self::Binance => 5,
            Self::Okx => 20,
            Self::Bybit => 35,
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, value_enum, default_value_t = Exchange::Binance)]
    pub exchange: Exchange,
    #[arg(long, value_enum, default_value_t = Market::Spot)]
    pub market: Market,
    #[arg(
        long,
        default_value_t = true,
        action = clap::ArgAction::Set,
        help = "run spot, USD-M, and COIN-M together; set false for --market only"
    )]
    pub all_markets: bool,
    #[arg(long, default_value = DEFAULT_SYMBOLS)]
    symbols: String,
    #[arg(long)]
    streams: Option<String>,
    #[arg(
        long,
        default_value_t = 4,
        help = "desired total connection count; automatically increased for stream limits"
    )]
    pub ws_connections: usize,
    #[arg(long)]
    ws_url: Option<String>,
    #[arg(long)]
    rest_url: Option<String>,
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
    #[arg(long)]
    archive_minute: Option<u32>,
}

impl Config {
    pub fn dataset_configs(&self) -> Vec<Self> {
        let markets: &[Market] = if self.all_markets {
            &[Market::Spot, Market::Usdm, Market::Coinm]
        } else {
            std::slice::from_ref(&self.market)
        };
        markets
            .iter()
            .map(|market| {
                let mut config = self.clone();
                config.market = *market;
                config.all_markets = false;
                if self.all_markets {
                    let parent = self
                        .database
                        .parent()
                        .unwrap_or_else(|| std::path::Path::new("."));
                    config.database = parent.join(format!(
                        "{}-{}.duckdb",
                        self.exchange.as_str(),
                        market.as_str()
                    ));
                    config.archive_minute = None;
                }
                config
            })
            .collect()
    }
    pub fn validate(&self, symbols: &[String]) -> Result<()> {
        if self.exchange != Exchange::Binance {
            bail!("only Binance is enabled in this release");
        }
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
        if self.archive_minute() > 59 {
            bail!("archive minute must be <= 59");
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
        let client = reqwest::Client::new();
        let url = self.instrument_url();
        let mut delay = Duration::from_secs(1);
        let response = loop {
            match client.get(&url).send().await {
                Ok(response) => match response.error_for_status() {
                    Ok(response) => match response.json::<serde_json::Value>().await {
                        Ok(value) => break value,
                        Err(error) => tracing::warn!(%error, %url, "decode exchangeInfo failed"),
                    },
                    Err(error) => tracing::warn!(%error, %url, "exchangeInfo rejected"),
                },
                Err(error) => tracing::warn!(%error, %url, "fetch exchangeInfo failed"),
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        };
        let list = match (self.exchange, self.market) {
            (Exchange::Binance, _) => response.get("symbols"),
            (Exchange::Okx, _) => response.get("data"),
            (Exchange::Bybit, _) => response.pointer("/result/list"),
        };
        let mut symbols = list
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| match (self.exchange, self.market) {
                (Exchange::Binance, Market::Spot) => {
                    item.get("status").and_then(serde_json::Value::as_str) == Some("TRADING")
                        && item
                            .get("isSpotTradingAllowed")
                            .and_then(serde_json::Value::as_bool)
                            == Some(true)
                }
                (Exchange::Binance, Market::Usdm | Market::Coinm) => {
                    item.get("status")
                        .or_else(|| item.get("contractStatus"))
                        .and_then(serde_json::Value::as_str)
                        == Some("TRADING")
                }
                (Exchange::Okx, _) => {
                    item.get("state").and_then(serde_json::Value::as_str) == Some("live")
                }
                (Exchange::Bybit, _) => {
                    item.get("status").and_then(serde_json::Value::as_str) == Some("Trading")
                }
            })
            .filter_map(|item| match self.exchange {
                Exchange::Okx => item.get("instId"),
                Exchange::Binance | Exchange::Bybit => item.get("symbol"),
            })
            .filter_map(serde_json::Value::as_str)
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
        let defaults = if self.exchange == Exchange::Binance && self.market != Market::Spot {
            DEFAULT_FUTURES_STREAMS
        } else {
            DEFAULT_STREAMS
        };
        csv(self.streams.as_deref().unwrap_or(defaults), false)
    }

    pub const fn flush_interval(&self) -> Duration {
        Duration::from_secs(self.flush_seconds)
    }

    fn required_connection_count(&self, symbol_count: usize, stream_count: usize) -> usize {
        let required = match self.exchange {
            Exchange::Binance => (symbol_count * stream_count).div_ceil(1_024),
            Exchange::Okx | Exchange::Bybit => symbol_count.div_ceil(100),
        };
        required.max(1).min(symbol_count)
    }

    pub fn connection_count(&self, symbol_count: usize) -> usize {
        self.connection_counts(symbol_count).into_iter().sum()
    }

    pub fn connection_counts(&self, symbol_count: usize) -> Vec<usize> {
        let groups = self.stream_groups();
        let mut counts = groups
            .iter()
            .map(|group| self.required_connection_count(symbol_count, group.streams.len()))
            .collect::<Vec<_>>();
        let target = self.ws_connections.max(counts.iter().sum());
        while counts.iter().sum::<usize>() < target {
            let Some((index, _)) = counts
                .iter()
                .enumerate()
                .filter(|(_, count)| **count < symbol_count)
                .min_by_key(|(index, count)| {
                    (*count * 1_024).div_ceil(groups[*index].streams.len())
                })
            else {
                break;
            };
            counts[index] += 1;
        }
        counts
    }

    pub fn rest_url(&self) -> String {
        self.rest_url
            .clone()
            .unwrap_or_else(|| match (self.exchange, self.market) {
                (Exchange::Binance, Market::Spot) => "https://data-api.binance.vision".to_owned(),
                (Exchange::Binance, Market::Usdm) => "https://fapi.binance.com".to_owned(),
                (Exchange::Binance, Market::Coinm) => "https://dapi.binance.com".to_owned(),
                (Exchange::Okx, _) => "https://www.okx.com".to_owned(),
                (Exchange::Bybit, _) => "https://api.bybit.com".to_owned(),
            })
    }

    pub fn stream_groups(&self) -> Vec<StreamGroup> {
        let streams = self.streams();
        if let Some(url) = &self.ws_url {
            return vec![StreamGroup {
                name: "custom",
                url: url.clone(),
                streams,
            }];
        }
        if (self.exchange, self.market) == (Exchange::Binance, Market::Usdm) {
            let (public, market): (Vec<_>, Vec<_>) = streams
                .into_iter()
                .partition(|stream| stream.starts_with("depth") || stream.as_str() == "bookTicker");
            return [
                ("public", "wss://fstream.binance.com/public/stream", public),
                ("market", "wss://fstream.binance.com/market/stream", market),
            ]
            .into_iter()
            .filter(|(_, _, streams)| !streams.is_empty())
            .map(|(name, url, streams)| StreamGroup {
                name,
                url: url.to_owned(),
                streams,
            })
            .collect();
        }
        vec![StreamGroup {
            name: "market",
            url: match (self.exchange, self.market) {
                (Exchange::Binance, Market::Spot) => {
                    "wss://data-stream.binance.vision/stream".to_owned()
                }
                (Exchange::Binance, Market::Usdm) => unreachable!(),
                (Exchange::Binance, Market::Coinm) => "wss://dstream.binance.com/stream".to_owned(),
                (Exchange::Okx, _) => "wss://ws.okx.com:8443/ws/v5/public".to_owned(),
                (Exchange::Bybit, _) => "wss://stream.bybit.com/v5/public/spot".to_owned(),
            },
            streams,
        }]
    }

    pub fn archive_minute(&self) -> u32 {
        self.archive_minute
            .unwrap_or_else(|| match (self.exchange, self.market) {
                (Exchange::Binance, Market::Spot) => 5,
                (Exchange::Binance, Market::Usdm) => 20,
                (Exchange::Binance, Market::Coinm) => 35,
                _ => self.exchange.default_archive_minute(),
            })
    }

    pub fn dataset_parquet_dir(&self) -> PathBuf {
        self.parquet_dir
            .join(self.exchange.as_str())
            .join(self.market.as_str())
    }

    pub fn dataset_s3_prefix(&self) -> String {
        format!(
            "{}/{}/{}",
            self.s3_prefix.trim_matches('/'),
            self.exchange.as_str(),
            self.market.as_str()
        )
    }

    fn instrument_url(&self) -> String {
        let base = self.rest_url();
        match (self.exchange, self.market) {
            (Exchange::Binance, Market::Spot) => {
                format!("{}/api/v3/exchangeInfo", base.trim_end_matches('/'))
            }
            (Exchange::Binance, Market::Usdm) => {
                format!("{}/fapi/v1/exchangeInfo", base.trim_end_matches('/'))
            }
            (Exchange::Binance, Market::Coinm) => {
                format!("{}/dapi/v1/exchangeInfo", base.trim_end_matches('/'))
            }
            (Exchange::Okx, _) => format!(
                "{}/api/v5/public/instruments?instType=SPOT",
                base.trim_end_matches('/')
            ),
            (Exchange::Bybit, _) => format!(
                "{}/v5/market/instruments-info?category=spot",
                base.trim_end_matches('/')
            ),
        }
    }

    pub fn shards(&self, symbols: &[String], streams: &[String], count: usize) -> Vec<Vec<String>> {
        let mut shards = vec![Vec::new(); count];
        for (index, symbol) in symbols.iter().enumerate() {
            for stream in streams {
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
