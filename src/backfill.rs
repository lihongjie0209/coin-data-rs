use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;

use crate::{
    config::Market, futures_parser, parser, storage::Storage, stream_writer::StreamWriter,
};

#[derive(Clone)]
pub struct Backfiller {
    client: reqwest::Client,
    rest_url: String,
    symbols: Vec<String>,
    writer: StreamWriter,
    market: Market,
    directory: PathBuf,
}

impl Backfiller {
    pub fn new(
        rest_url: String,
        symbols: Vec<String>,
        writer: StreamWriter,
        market: Market,
        directory: PathBuf,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            rest_url,
            symbols,
            writer,
            market,
            directory,
        }
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(until_next_check()).await;
                if let Err(error) = self.run().await {
                    tracing::error!(%error, "aggregate trade Parquet audit failed");
                }
            }
        });
    }

    pub async fn run(&self) -> Result<usize> {
        let mut inserted = 0;
        for (symbol, first, last) in self.gaps().await? {
            inserted += self.backfill_gap(&symbol, first, last).await?;
        }
        if inserted > 0 {
            tracing::info!(
                inserted,
                market = self.market.as_str(),
                "aggregate trade gaps backfilled"
            );
        }
        Ok(inserted)
    }

    async fn gaps(&self) -> Result<Vec<(String, u64, u64)>> {
        let table = if self.market == Market::Spot {
            "aggregate_trades"
        } else {
            "futures_aggregate_trades"
        };
        if !self.directory.is_dir()
            || !self
                .directory
                .read_dir()?
                .flatten()
                .any(|entry| entry.path().join(table).is_dir())
        {
            return Ok(Vec::new());
        }
        let sql = format!(
            "WITH ids AS (SELECT DISTINCT symbol,aggregate_trade_id id FROM {table} WHERE event_time >= now()-INTERVAL '9 hours'), gaps AS (SELECT symbol,lag(id) OVER (PARTITION BY symbol ORDER BY id) previous_id,id FROM ids) SELECT symbol,previous_id+1 first_id,id-1 last_id FROM gaps WHERE id>previous_id+1 ORDER BY symbol,id LIMIT 10000"
        );
        let directory = self.directory.clone();
        let response =
            tokio::task::spawn_blocking(move || Storage::query_parquet(&directory, &sql)).await??;
        Ok(response
            .get("rows")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|gap| {
                Some((
                    gap.get("symbol")?.as_str()?.to_owned(),
                    gap.get("first_id")?.as_u64()?,
                    gap.get("last_id")?.as_u64()?,
                ))
            })
            .filter(|(symbol, _, _)| self.symbols.contains(symbol))
            .collect())
    }

    async fn backfill_gap(&self, symbol: &str, first: u64, last: u64) -> Result<usize> {
        let mut inserted = 0;
        let mut next = first;
        while next <= last {
            let limit = (last - next + 1).min(1_000);
            let endpoint = match self.market {
                Market::Spot => "/api/v3/aggTrades",
                Market::Usdm => "/fapi/v1/aggTrades",
                Market::Coinm => "/dapi/v1/aggTrades",
            };
            let values: Vec<Value> = self
                .client
                .get(format!("{}{endpoint}", self.rest_url.trim_end_matches('/')))
                .query(&[
                    ("symbol", symbol.to_owned()),
                    ("fromId", next.to_string()),
                    ("limit", limit.to_string()),
                ])
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            if values.is_empty() {
                break;
            }
            let received = Utc::now();
            let records = values
                .iter()
                .map(|value| match self.market {
                    Market::Spot => parser::parse_rest_aggregate_trade(symbol, value, received),
                    Market::Usdm => futures_parser::parse_rest_aggregate_trade(
                        symbol,
                        value,
                        received,
                        "binance_usdm_rest_backfill",
                    ),
                    Market::Coinm => futures_parser::parse_rest_aggregate_trade(
                        symbol,
                        value,
                        received,
                        "binance_coinm_rest_backfill",
                    ),
                })
                .collect::<Vec<_>>();
            next = values
                .last()
                .and_then(|value| value.get("a"))
                .and_then(Value::as_u64)
                .context("missing aggregate trade id")?
                + 1;
            inserted += records.len();
            self.writer.records(records).await?;
        }
        Ok(inserted)
    }
}

fn until_next_check() -> Duration {
    let current = Utc::now().timestamp();
    let next_boundary = current - current.rem_euclid(600) + 600;
    Duration::from_secs(u64::try_from((next_boundary - 60 - current).max(1)).unwrap_or(1))
}
