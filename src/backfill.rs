use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::{config::Market, futures_parser, parser, writer::Writer};

const MAX_GAPS_PER_AUDIT: usize = 100;

#[derive(Clone)]
pub struct Backfiller {
    client: reqwest::Client,
    rest_url: String,
    writer: Writer,
    market: Market,
    audit_lock: Arc<Mutex<()>>,
}

impl Backfiller {
    pub fn new(rest_url: String, writer: Writer, market: Market) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            rest_url,
            writer,
            market,
            audit_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(until_next_check()).await;
                let end = Utc::now();
                let start = end - chrono::Duration::minutes(20);
                if let Err(error) = self.run_range(start, end).await {
                    tracing::error!(error = %format!("{error:#}"), "aggregate trade backfill failed");
                }
            }
        });
    }

    pub async fn run_range(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<usize> {
        let _guard = self.audit_lock.lock().await;
        let table = if self.market == Market::Spot {
            "aggregate_trades"
        } else {
            "futures_aggregate_trades"
        };
        let gaps = self
            .writer
            .aggregate_trade_gaps(table, start, end, MAX_GAPS_PER_AUDIT)
            .await
            .context("query aggregate trade gaps")?;
        let has_more = gaps.len() == MAX_GAPS_PER_AUDIT;
        let mut inserted = 0;
        for gap in gaps {
            inserted += self
                .backfill_gap(&gap.symbol, gap.first_id, gap.last_id)
                .await
                .with_context(|| {
                    format!(
                        "backfill {} IDs {}..{}",
                        gap.symbol, gap.first_id, gap.last_id
                    )
                })?;
        }
        if inserted > 0 {
            tracing::info!(
                inserted,
                market = self.market.as_str(),
                "aggregate trade gaps backfilled"
            );
        }
        if has_more {
            anyhow::bail!(
                "aggregate trade audit reached the {} gap limit; remaining gaps will be retried",
                MAX_GAPS_PER_AUDIT
            );
        }
        Ok(inserted)
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
            let values = self.fetch(symbol, next, limit, endpoint).await?;
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

    async fn fetch(
        &self,
        symbol: &str,
        from_id: u64,
        limit: u64,
        endpoint: &str,
    ) -> Result<Vec<Value>> {
        let url = format!("{}{endpoint}", self.rest_url.trim_end_matches('/'));
        let mut delay = Duration::from_millis(250);
        let mut last_error = None;
        for _ in 0..3 {
            let query = [
                ("symbol", symbol.to_owned()),
                ("fromId", from_id.to_string()),
                ("limit", limit.to_string()),
            ];
            match self.client.get(&url).query(&query).send().await {
                Ok(response) => {
                    let retry_after = response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.parse::<u64>().ok());
                    match response.error_for_status() {
                        Ok(response) => {
                            let values =
                                response.json().await.context("decode aggregate trades")?;
                            tokio::time::sleep(self.request_spacing()).await;
                            return Ok(values);
                        }
                        Err(error) => {
                            last_error = Some(error.into());
                            if let Some(seconds) = retry_after {
                                delay = Duration::from_secs(seconds.clamp(1, 60));
                            }
                        }
                    }
                }
                Err(error) => last_error = Some(error.into()),
            }
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("aggregate trade request failed")))
    }

    fn request_spacing(&self) -> Duration {
        match self.market {
            Market::Spot => Duration::from_millis(100),
            Market::Usdm | Market::Coinm => Duration::from_secs(1),
        }
    }
}

fn until_next_check() -> Duration {
    let current = Utc::now().timestamp();
    let next_boundary = current - current.rem_euclid(600) + 600;
    Duration::from_secs(u64::try_from((next_boundary - 60 - current).max(1)).unwrap_or(1))
}
