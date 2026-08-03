use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;

use crate::{parser, writer::Writer};

#[derive(Clone)]
pub struct Backfiller {
    client: reqwest::Client,
    rest_url: String,
    symbols: Vec<String>,
    writer: Writer,
}

impl Backfiller {
    pub fn new(rest_url: String, symbols: Vec<String>, writer: Writer) -> Self {
        Self {
            client: reqwest::Client::new(),
            rest_url,
            symbols,
            writer,
        }
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(600));
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(error) = self.run().await {
                    tracing::error!(%error, "aggregate trade backfill failed");
                }
            }
        });
    }

    pub async fn run(&self) -> Result<usize> {
        let mut inserted = 0;
        for symbol in &self.symbols {
            inserted += self.backfill_symbol(symbol).await?;
        }
        if inserted > 0 {
            tracing::info!(inserted, "aggregate trade gaps backfilled");
        }
        Ok(inserted)
    }

    async fn backfill_symbol(&self, symbol: &str) -> Result<usize> {
        let escaped = symbol.replace('\'', "''");
        let sql = format!(
            "WITH ids AS (SELECT DISTINCT aggregate_trade_id id FROM aggregate_trades WHERE symbol='{escaped}' AND event_time >= now()-INTERVAL '9 hours'), gaps AS (SELECT lag(id) OVER (ORDER BY id) previous_id,id FROM ids) SELECT previous_id+1 first_id,id-1 last_id FROM gaps WHERE id>previous_id+1 ORDER BY id LIMIT 100"
        );
        let response = self.writer.query(sql).await?;
        let gaps = response
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut inserted = 0;
        for gap in gaps {
            let mut next = gap
                .get("first_id")
                .and_then(Value::as_u64)
                .context("missing first_id")?;
            let last = gap
                .get("last_id")
                .and_then(Value::as_u64)
                .context("missing last_id")?;
            while next <= last {
                let limit = (last - next + 1).min(1_000);
                let query = [
                    ("symbol", symbol.to_owned()),
                    ("fromId", next.to_string()),
                    ("limit", limit.to_string()),
                ];
                let values: Vec<Value> = self
                    .client
                    .get(format!(
                        "{}/api/v3/aggTrades",
                        self.rest_url.trim_end_matches('/')
                    ))
                    .query(&query)
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
                    .map(|value| parser::parse_rest_aggregate_trade(symbol, value, received))
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
        }
        Ok(inserted)
    }
}
