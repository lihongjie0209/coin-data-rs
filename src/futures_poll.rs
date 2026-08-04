use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use futures_util::{StreamExt, stream};

use crate::{config::Market, futures_parser, rate_limit, writer::Writer};

#[derive(Clone)]
pub struct OpenInterestPoller {
    client: reqwest::Client,
    rest_url: String,
    symbols: Vec<String>,
    market: Market,
    writer: Writer,
}

impl OpenInterestPoller {
    pub fn new(rest_url: String, symbols: Vec<String>, market: Market, writer: Writer) -> Self {
        Self {
            client: reqwest::Client::new(),
            rest_url,
            symbols,
            market,
            writer,
        }
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if let Err(error) = self.run().await {
                    tracing::warn!(%error, "open interest poll failed");
                }
            }
        });
    }

    async fn run(&self) -> Result<()> {
        let blocked = rate_limit::remaining_seconds();
        if blocked > 0 {
            tracing::warn!(
                blocked_seconds = blocked,
                "open interest poll skipped by Binance REST backoff"
            );
            return Ok(());
        }
        let endpoint = match self.market {
            Market::Usdm => "/fapi/v1/openInterest",
            Market::Coinm => "/dapi/v1/openInterest",
            Market::Spot => return Ok(()),
        };
        let source = if self.market == Market::Usdm {
            "binance_usdm_rest_poll"
        } else {
            "binance_coinm_rest_poll"
        };
        let requests = stream::iter(self.symbols.iter().cloned().map(|symbol| {
            let client = self.client.clone();
            let url = format!("{}{endpoint}", self.rest_url.trim_end_matches('/'));
            async move {
                let response = client.get(url).query(&[("symbol", &symbol)]).send().await?;
                rate_limit::observe_response(&response);
                let value = response
                    .error_for_status()?
                    .json::<serde_json::Value>()
                    .await?;
                anyhow::Ok((symbol, value))
            }
        }))
        .buffer_unordered(8);
        tokio::pin!(requests);
        let received = Utc::now();
        let mut records = Vec::with_capacity(self.symbols.len());
        while let Some(result) = requests.next().await {
            match result {
                Ok((symbol, value)) => records.push(futures_parser::parse_open_interest(
                    &symbol, &value, received, source,
                )),
                Err(error) => {
                    tracing::warn!(%error, "fetch open interest for symbol failed");
                    if rate_limit::remaining_seconds() > 0 {
                        break;
                    }
                }
            }
        }
        let count = records.len();
        self.writer.records(self.market, records).await?;
        tracing::info!(count, "open interest snapshot collected");
        Ok(())
    }
}
