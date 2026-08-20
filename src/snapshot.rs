use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use chrono::{DateTime, Utc};
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use serde_json::Value;

use crate::{
    config::{Config, Market},
    rate_limit,
};

const REST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_WEIGHT_FRACTION: f64 = 0.80;

#[derive(Debug, Clone)]
struct SnapshotJob {
    symbol: String,
    next_run: Instant,
}

#[derive(Debug, Serialize)]
struct SnapshotEnvelope {
    exchange: &'static str,
    market: &'static str,
    symbol: String,
    captured_at: DateTime<Utc>,
    requested_at: DateTime<Utc>,
    last_update_id: u64,
    limit: u16,
    payload: Value,
}

#[derive(Debug, Clone, Copy)]
struct SnapshotResponse {
    used_weight: Option<u64>,
}

#[derive(Debug)]
struct WeightedLimiter {
    capacity: f64,
    tokens: f64,
    refill_per_second: f64,
    last_refill: Instant,
    paused_until: Option<Instant>,
}

impl WeightedLimiter {
    fn new(weight_per_minute: u64) -> Self {
        let budget = weight_per_minute as f64 * DEFAULT_WEIGHT_FRACTION;
        Self {
            capacity: budget,
            tokens: budget,
            refill_per_second: budget / 60.0,
            last_refill: Instant::now(),
            paused_until: None,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_per_second).min(self.capacity);
        self.last_refill = now;
    }

    fn wait_for(&mut self, weight: u64) -> Duration {
        let now = Instant::now();
        self.refill(now);
        if let Some(until) = self.paused_until {
            if until > now {
                return until.duration_since(now);
            }
            self.paused_until = None;
        }
        let needed = weight as f64 - self.tokens;
        if needed <= 0.0 {
            self.tokens -= weight as f64;
            Duration::ZERO
        } else {
            Duration::from_secs_f64(needed / self.refill_per_second)
        }
    }

    fn pause(&mut self, duration: Duration) {
        self.paused_until = Some(Instant::now() + duration);
    }

    fn observe_used_weight(&mut self, used_weight: u64) {
        let remaining = (self.capacity - used_weight as f64).max(0.0);
        self.tokens = self.tokens.min(remaining);
    }
}

#[derive(Clone)]
pub struct SnapshotSyncer {
    config: Arc<Config>,
    market: Market,
    symbols: Arc<Vec<String>>,
    client: reqwest::Client,
    s3: S3Client,
}

impl SnapshotSyncer {
    pub async fn new(config: Config, market: Market, symbols: Vec<String>) -> Self {
        let sdk = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.aws_region.clone()))
            .load()
            .await;
        let mut s3 = aws_sdk_s3::config::Builder::from(&sdk);
        if let Some(endpoint) = &config.s3_endpoint {
            s3 = s3.endpoint_url(endpoint).force_path_style(false);
        }
        let timeout = reqwest::Client::builder()
            .connect_timeout(REST_TIMEOUT)
            .timeout(REST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            config: Arc::new(config),
            market,
            symbols: Arc::new(symbols),
            client: timeout,
            s3: S3Client::from_conf(s3.build()),
        }
    }

    pub fn spawn(self) {
        if !self.config.snapshot_enabled {
            tracing::info!(market = self.market.as_str(), "REST snapshot sync disabled");
            return;
        }
        tokio::spawn(async move {
            if let Err(error) = self.run().await {
                tracing::error!(market = self.market.as_str(), error = %format!("{error:#}"), "snapshot scheduler stopped");
            }
        });
    }

    async fn run(&self) -> Result<()> {
        let mut limiter = WeightedLimiter::new(self.weight_limit());
        let mut jobs = self.initial_jobs();
        let mut active = FuturesUnordered::new();
        let mut successes = 0u64;
        let mut reported_batches = 0u64;
        loop {
            jobs.make_contiguous()
                .sort_unstable_by_key(|job| job.next_run);
            let mut limiter_wait = Duration::ZERO;
            while active.len() < self.config.snapshot_concurrency {
                let Some(job) = jobs.front() else { break };
                if job.next_run > Instant::now() {
                    break;
                }
                let Some(job) = jobs.pop_front() else { break };
                let weight = self.request_weight();
                let wait = limiter.wait_for(weight);
                if !wait.is_zero() {
                    jobs.push_front(job);
                    limiter_wait = wait;
                    break;
                }
                let this = self.clone();
                let symbol = job.symbol.clone();
                active.push(async move { (job, this.fetch_and_store(&symbol).await) });
            }
            if let Some((mut job, result)) = active.next().await {
                match result {
                    Ok(response) => {
                        if let Some(used_weight) = response.used_weight {
                            limiter.observe_used_weight(used_weight);
                        }
                        successes = successes.saturating_add(1);
                        job.next_run = Instant::now()
                            + Duration::from_secs(self.config.snapshot_interval_seconds);
                        jobs.push_back(job);
                    }
                    Err(error) => {
                        let retry = if rate_limit::remaining_seconds() > 0 {
                            rate_limit::remaining_seconds()
                        } else {
                            30
                        };
                        tracing::warn!(market = self.market.as_str(), symbol = %job.symbol, retry_seconds = retry, error = %format!("{error:#}"), "snapshot request failed");
                        job.next_run = Instant::now() + Duration::from_secs(retry.min(300));
                        jobs.push_back(job);
                        limiter.pause(Duration::from_secs(retry.min(60)));
                    }
                }
                if successes / 100 > reported_batches {
                    reported_batches = successes / 100;
                    tracing::info!(
                        market = self.market.as_str(),
                        snapshots = successes,
                        queued = jobs.len(),
                        "snapshot sync progress"
                    );
                }
            } else if let Some(job) = jobs.front() {
                let due_wait = job.next_run.saturating_duration_since(Instant::now());
                let wait = if limiter_wait.is_zero() {
                    due_wait
                } else if due_wait.is_zero() {
                    limiter_wait
                } else {
                    due_wait.min(limiter_wait)
                };
                tokio::time::sleep(wait.min(Duration::from_secs(5))).await;
            }
        }
    }

    fn initial_jobs(&self) -> VecDeque<SnapshotJob> {
        let interval = Duration::from_secs(self.config.snapshot_interval_seconds);
        let count = self.symbols.len().max(1) as u32;
        let now = Instant::now();
        self.symbols
            .iter()
            .enumerate()
            .map(|(index, symbol)| SnapshotJob {
                symbol: symbol.clone(),
                next_run: now + interval.mul_f64(index as f64 / count as f64),
            })
            .collect()
    }

    async fn fetch_and_store(&self, symbol: &str) -> Result<SnapshotResponse> {
        let endpoint = format!(
            "{}/{}",
            self.config.rest_url().trim_end_matches('/'),
            self.depth_endpoint()
        );
        let requested_at = Utc::now();
        let response = self
            .client
            .get(endpoint)
            .query(&[
                ("symbol", symbol),
                ("limit", &self.config.snapshot_depth_limit.to_string()),
            ])
            .send()
            .await
            .context("request depth snapshot")?;
        let blocked = rate_limit::observe_response(&response);
        if blocked {
            bail!("Binance REST rate limit response: {}", response.status());
        }
        let response = response
            .error_for_status()
            .context("depth snapshot rejected")?;
        let used_weight = response.headers().iter().find_map(|(name, value)| {
            (name.as_str().eq_ignore_ascii_case("x-mbx-used-weight-1m"))
                .then(|| value.to_str().ok())
                .flatten()
                .and_then(|value| value.parse::<u64>().ok())
        });
        let captured_at = Utc::now();
        let payload = response
            .json::<Value>()
            .await
            .context("decode depth snapshot")?;
        let last_update_id = payload
            .get("lastUpdateId")
            .and_then(Value::as_u64)
            .context("depth snapshot has no lastUpdateId")?;
        let envelope = SnapshotEnvelope {
            exchange: "binance",
            market: self.market.as_str(),
            symbol: symbol.to_owned(),
            captured_at,
            requested_at,
            last_update_id,
            limit: self.config.snapshot_depth_limit,
            payload,
        };
        let encoded = serde_json::to_vec(&envelope).context("encode snapshot")?;
        let compressed =
            zstd::stream::encode_all(encoded.as_slice(), 3).context("compress snapshot")?;
        let key = format!(
            "{}/binance/{}/{}/date={}/hour={}/snapshot-{}-u{}.json.zst",
            self.config.snapshot_prefix.trim_matches('/'),
            self.market.as_str(),
            symbol.to_uppercase(),
            captured_at.format("%Y-%m-%d"),
            captured_at.format("%H"),
            captured_at.format("%Y%m%dT%H%M%S%.3fZ"),
            last_update_id,
        );
        self.s3
            .put_object()
            .bucket(&self.config.s3_bucket)
            .key(&key)
            .content_type("application/zstd")
            .body(ByteStream::from(compressed))
            .send()
            .await
            .context("upload depth snapshot")?;
        tracing::debug!(market = self.market.as_str(), symbol, %key, last_update_id, "depth snapshot uploaded");
        Ok(SnapshotResponse { used_weight })
    }

    const fn depth_endpoint(&self) -> &'static str {
        match self.market {
            Market::Spot => "api/v3/depth",
            Market::Usdm => "fapi/v1/depth",
            Market::Coinm => "dapi/v1/depth",
        }
    }

    const fn weight_limit(&self) -> u64 {
        match self.market {
            Market::Spot => 6_000,
            Market::Usdm | Market::Coinm => 2_400,
        }
    }

    fn request_weight(&self) -> u64 {
        request_weight_for(self.market, self.config.snapshot_depth_limit)
    }
}

fn request_weight_for(market: Market, limit: u16) -> u64 {
    match market {
        Market::Spot => match limit {
            1..=100 => 5,
            101..=500 => 25,
            501..=1000 => 50,
            _ => 250,
        },
        Market::Usdm | Market::Coinm => match limit {
            0..=50 => 2,
            51..=100 => 5,
            101..=500 => 10,
            _ => 20,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_depth_weight_should_follow_binance_ranges() {
        assert_eq!(request_weight_for(Market::Spot, 500), 25);
    }

    #[test]
    fn futures_depth_weight_should_use_maximum_weight_for_large_snapshot() {
        assert_eq!(request_weight_for(Market::Usdm, 1000), 20);
    }

    #[test]
    fn limiter_should_wait_when_budget_is_exhausted() {
        let mut limiter = WeightedLimiter::new(100);
        assert!(limiter.wait_for(80).is_zero());
        assert!(limiter.wait_for(80) > Duration::ZERO);
    }
}
