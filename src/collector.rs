use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    config::Market, futures_parser, model::Record, parser, runtime::Metrics, writer::Writer,
};

const METRICS_FLUSH_MESSAGES: u64 = 256;
const METRICS_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

struct ReceivedMetrics<'a> {
    metrics: &'a Metrics,
    pending: u64,
    latest_unix_ms: u64,
    last_flush: std::time::Instant,
}

impl<'a> ReceivedMetrics<'a> {
    fn new(metrics: &'a Metrics) -> Self {
        Self {
            metrics,
            pending: 0,
            latest_unix_ms: 0,
            last_flush: std::time::Instant::now(),
        }
    }

    fn record(&mut self, received: chrono::DateTime<Utc>) {
        self.pending += 1;
        self.latest_unix_ms = self
            .latest_unix_ms
            .max(received.timestamp_millis().max(0) as u64);
        if self.pending >= METRICS_FLUSH_MESSAGES
            || self.last_flush.elapsed() >= METRICS_FLUSH_INTERVAL
        {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.pending == 0 {
            return;
        }
        self.metrics
            .received_messages
            .fetch_add(self.pending, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .last_message_unix_ms
            .fetch_max(self.latest_unix_ms, std::sync::atomic::Ordering::Relaxed);
        self.pending = 0;
        self.latest_unix_ms = 0;
        self.last_flush = std::time::Instant::now();
    }
}

impl Drop for ReceivedMetrics<'_> {
    fn drop(&mut self) {
        self.flush();
    }
}

pub struct Payload {
    bytes: Bytes,
    received: chrono::DateTime<Utc>,
    shard_id: usize,
}

pub fn start_processor(
    writer: Writer,
    metrics: Arc<Metrics>,
    market: Market,
) -> mpsc::Sender<Payload> {
    let (sender, receiver) = mpsc::channel(20_000);
    tokio::spawn(process_payloads(receiver, writer, metrics, market));
    sender
}

pub async fn run_shard(
    id: usize,
    group: &'static str,
    base_url: String,
    streams: Vec<String>,
    payload_sender: mpsc::Sender<Payload>,
    metrics: Arc<Metrics>,
) {
    let mut delay = Duration::from_secs(1);
    let mut connected_once = false;
    loop {
        if connected_once {
            metrics
                .reconnects
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let started = std::time::Instant::now();
        match collect(&base_url, &streams, id, group, &payload_sender, &metrics).await {
            Ok(()) => tracing::warn!(shard = id, group, "websocket closed"),
            Err(error) => tracing::warn!(shard = id, group, %error, "websocket disconnected"),
        }
        connected_once = true;
        if started.elapsed() >= Duration::from_secs(5 * 60) {
            delay = Duration::from_secs(1);
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

async fn collect(
    url: &str,
    streams: &[String],
    id: usize,
    group: &'static str,
    payload_sender: &mpsc::Sender<Payload>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let (socket, _) = connect_async(url)
        .await
        .context("connect Binance websocket")?;
    let (mut outgoing, mut incoming) = socket.split();
    let request = serde_json::json!({"method": "SUBSCRIBE", "params": streams, "id": 1});
    outgoing
        .send(Message::Text(request.to_string().into()))
        .await
        .context("subscribe Binance streams")?;
    tracing::info!(
        shard = id,
        group,
        streams = streams.len(),
        "websocket connected"
    );
    let mut received_metrics = ReceivedMetrics::new(metrics);
    let rotation = tokio::time::sleep(Duration::from_secs(23 * 60 * 60 + 50 * 60));
    tokio::pin!(rotation);
    loop {
        let message = tokio::select! {
            _ = &mut rotation => bail!("scheduled websocket rotation"),
            message = incoming.next() => message.context("websocket stream ended")??,
        };
        let payload = match message {
            Message::Text(text) => <_ as AsRef<Bytes>>::as_ref(&text).clone(),
            Message::Binary(bytes) => bytes,
            Message::Ping(payload) => {
                outgoing
                    .send(Message::Pong(payload))
                    .await
                    .context("reply websocket pong")?;
                continue;
            }
            Message::Close(_) => return Ok(()),
            _ => continue,
        };
        let received = Utc::now();
        received_metrics.record(received);
        payload_sender
            .send(Payload {
                bytes: payload,
                received,
                shard_id: id,
            })
            .await
            .context("websocket processing pipeline stopped")?;
    }
}

async fn process_payloads(
    mut receiver: mpsc::Receiver<Payload>,
    writer: Writer,
    metrics: Arc<Metrics>,
    market: Market,
) -> Result<()> {
    const PAYLOAD_BATCH_SIZE: usize = 512;
    const COALESCE_INTERVAL: Duration = Duration::from_millis(5);

    let mut payloads = Vec::with_capacity(PAYLOAD_BATCH_SIZE);
    loop {
        let Some(payload) = receiver.recv().await else {
            return Ok(());
        };
        payloads.push(payload);
        tokio::time::sleep(COALESCE_INTERVAL).await;
        while payloads.len() < PAYLOAD_BATCH_SIZE {
            let Ok(payload) = receiver.try_recv() else {
                break;
            };
            payloads.push(payload);
        }
        let mut record_batch = Vec::with_capacity(payloads.len());
        let mut parsed_records = 0_u64;
        for payload in payloads.drain(..) {
            let initial_len = record_batch.len();
            let parsed = if market == Market::Spot {
                parser::parse_into(
                    &payload.bytes,
                    payload.received,
                    "binance_spot_websocket",
                    &mut record_batch,
                )
            } else {
                futures_parser::parse_into(
                    &payload.bytes,
                    payload.received,
                    if market == Market::Usdm {
                        "binance_usdm_websocket"
                    } else {
                        "binance_coinm_websocket"
                    },
                    &mut record_batch,
                )
            };
            match parsed {
                Ok(()) if record_batch.len() > initial_len => {
                    parsed_records += (record_batch.len() - initial_len) as u64;
                }
                Ok(()) => {
                    if let Ok(control) = serde_json::from_slice::<serde_json::Value>(&payload.bytes)
                        && (control.get("result").is_some() || control.get("code").is_some())
                    {
                        tracing::info!(shard = payload.shard_id, response = %control, "websocket control response");
                    }
                }
                Err(error) => {
                    record_batch.truncate(initial_len);
                    metrics
                        .parse_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(shard = payload.shard_id, %error, "invalid websocket event");
                }
            }
        }
        metrics
            .parsed_records
            .fetch_add(parsed_records, std::sync::atomic::Ordering::Relaxed);
        if !record_batch.is_empty() {
            write_records(&writer, market, record_batch).await?;
        }
    }
}

async fn write_records(
    writer: &Writer,
    fallback_market: Market,
    records: Vec<Record>,
) -> Result<()> {
    let needs_routing = records.iter().any(|record| {
        record
            .target_market
            .is_some_and(|target| target != fallback_market)
    });
    if !needs_routing {
        return writer.records(fallback_market, records).await;
    }

    let mut spot = Vec::new();
    let mut usdm = Vec::new();
    let mut coinm = Vec::new();
    for record in records {
        match record.target_market.unwrap_or(fallback_market) {
            Market::Spot => spot.push(record),
            Market::Usdm => usdm.push(record),
            Market::Coinm => coinm.push(record),
        }
    }
    for (market, records) in [
        (Market::Spot, spot),
        (Market::Usdm, usdm),
        (Market::Coinm, coinm),
    ] {
        if !records.is_empty() {
            writer.records(market, records).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use chrono::TimeZone;

    use super::*;

    #[test]
    fn received_metrics_should_flush_pending_values_when_dropped() {
        let metrics = Metrics::default();
        {
            let mut received = ReceivedMetrics::new(&metrics);
            received.record(Utc.timestamp_millis_opt(123).single().unwrap_or_default());
        }

        assert_eq!(metrics.snapshot().received_messages, 1);
    }

    #[test]
    fn received_metrics_should_not_move_latest_message_backwards() {
        let metrics = Metrics::default();
        metrics.last_message_unix_ms.store(456, Ordering::Relaxed);
        {
            let mut received = ReceivedMetrics::new(&metrics);
            received.record(Utc.timestamp_millis_opt(123).single().unwrap_or_default());
        }

        assert_eq!(metrics.snapshot().last_message_unix_ms, 456);
    }
}
