use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{config::Market, futures_parser, parser, runtime::Metrics, writer::Writer};

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
        metrics
            .received_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics.last_message_unix_ms.store(
            received.timestamp_millis().max(0) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
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
        for payload in payloads.drain(..) {
            let parsed = if market == Market::Spot {
                parser::parse(&payload.bytes, payload.received, "binance_spot_websocket")
            } else {
                futures_parser::parse(
                    &payload.bytes,
                    payload.received,
                    if market == Market::Usdm {
                        "binance_usdm_websocket"
                    } else {
                        "binance_coinm_websocket"
                    },
                )
            };
            match parsed {
                Ok(records) if !records.is_empty() => {
                    metrics
                        .parsed_records
                        .fetch_add(records.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    record_batch.extend(records);
                }
                Ok(_) => {
                    if let Ok(control) = serde_json::from_slice::<serde_json::Value>(&payload.bytes)
                        && (control.get("result").is_some() || control.get("code").is_some())
                    {
                        tracing::info!(shard = payload.shard_id, response = %control, "websocket control response");
                    }
                }
                Err(error) => {
                    metrics
                        .parse_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(shard = payload.shard_id, %error, "invalid websocket event");
                }
            }
        }
        if !record_batch.is_empty() {
            writer.records(market, record_batch).await?;
        }
    }
}
