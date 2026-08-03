use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    config::Market, futures_parser, parser, runtime::Metrics, stream_writer::StreamWriter,
};

pub async fn run_shard(
    id: usize,
    group: &'static str,
    base_url: String,
    streams: Vec<String>,
    writer: StreamWriter,
    metrics: Arc<Metrics>,
    market: Market,
) {
    let mut delay = Duration::from_secs(1);
    let mut connected_once = false;
    loop {
        if connected_once {
            metrics
                .reconnects
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        match collect(&base_url, &streams, id, group, &writer, &metrics, market).await {
            Ok(()) => tracing::warn!(shard = id, group, "websocket closed"),
            Err(error) => tracing::warn!(shard = id, group, %error, "websocket disconnected"),
        }
        connected_once = true;
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

async fn collect(
    url: &str,
    streams: &[String],
    id: usize,
    group: &'static str,
    writer: &StreamWriter,
    metrics: &Metrics,
    market: Market,
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
            Message::Text(text) => text.as_bytes().to_vec(),
            Message::Binary(bytes) => bytes.to_vec(),
            Message::Close(_) => return Ok(()),
            _ => continue,
        };
        metrics
            .received_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        metrics.last_message_unix_ms.store(
            Utc::now().timestamp_millis().max(0) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        let received = Utc::now();
        let parsed = if market == Market::Spot {
            parser::parse(&payload, received, "binance_spot_websocket")
        } else {
            futures_parser::parse(
                &payload,
                received,
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
                writer.records(records).await?;
            }
            Ok(_) => {
                if let Ok(control) = serde_json::from_slice::<serde_json::Value>(&payload)
                    && (control.get("result").is_some() || control.get("code").is_some())
                {
                    tracing::info!(shard = id, response = %control, "websocket control response");
                }
            }
            Err(error) => {
                metrics
                    .parse_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(shard = id, %error, "invalid websocket event");
            }
        }
    }
}
