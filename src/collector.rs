use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{parser, runtime::Metrics, writer::Writer};

pub async fn run_shard(
    id: usize,
    base_url: String,
    streams: Vec<String>,
    writer: Writer,
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
        match collect(&base_url, &streams, id, &writer, &metrics).await {
            Ok(()) => tracing::warn!(shard = id, "websocket closed"),
            Err(error) => tracing::warn!(shard = id, %error, "websocket disconnected"),
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
    writer: &Writer,
    metrics: &Metrics,
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
    tracing::info!(shard = id, "websocket connected");
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
        match parser::parse(&payload, Utc::now(), "websocket") {
            Ok(records) if !records.is_empty() => {
                metrics
                    .parsed_records
                    .fetch_add(records.len() as u64, std::sync::atomic::Ordering::Relaxed);
                writer.records(records).await?;
            }
            Ok(_) => {}
            Err(error) => {
                metrics
                    .parse_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(shard = id, %error, "invalid websocket event");
            }
        }
    }
}
