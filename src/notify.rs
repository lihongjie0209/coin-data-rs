use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::runtime::Metrics;

#[derive(Clone)]
pub struct TelegramNotifier {
    client: reqwest::Client,
    endpoint: Option<String>,
    chat_id: Option<String>,
    metrics: Arc<Metrics>,
    dataset: String,
}

pub struct ArchiveNotification<'a> {
    pub status: &'a str,
    pub hour: String,
    pub files: usize,
    pub source_files: usize,
    pub bytes: u64,
    pub elapsed_seconds: f64,
    pub error: Option<&'a anyhow::Error>,
    pub data_directory: &'a Path,
}

impl TelegramNotifier {
    pub fn from_env(metrics: Arc<Metrics>, dataset: String) -> Self {
        let token = std::env::var("TELEGRAM_BOT_TOKEN").ok();
        let chat_id = std::env::var("TELEGRAM_CHAT_ID").ok();
        let endpoint =
            token.map(|token| format!("https://api.telegram.org/bot{token}/sendMessage"));
        Self {
            client: reqwest::Client::new(),
            endpoint,
            chat_id,
            metrics,
            dataset,
        }
    }

    pub async fn send_archive_report(&self, report: ArchiveNotification<'_>) -> Result<()> {
        let (Some(endpoint), Some(chat_id)) = (&self.endpoint, &self.chat_id) else {
            return Ok(());
        };
        let runtime = self.metrics.snapshot();
        let load = std::fs::read_to_string("/proc/loadavg")
            .ok()
            .and_then(|value| value.split_whitespace().next().map(str::to_owned))
            .unwrap_or_else(|| "unknown".to_owned());
        let (memory_used, memory_total) = memory_usage().unwrap_or_default();
        let disk_total = fs2::total_space(report.data_directory).unwrap_or_default();
        let disk_available = fs2::available_space(report.data_directory).unwrap_or_default();
        let mut text = format!(
            "Parquet upload {}\ndataset: {}\nwindow: {}\nmerged files: {}\nsource parts: {}\ndata: {}\nelapsed: {:.1}s\nrecords: received={} parsed={} written={} dropped={} errors={} reconnects={}\nwriter queue: current={} peak={}\nlast message: {}\nload1: {load}\nmemory: {} / {}\ndisk: {} free / {}",
            report.status,
            self.dataset,
            report.hour,
            report.files,
            report.source_files,
            human_bytes(report.bytes),
            report.elapsed_seconds,
            runtime.received_messages,
            runtime.parsed_records,
            runtime.written_records,
            runtime.dropped_messages,
            runtime.parse_errors,
            runtime.reconnects,
            runtime.writer_queue_depth,
            runtime.writer_queue_high_watermark,
            runtime.last_message_unix_ms,
            human_bytes(memory_used),
            human_bytes(memory_total),
            human_bytes(disk_available),
            human_bytes(disk_total),
        );
        if let Some(error) = report.error {
            let detail = format!("{error:#}").chars().take(1_000).collect::<String>();
            text.push_str(&format!("\nerror: {detail}"));
        }
        self.client
            .post(endpoint)
            .json(&TelegramMessage {
                chat_id,
                text: &text,
            })
            .send()
            .await
            .context("send Telegram notification")?
            .error_for_status()
            .context("Telegram rejected notification")?;
        Ok(())
    }
}

#[derive(Serialize)]
struct TelegramMessage<'a> {
    chat_id: &'a str,
    text: &'a str,
}

fn memory_usage() -> Result<(u64, u64)> {
    let content = std::fs::read_to_string("/proc/meminfo")?;
    let mut total = None;
    let mut available = None;
    for line in content.lines() {
        let mut fields = line.split_whitespace();
        match fields.next() {
            Some("MemTotal:") => total = fields.next().and_then(|value| value.parse::<u64>().ok()),
            Some("MemAvailable:") => {
                available = fields.next().and_then(|value| value.parse::<u64>().ok())
            }
            _ => {}
        }
    }
    let total = total.context("MemTotal missing")?.saturating_mul(1_024);
    let available = available
        .context("MemAvailable missing")?
        .saturating_mul(1_024);
    Ok((total.saturating_sub(available), total))
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1_024.0 && unit < UNITS.len() - 1 {
        value /= 1_024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}
