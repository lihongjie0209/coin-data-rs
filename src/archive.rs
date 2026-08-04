use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{Client, primitives::ByteStream};
use chrono::{DateTime, NaiveDateTime, Utc};
use tokio::sync::mpsc;

use crate::{
    config::Config,
    notify::{ArchiveNotification, TelegramNotifier},
    parquet_store::Segment,
    writer::{Writer, floor_hour},
};

const S3_REQUEST_TIMEOUT: Duration = Duration::from_secs(90);
const UPLOAD_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
struct UploadItem {
    path: PathBuf,
}

#[derive(Clone)]
pub struct Archiver {
    writer: Writer,
    client: Client,
    bucket: String,
    prefix: String,
    min_retention_hours: u64,
    max_retention_hours: u64,
    min_free_disk_percent: u64,
    notifier: TelegramNotifier,
    upload_lock: Arc<tokio::sync::Mutex<()>>,
}

impl Archiver {
    pub async fn new(config: &Config, writer: Writer, notifier: TelegramNotifier) -> Self {
        let sdk = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.aws_region.clone()))
            .load()
            .await;
        Self {
            writer,
            client: Client::new(&sdk),
            bucket: config.s3_bucket.clone(),
            prefix: config.s3_prefix.trim_matches('/').to_owned(),
            min_retention_hours: config.min_retention_hours,
            max_retention_hours: config.max_retention_hours,
            min_free_disk_percent: config.min_free_disk_percent,
            notifier,
            upload_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn spawn(self: Arc<Self>, mut segments: mpsc::UnboundedReceiver<Segment>) {
        tokio::spawn(async move {
            let mut scan = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    item = segments.recv() => match item {
                        Some(segment) => self.upload_with_retry(UploadItem { path: segment.path }).await,
                        None => return,
                    },
                    _ = scan.tick() => {
                        if let Err(error) = self.upload_pending().await {
                            tracing::warn!(error = %format!("{error:#}"), "scan Parquet segments failed");
                        }
                    }
                }
            }
        });
    }

    pub async fn export(&self, hour: DateTime<Utc>, force: bool) -> Result<Vec<String>> {
        let hour = floor_hour(hour)?;
        self.writer.flush().await?;
        let started = Instant::now();
        let mut keys = Vec::new();
        let mut bytes = 0u64;
        let result = async {
            for path in parquet_files(self.writer.directory())? {
                if hour_from_path(&path) != Some(hour) {
                    continue;
                }
                bytes = bytes.saturating_add(std::fs::metadata(&path)?.len());
                keys.push(self.upload(UploadItem { path }, force).await?);
            }
            if keys.is_empty() {
                bail!("no Parquet segments found for {hour}");
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let status = if result.is_ok() { "SUCCESS" } else { "FAILED" };
        if let Err(error) = self
            .notifier
            .send_archive_report(ArchiveNotification {
                status,
                hour: hour.format("%Y-%m-%d %H:00 UTC").to_string(),
                files: keys.len(),
                bytes: if result.is_ok() { bytes } else { 0 },
                elapsed_seconds: started.elapsed().as_secs_f64(),
                error: result.as_ref().err(),
                data_directory: self.writer.directory(),
            })
            .await
        {
            tracing::warn!(%error, "Telegram archive notification failed");
        }
        result?;
        Ok(keys)
    }

    async fn upload_with_retry(&self, item: UploadItem) {
        for attempt in 1..=4 {
            match self.upload(item.clone(), false).await {
                Ok(_) => return,
                Err(error) if attempt < 4 => {
                    tracing::warn!(error = %format!("{error:#}"), attempt, path = %item.path.display(), "Parquet upload will retry");
                    tokio::time::sleep(UPLOAD_RETRY_DELAY).await;
                }
                Err(error) => {
                    tracing::error!(error = %format!("{error:#}"), path = %item.path.display(), "Parquet upload retries exhausted")
                }
            }
        }
    }

    async fn upload(&self, item: UploadItem, force: bool) -> Result<String> {
        let _guard = self.upload_lock.lock().await;
        if !item.path.is_file()
            || item.path.extension().and_then(|value| value.to_str()) != Some("parquet")
        {
            bail!("Parquet segment does not exist: {}", item.path.display());
        }
        let marker = upload_marker(&item.path);
        if marker.exists() && !force {
            return Ok(std::fs::read_to_string(marker)?.trim().to_owned());
        }
        let relative = item
            .path
            .strip_prefix(self.writer.directory())
            .context("segment is outside data directory")?;
        let key = format!("{}/{}", self.prefix, relative.display());
        let body = ByteStream::from_path(&item.path)
            .await
            .context("read Parquet segment")?;
        await_s3(
            "put Parquet segment",
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(body)
                .send(),
        )
        .await?;
        std::fs::write(&marker, format!("{key}\n")).context("write upload marker")?;
        self.cleanup()?;
        tracing::info!(%key, bytes = std::fs::metadata(&item.path)?.len(), "Parquet segment uploaded");
        Ok(key)
    }

    async fn upload_pending(&self) -> Result<()> {
        for path in parquet_files(self.writer.directory())? {
            if upload_marker(&path).exists() {
                continue;
            }
            let Some(_) = hour_from_path(&path) else {
                continue;
            };
            self.upload_with_retry(UploadItem { path }).await;
        }
        Ok(())
    }

    fn cleanup(&self) -> Result<()> {
        let total = fs2::total_space(self.writer.directory())?;
        let available = fs2::available_space(self.writer.directory())?;
        let free = available
            .saturating_mul(100)
            .checked_div(total)
            .unwrap_or(100);
        let retention = if free < self.min_free_disk_percent {
            self.min_retention_hours
        } else {
            self.max_retention_hours
        };
        let cutoff = floor_hour(Utc::now())? - chrono::Duration::hours(i64::try_from(retention)?);
        for path in parquet_files(self.writer.directory())? {
            let Some(hour) = hour_from_path(&path) else {
                continue;
            };
            let marker = upload_marker(&path);
            if hour <= cutoff && marker.exists() {
                std::fs::remove_file(&path)?;
                std::fs::remove_file(marker)?;
            }
        }
        Ok(())
    }
}

async fn await_s3<T, E>(
    label: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T>
where
    E: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(S3_REQUEST_TIMEOUT, future)
        .await
        .with_context(|| format!("S3 {label} timed out after 90 seconds"))?
        .with_context(|| format!("S3 {label} failed"))
}

fn parquet_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        if !directory.exists() {
            continue;
        }
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("parquet") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn upload_marker(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".uploaded");
    PathBuf::from(value)
}

fn hour_from_path(path: &Path) -> Option<DateTime<Utc>> {
    let hour = path.parent()?.file_name()?.to_str()?;
    let day = path.parent()?.parent()?.file_name()?.to_str()?;
    NaiveDateTime::parse_from_str(&format!("{day} {hour}:00:00"), "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|value| value.and_utc())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_segment_hour() {
        let parsed = hour_from_path(Path::new(
            "/data/binance/spot/trades/2026-08-03/12/segment-1.parquet",
        ));
        assert_eq!(
            parsed.map(|value| value.to_rfc3339()),
            Some("2026-08-03T12:00:00+00:00".to_owned())
        );
    }
}
