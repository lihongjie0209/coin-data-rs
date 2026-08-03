use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result, bail};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{
    Client,
    primitives::{ByteStream, Length},
    types::{CompletedMultipartUpload, CompletedPart},
};
use chrono::{DateTime, Datelike, Timelike, Utc};
use tokio::sync::mpsc;

use crate::{
    config::Config,
    notify::{ArchiveNotification, TelegramNotifier},
    writer::{ClosedDatabase, Writer, floor_hour},
};

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
            prefix: format!(
                "{}/{}",
                config.s3_prefix.trim_matches('/'),
                config.exchange.as_str()
            ),
            min_retention_hours: config.min_retention_hours,
            max_retention_hours: config.max_retention_hours,
            min_free_disk_percent: config.min_free_disk_percent,
            notifier,
            upload_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn spawn(self: Arc<Self>, mut closed: mpsc::UnboundedReceiver<ClosedDatabase>) {
        tokio::spawn(async move {
            let mut scan = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tokio::select! {
                    item = closed.recv() => match item {
                        Some(item) => self.upload_with_retry(item).await,
                        None => return,
                    },
                    _ = scan.tick() => {
                        if let Err(error) = self.upload_pending().await {
                            tracing::warn!(error = %format!("{error:#}"), "scan hourly databases failed");
                        }
                    }
                }
            }
        });
    }

    pub async fn export(&self, hour: DateTime<Utc>, force: bool) -> Result<Vec<String>> {
        let hour = floor_hour(hour)?;
        if hour >= floor_hour(Utc::now())? {
            bail!("the active hour cannot be uploaded");
        }
        let item = ClosedDatabase {
            hour,
            path: self.writer.database_path(hour),
        };
        Ok(vec![self.upload(item, force).await?])
    }

    async fn upload_with_retry(&self, item: ClosedDatabase) {
        for attempt in 1..=4 {
            match self.upload(item.clone(), false).await {
                Ok(_) => return,
                Err(error) if attempt < 4 => {
                    tracing::warn!(error = %format!("{error:#}"), attempt, path = %item.path.display(), "DuckDB upload will retry");
                    tokio::time::sleep(std::time::Duration::from_secs(120)).await;
                }
                Err(error) => {
                    tracing::error!(error = %format!("{error:#}"), path = %item.path.display(), "DuckDB upload retries exhausted")
                }
            }
        }
    }

    async fn upload(&self, item: ClosedDatabase, force: bool) -> Result<String> {
        let _guard = self.upload_lock.lock().await;
        if !item.path.is_file() {
            bail!("hourly database does not exist: {}", item.path.display());
        }
        let marker = item.path.with_extension("duckdb.uploaded");
        if marker.exists() && !force {
            return Ok(std::fs::read_to_string(marker)?.trim().to_owned());
        }
        let started = Instant::now();
        let key = format!(
            "{}/{:04}-{:02}-{:02}/{:02}.duckdb",
            self.prefix,
            item.hour.year(),
            item.hour.month(),
            item.hour.day(),
            item.hour.hour()
        );
        let result = async {
            self.upload_object(&item.path, &key).await?;
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await?;
            std::fs::write(&marker, format!("{key}\n")).context("write upload marker")?;
            self.cleanup()?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let size = std::fs::metadata(&item.path)
            .map(|value| value.len())
            .unwrap_or_default();
        let status = if result.is_ok() { "SUCCESS" } else { "FAILED" };
        if let Err(error) = self
            .notifier
            .send_archive_report(ArchiveNotification {
                status,
                hour: item.hour.format("%Y-%m-%d %H:00 UTC").to_string(),
                files: usize::from(result.is_ok()),
                bytes: if result.is_ok() { size } else { 0 },
                elapsed_seconds: started.elapsed().as_secs_f64(),
                error: result.as_ref().err(),
                data_directory: self.writer.directory(),
            })
            .await
        {
            tracing::warn!(%error, "Telegram archive notification failed");
        }
        result?;
        Ok(key)
    }

    async fn upload_object(&self, path: &Path, key: &str) -> Result<()> {
        const PART_SIZE: u64 = 64 * 1_024 * 1_024;
        let size = std::fs::metadata(path)?.len();
        if size <= PART_SIZE {
            let body = ByteStream::from_path(path)
                .await
                .context("read hourly DuckDB")?;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body)
                .send()
                .await?;
            return Ok(());
        }

        let created = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        let upload_id = created
            .upload_id()
            .context("S3 omitted multipart upload ID")?;
        let result = async {
            let mut parts = Vec::new();
            let mut offset = 0;
            let mut part_number = 1;
            while offset < size {
                let length = PART_SIZE.min(size - offset);
                let body = ByteStream::read_from()
                    .path(path)
                    .offset(offset)
                    .length(Length::Exact(length))
                    .build()
                    .await?;
                let uploaded = self
                    .client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .body(body)
                    .send()
                    .await?;
                parts.push(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .set_e_tag(uploaded.e_tag().map(str::to_owned))
                        .build(),
                );
                offset += length;
                part_number += 1;
            }
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if result.is_err()
            && let Err(error) = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
        {
            tracing::warn!(%error, %key, "abort multipart upload failed");
        }
        result
    }

    async fn upload_pending(&self) -> Result<()> {
        let current = floor_hour(Utc::now())?;
        for entry in walk_files(self.writer.directory())? {
            if entry.extension().and_then(|v| v.to_str()) != Some("duckdb") {
                continue;
            }
            let Some(hour) = hour_from_path(&entry) else {
                continue;
            };
            if hour < current && !entry.with_extension("duckdb.uploaded").exists() {
                self.upload_with_retry(ClosedDatabase { hour, path: entry })
                    .await;
            }
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
        for path in walk_files(self.writer.directory())? {
            if path.extension().and_then(|v| v.to_str()) != Some("duckdb") {
                continue;
            }
            let Some(hour) = hour_from_path(&path) else {
                continue;
            };
            let marker = path.with_extension("duckdb.uploaded");
            if hour < cutoff && marker.exists() {
                std::fs::remove_file(&path)?;
                std::fs::remove_file(marker)?;
            }
        }
        Ok(())
    }
}

fn walk_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for day in std::fs::read_dir(root)? {
        let day = day?.path();
        if !day.is_dir() {
            continue;
        }
        for file in std::fs::read_dir(day)? {
            files.push(file?.path());
        }
    }
    Ok(files)
}

fn hour_from_path(path: &Path) -> Option<DateTime<Utc>> {
    let day = path.parent()?.file_name()?.to_str()?;
    let hour = path.file_stem()?.to_str()?;
    DateTime::parse_from_str(&format!("{day} {hour} +0000"), "%Y-%m-%d %H %z")
        .ok()
        .map(|v| v.with_timezone(&Utc))
}
