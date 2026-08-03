use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{Client, primitives::ByteStream};
use chrono::{DateTime, Timelike, Utc};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use tokio::sync::{mpsc, oneshot};

use crate::notify::{ArchiveNotification, TelegramNotifier};

#[derive(Debug, serde::Serialize)]
pub struct UploadResult {
    pub source_files: usize,
    pub merged_files: usize,
    pub uploaded_bytes: u64,
}

pub struct UploaderConfig {
    pub directory: PathBuf,
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub min_retention_hours: u64,
    pub max_retention_hours: u64,
    pub min_free_disk_percent: u64,
    pub notifier: TelegramNotifier,
}

#[derive(Clone)]
pub struct Uploader {
    sender: mpsc::Sender<Command>,
}

enum Command {
    File(PathBuf),
    Scan {
        window: Option<DateTime<Utc>>,
        force: bool,
        response: oneshot::Sender<Result<UploadResult>>,
    },
}

impl Uploader {
    pub async fn start(config: UploaderConfig) -> Self {
        let sdk = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region))
            .load()
            .await;
        let (sender, receiver) = mpsc::channel(10_000);
        tokio::spawn(
            Worker {
                client: Client::new(&sdk),
                directory: config.directory,
                bucket: config.bucket,
                prefix: config.prefix,
                min_retention_hours: config.min_retention_hours,
                max_retention_hours: config.max_retention_hours,
                min_free_disk_percent: config.min_free_disk_percent,
                notifier: config.notifier,
            }
            .run(receiver),
        );
        let periodic = sender.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(until_next_half_hour()).await;
                let (response, _) = oneshot::channel();
                if periodic
                    .send(Command::Scan {
                        window: None,
                        force: false,
                        response,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Self { sender }
    }

    pub fn blocking_file(&self, path: PathBuf) -> Result<()> {
        self.sender.blocking_send(Command::File(path))?;
        Ok(())
    }

    pub async fn scan(&self) -> Result<UploadResult> {
        self.scan_window(None, false).await
    }

    pub async fn scan_window(
        &self,
        window: Option<DateTime<Utc>>,
        force: bool,
    ) -> Result<UploadResult> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Scan {
                window,
                force,
                response: sender,
            })
            .await?;
        receiver.await.context("uploader dropped scan response")?
    }
}

struct Worker {
    client: Client,
    directory: PathBuf,
    bucket: String,
    prefix: String,
    min_retention_hours: u64,
    max_retention_hours: u64,
    min_free_disk_percent: u64,
    notifier: TelegramNotifier,
}

impl Worker {
    async fn run(self, mut receiver: mpsc::Receiver<Command>) {
        while let Some(command) = receiver.recv().await {
            match command {
                Command::File(path) => {
                    tracing::trace!(path = %path.display(), "Parquet part queued for merge");
                }
                Command::Scan {
                    window,
                    force,
                    response,
                } => {
                    let _ = response.send(self.scan(window, force).await);
                }
            }
        }
    }

    async fn scan(&self, requested: Option<DateTime<Utc>>, force: bool) -> Result<UploadResult> {
        let started = Instant::now();
        let directory = self.directory.clone();
        let files = tokio::task::spawn_blocking(move || pending_files(&directory)).await??;
        let mut groups: HashMap<(PathBuf, DateTime<Utc>), Vec<PathBuf>> = HashMap::new();
        for file in files {
            let modified: DateTime<Utc> = std::fs::metadata(&file)?.modified()?.into();
            let window = modified
                .with_minute(if modified.minute() < 30 { 0 } else { 30 })
                .and_then(|value| value.with_second(0))
                .and_then(|value| value.with_nanosecond(0))
                .context("invalid upload window")?;
            if requested.is_some_and(|value| half_hour(value) != window) {
                continue;
            }
            if !force && window + chrono::Duration::minutes(30) > Utc::now() {
                continue;
            }
            let parent = file
                .parent()
                .context("Parquet part has no parent")?
                .to_path_buf();
            groups.entry((parent, window)).or_default().push(file);
        }
        let mut result = UploadResult {
            source_files: 0,
            merged_files: 0,
            uploaded_bytes: 0,
        };
        let mut first_error = None;
        for ((parent, window), mut files) in groups {
            files.sort_unstable();
            match self.merge_upload(&parent, window, &files).await {
                Ok(bytes) => {
                    result.source_files += files.len();
                    result.merged_files += 1;
                    result.uploaded_bytes += bytes;
                }
                Err(error) => {
                    tracing::warn!(parts = files.len(), %error, "merge/upload failed");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        self.cleanup_uploaded()?;
        let status = if first_error.is_some() {
            "FAILED"
        } else {
            "SUCCESS"
        };
        let hour = requested.unwrap_or_else(Utc::now).to_rfc3339();
        if let Err(error) = self
            .notifier
            .send_archive_report(ArchiveNotification {
                status,
                hour,
                files: result.merged_files,
                source_files: result.source_files,
                bytes: result.uploaded_bytes,
                elapsed_seconds: started.elapsed().as_secs_f64(),
                error: first_error.as_ref(),
                data_directory: &self.directory,
            })
            .await
        {
            tracing::warn!(%error, "Telegram upload notification failed");
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(result)
    }

    async fn merge_upload(
        &self,
        parent: &std::path::Path,
        window: DateTime<Utc>,
        files: &[PathBuf],
    ) -> Result<u64> {
        let staging = self.directory.join(".upload-staging");
        tokio::fs::create_dir_all(&staging).await?;
        let merged = staging.join(format!(
            "{}-{}-{}.parquet",
            window.format("%Y%m%dT%H%M"),
            std::process::id(),
            files.len()
        ));
        let source = files.to_vec();
        let target = merged.clone();
        tokio::task::spawn_blocking(move || merge_parts(&source, &target)).await??;
        let relative = parent
            .strip_prefix(&self.directory)
            .context("Parquet partition outside dataset directory")?;
        let key = format!(
            "{}/{}/data-{}.parquet",
            self.prefix.trim_matches('/'),
            relative.to_string_lossy(),
            window.format("%M")
        );
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from_path(&merged).await?)
            .send()
            .await?;
        let bytes = tokio::fs::metadata(&merged).await?.len();
        self.client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;
        for file in files {
            tokio::fs::write(uploaded_marker(file), b"").await?;
        }
        tokio::fs::remove_file(merged).await?;
        Ok(bytes)
    }

    fn cleanup_uploaded(&self) -> Result<()> {
        let total = fs2::total_space(&self.directory)?;
        let available = fs2::available_space(&self.directory)?;
        let free_percent = available
            .saturating_mul(100)
            .checked_div(total)
            .unwrap_or(100);
        let retention = if free_percent < self.min_free_disk_percent {
            self.min_retention_hours
        } else {
            self.max_retention_hours
        };
        let cutoff = std::time::SystemTime::now()
            .checked_sub(Duration::from_secs(retention.saturating_mul(3_600)))
            .context("invalid retention cutoff")?;
        for marker in uploaded_markers(&self.directory)? {
            let part = marker.with_extension("");
            if std::fs::metadata(&part)
                .and_then(|metadata| metadata.modified())
                .is_ok_and(|modified| modified < cutoff)
            {
                let _ = std::fs::remove_file(&part);
                let _ = std::fs::remove_file(&marker);
            }
        }
        Ok(())
    }
}

fn merge_parts(files: &[PathBuf], target: &std::path::Path) -> Result<()> {
    let first = files.first().context("no Parquet parts to merge")?;
    let first_reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(first)?)?;
    let schema = first_reader.schema().clone();
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(16_384))
        .build();
    let temporary = target.with_extension("parquet.tmp");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&temporary)?, schema, Some(properties))?;
    for file in files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file)?)?
            .with_batch_size(8_192)
            .build()?;
        for batch in reader {
            writer.write(&batch?)?;
        }
    }
    writer.close()?;
    std::fs::rename(temporary, target)?;
    Ok(())
}

fn pending_files(directory: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut pending = Vec::new();
    let mut directories = vec![directory.to_path_buf()];
    while let Some(directory) = directories.pop() {
        if !directory.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir()
                && !path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with('.'))
            {
                directories.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("parquet")
                && !uploaded_marker(&path).is_file()
            {
                pending.push(path);
            }
        }
    }
    pending.sort_unstable();
    Ok(pending)
}

fn uploaded_marker(path: &std::path::Path) -> PathBuf {
    path.with_extension("parquet.uploaded")
}

fn half_hour(value: DateTime<Utc>) -> DateTime<Utc> {
    value
        .with_minute(if value.minute() < 30 { 0 } else { 30 })
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .unwrap_or(value)
}

fn uploaded_markers(directory: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut markers = Vec::new();
    let mut directories = vec![directory.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir()
                && !path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with('.'))
            {
                directories.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("uploaded") {
                markers.push(path);
            }
        }
    }
    Ok(markers)
}

fn until_next_half_hour() -> Duration {
    let now = Utc::now();
    let elapsed = u64::from(now.minute() % 30) * 60 + u64::from(now.second());
    Duration::from_secs(30 * 60 - elapsed + 62)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use tempfile::tempdir;

    use super::*;
    use crate::{parquet_sink, parser};

    #[test]
    fn merges_parts_with_bounded_record_batches() -> Result<()> {
        let directory = tempdir()?;
        let received = Utc
            .with_ymd_and_hms(2026, 8, 3, 9, 1, 0)
            .single()
            .context("invalid test time")?;
        let payload = br#"{"e":"aggTrade","E":1785747660000,"s":"BTCUSDT","a":1,"p":"1","q":"2","f":1,"l":1,"T":1785747660000,"m":false,"M":true}"#;
        let records = parser::parse(payload, received, "test")?;
        let partition = parquet_sink::partition(&records[0])?;
        let first = parquet_sink::write_part(directory.path(), &partition, 1, &records)?;
        let second = parquet_sink::write_part(directory.path(), &partition, 2, &records)?;
        let merged = directory.path().join("merged.parquet");
        merge_parts(&[first, second], &merged)?;
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(merged)?)?.build()?;
        let rows = reader
            .map(|batch| batch.map(|batch| batch.num_rows()))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<usize>();
        assert_eq!(rows, 2);
        Ok(())
    }
}
