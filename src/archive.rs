use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{Client, primitives::ByteStream};
use chrono::{DateTime, Timelike, Utc};
use futures_util::{StreamExt, TryStreamExt, stream};

use crate::{backfill::Backfiller, config::Config, storage::UploadRecord, writer::Writer};

#[derive(Clone)]
pub struct Archiver {
    writer: Writer,
    client: Client,
    bucket: String,
    prefix: String,
    directory: PathBuf,
    backfiller: Backfiller,
    min_retention_hours: u64,
    max_retention_hours: u64,
    min_free_disk_percent: u64,
}

impl Archiver {
    pub async fn new(config: &Config, writer: Writer, backfiller: Backfiller) -> Self {
        let sdk = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.aws_region.clone()))
            .load()
            .await;
        Self {
            writer,
            client: Client::new(&sdk),
            bucket: config.s3_bucket.clone(),
            prefix: config.s3_prefix.trim_matches('/').to_owned(),
            directory: config.parquet_dir.clone(),
            backfiller,
            min_retention_hours: config.min_retention_hours,
            max_retention_hours: config.max_retention_hours,
            min_free_disk_percent: config.min_free_disk_percent,
        }
    }

    pub async fn export(&self, start: DateTime<Utc>, force: bool) -> Result<Vec<String>> {
        std::fs::create_dir_all(&self.directory).context("create parquet directory")?;
        self.cleanup().await?;
        self.backfiller.run().await.context("pre-export backfill")?;
        let start = start
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0))
            .context("invalid archive hour")?;
        let end = start + chrono::Duration::hours(1);
        let files = self
            .writer
            .export(start, end, self.directory.clone(), force)
            .await?;
        let uploads = stream::iter(files.into_iter().map(|file| self.upload_file(file, start)))
            .buffer_unordered(16)
            .try_collect::<Vec<_>>()
            .await?;
        let keys = uploads
            .iter()
            .map(|upload| upload.key.clone())
            .collect::<Vec<_>>();
        self.writer.record_uploads(uploads).await?;
        self.cleanup().await?;
        Ok(keys)
    }

    async fn upload_file(
        &self,
        file: crate::storage::ExportFile,
        start: DateTime<Utc>,
    ) -> Result<UploadRecord> {
        let relative = file
            .path
            .strip_prefix(&self.directory)
            .context("parquet file is outside archive directory")?;
        let key = format!("{}/{}", self.prefix, relative.to_string_lossy());
        let body = ByteStream::from_path(&file.path)
            .await
            .context("read parquet file")?;
        let output = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await?;
        self.client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;
        let size = std::fs::metadata(&file.path)?.len();
        Ok(UploadRecord {
            table: file.table,
            symbol: file.symbol,
            start,
            bucket: self.bucket.clone(),
            key,
            etag: output.e_tag().unwrap_or_default().to_owned(),
            size,
        })
    }

    async fn cleanup(&self) -> Result<()> {
        let total = fs2::total_space(&self.directory)?;
        let available = fs2::available_space(&self.directory)?;
        let free_percent = available
            .saturating_mul(100)
            .checked_div(total)
            .unwrap_or(100);
        let hours = if free_percent < self.min_free_disk_percent {
            self.min_retention_hours
        } else {
            self.max_retention_hours
        };
        let cutoff = Utc::now() - chrono::Duration::hours(i64::try_from(hours)?);
        let deleted = self.writer.cleanup(cutoff, self.bucket.clone()).await?;
        if deleted > 0 {
            tracing::info!(
                deleted,
                free_percent,
                retention_hours = hours,
                "cleaned uploaded rows"
            );
        }
        Ok(())
    }

    pub fn spawn_hourly(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                let now = Utc::now();
                let next = now
                    .with_minute(0)
                    .and_then(|value| value.with_second(0))
                    .and_then(|value| value.with_nanosecond(0))
                    .map(|value| value + chrono::Duration::hours(1));
                let Some(next) = next else { return };
                let wait =
                    (next - now).to_std().unwrap_or_default() + std::time::Duration::from_secs(5);
                tokio::time::sleep(wait).await;
                let hour = next - chrono::Duration::hours(1);
                if let Err(error) = self.export(hour, false).await {
                    tracing::error!(%error, %hour, "hourly archive failed");
                }
            }
        });
    }
}
