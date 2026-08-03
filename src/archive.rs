use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{Client, primitives::ByteStream};
use chrono::{DateTime, Timelike, Utc};

use crate::{backfill::Backfiller, config::Config, writer::Writer};

#[derive(Clone)]
pub struct Archiver {
    writer: Writer,
    client: Client,
    bucket: String,
    prefix: String,
    directory: PathBuf,
    backfiller: Backfiller,
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
        }
    }

    pub async fn export(&self, start: DateTime<Utc>, force: bool) -> Result<Vec<String>> {
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
        let mut keys = Vec::with_capacity(files.len());
        for path in files {
            let filename = path
                .file_name()
                .context("parquet file has no name")?
                .to_string_lossy();
            let key = format!("{}/{}/{filename}", self.prefix, start.format("%Y/%m/%d/%H"));
            let body = ByteStream::from_path(&path)
                .await
                .context("read parquet file")?;
            self.client
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
            keys.push(key);
        }
        Ok(keys)
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
