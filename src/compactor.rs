use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::ArrowError;
use arrow_select::take::take;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{
    Client,
    primitives::ByteStream,
    types::{Delete, ObjectIdentifier, StorageClass},
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeDelta, Utc};
use clap::Parser;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

const OUTPUT_NAME: &str = "compacted.parquet";
const SUCCESS_NAME: &str = "_COMPACTION_SUCCESS.json";

#[derive(Debug, Clone, Parser)]
pub struct Options {
    #[arg(long, default_value = "coin-data-196920285698-ap-southeast-1-an")]
    pub bucket: String,
    #[arg(long, default_value = "parquet/rust")]
    pub prefix: String,
    #[arg(long, default_value = "parquet/compaction-source")]
    pub retention_prefix: String,
    #[arg(long, default_value = "ap-southeast-1")]
    pub region: String,
    #[arg(long, default_value_t = 20)]
    pub minimum_files: usize,
    #[arg(long, default_value_t = 10)]
    pub minimum_small_files: usize,
    #[arg(long, default_value_t = 8)]
    pub small_file_mb: u64,
    #[arg(long, default_value_t = 10)]
    pub settle_minutes: i64,
    #[arg(
        long,
        default_value_t = 48,
        help = "only discover hourly prefixes within this many hours"
    )]
    pub lookback_hours: i64,
    #[arg(
        long,
        default_value = "parquet/compaction-source-control/checkpoint.json",
        help = "S3 key used to persist completed partition fingerprints"
    )]
    pub checkpoint_key: String,
    #[arg(long, default_value_t = 262_144)]
    pub batch_rows: usize,
    #[arg(
        long,
        help = "perform writes; without this flag only report candidates"
    )]
    pub execute: bool,
    #[arg(
        long,
        default_value_t = 0,
        help = "limit partitions per run; 0 is unlimited"
    )]
    pub max_partitions: usize,
}

#[derive(Debug, Clone)]
struct Source {
    key: String,
    bytes: u64,
}

#[derive(Debug)]
struct Partition {
    prefix: String,
    hour: DateTime<Utc>,
    sources: Vec<Source>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Completion {
    completed_at: DateTime<Utc>,
    output_key: String,
    input_files: usize,
    input_bytes: u64,
    rows: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Checkpoint {
    version: u8,
    updated_at: Option<DateTime<Utc>>,
    partitions: BTreeMap<String, PartitionCheckpoint>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PartitionCheckpoint {
    hour: DateTime<Utc>,
    source_keys: Vec<String>,
    source_bytes: u64,
    completed_at: DateTime<Utc>,
}

impl PartitionCheckpoint {
    fn matches(&self, partition: &Partition) -> bool {
        let mut keys = partition
            .sources
            .iter()
            .map(|source| source.key.clone())
            .collect::<Vec<_>>();
        keys.sort_unstable();
        self.hour == partition.hour
            && self.source_bytes
                == partition
                    .sources
                    .iter()
                    .map(|source| source.bytes)
                    .sum::<u64>()
            && self.source_keys == keys
    }
}

pub struct Compactor {
    options: Options,
    client: Client,
}

impl Compactor {
    pub async fn new(options: Options) -> Result<Self> {
        validate(&options)?;
        let sdk = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(options.region.clone()))
            .load()
            .await;
        Ok(Self {
            options,
            client: Client::new(&sdk),
        })
    }

    pub async fn run(&self) -> Result<()> {
        if !self.options.execute {
            return self.run_partitions().await;
        }
        let lock_key = self.acquire_lock().await?;
        let result = self.run_partitions().await;
        let release = self.release_lock(&lock_key).await;
        result.and(release)
    }

    async fn run_partitions(&self) -> Result<()> {
        let mut checkpoint = self.load_checkpoint().await?;
        let mut partitions = self.candidates(&checkpoint).await?;
        partitions.sort_by_key(|partition| partition.hour);
        if self.options.max_partitions > 0 {
            partitions.truncate(self.options.max_partitions);
        }
        let files = partitions
            .iter()
            .map(|partition| partition.sources.len())
            .sum::<usize>();
        let bytes = partitions
            .iter()
            .flat_map(|partition| &partition.sources)
            .map(|source| source.bytes)
            .sum::<u64>();
        tracing::info!(
            partitions = partitions.len(),
            files,
            bytes,
            execute = self.options.execute,
            "compaction scan complete"
        );
        if !self.options.execute {
            return Ok(());
        }
        for partition in partitions {
            let partition_key = partition.prefix.clone();
            let mut source_keys = partition
                .sources
                .iter()
                .map(|source| source.key.clone())
                .collect::<Vec<_>>();
            source_keys.sort_unstable();
            let entry = PartitionCheckpoint {
                hour: partition.hour,
                source_keys,
                source_bytes: partition.sources.iter().map(|source| source.bytes).sum(),
                completed_at: Utc::now(),
            };
            self.compact(partition).await?;
            checkpoint.partitions.insert(partition_key, entry);
            checkpoint.updated_at = Some(Utc::now());
            self.save_checkpoint(&checkpoint).await?;
        }
        self.prune_checkpoint(&mut checkpoint);
        self.save_checkpoint(&checkpoint).await?;
        Ok(())
    }

    async fn load_checkpoint(&self) -> Result<Checkpoint> {
        match self
            .client
            .get_object()
            .bucket(&self.options.bucket)
            .key(&self.options.checkpoint_key)
            .send()
            .await
        {
            Ok(response) => {
                let bytes = response.body.collect().await?.into_bytes();
                serde_json::from_slice(&bytes).context("decode compaction checkpoint")
            }
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|value| value.meta().code() == Some("NoSuchKey")) =>
            {
                Ok(Checkpoint {
                    version: 1,
                    ..Checkpoint::default()
                })
            }
            Err(error) => Err(error).context("load compaction checkpoint"),
        }
    }

    async fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.options.bucket)
            .key(&self.options.checkpoint_key)
            .body(ByteStream::from(serde_json::to_vec(checkpoint)?))
            .send()
            .await
            .context("save compaction checkpoint")?;
        Ok(())
    }

    fn prune_checkpoint(&self, checkpoint: &mut Checkpoint) {
        let cutoff = Utc::now() - TimeDelta::hours(self.options.lookback_hours + 24);
        checkpoint
            .partitions
            .retain(|_, entry| entry.hour >= cutoff);
    }

    async fn acquire_lock(&self) -> Result<String> {
        let key = format!(
            "{}-control/lock",
            self.options.retention_prefix.trim_end_matches('/')
        );
        if let Ok(existing) = self
            .client
            .head_object()
            .bucket(&self.options.bucket)
            .key(&key)
            .send()
            .await
        {
            let stale_before = Utc::now().timestamp() - TimeDelta::hours(12).num_seconds();
            if existing
                .last_modified()
                .is_some_and(|modified| modified.secs() < stale_before)
            {
                self.client
                    .delete_object()
                    .bucket(&self.options.bucket)
                    .key(&key)
                    .send()
                    .await
                    .context("remove stale compaction lock")?;
            } else {
                bail!("another compaction task holds {key}");
            }
        }
        self.client
            .put_object()
            .bucket(&self.options.bucket)
            .key(&key)
            .if_none_match("*")
            .body(ByteStream::from(Utc::now().to_rfc3339().into_bytes()))
            .send()
            .await
            .context("acquire compaction lock")?;
        Ok(key)
    }

    async fn release_lock(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.options.bucket)
            .key(key)
            .send()
            .await
            .context("release compaction lock")?;
        Ok(())
    }

    async fn candidates(&self, checkpoint: &Checkpoint) -> Result<Vec<Partition>> {
        let cutoff = Utc::now() - TimeDelta::minutes(self.options.settle_minutes);
        let mut grouped = BTreeMap::<String, Partition>::new();
        let window_start = Utc::now() - TimeDelta::hours(self.options.lookback_hours);
        for hour_prefix in self.recent_hour_prefixes(window_start).await? {
            let mut pages = self
                .client
                .list_objects_v2()
                .bucket(&self.options.bucket)
                .prefix(&hour_prefix)
                .into_paginator()
                .send();
            while let Some(page) = pages.next().await {
                let page = page.context("list Parquet objects")?;
                for object in page.contents() {
                    let Some(key) = object.key() else {
                        continue;
                    };
                    if !is_source_key(key) {
                        continue;
                    }
                    let Some((partition_prefix, hour)) = partition_from_key(key) else {
                        tracing::warn!(%key, "ignore object outside an hourly partition");
                        continue;
                    };
                    if hour + TimeDelta::hours(1) > cutoff {
                        continue;
                    }
                    let bytes =
                        u64::try_from(object.size().unwrap_or_default()).unwrap_or_default();
                    let partition =
                        grouped
                            .entry(partition_prefix.clone())
                            .or_insert_with(|| Partition {
                                prefix: partition_prefix,
                                hour,
                                sources: Vec::new(),
                            });
                    partition.sources.push(Source {
                        key: key.to_owned(),
                        bytes,
                    });
                }
            }
        }
        let small_limit = self.options.small_file_mb.saturating_mul(1_024 * 1_024);
        Ok(grouped
            .into_values()
            .filter(|partition| {
                let small = partition
                    .sources
                    .iter()
                    .filter(|source| source.bytes < small_limit)
                    .count();
                partition.sources.len() >= self.options.minimum_files
                    || small >= self.options.minimum_small_files
            })
            .filter(|partition| {
                !checkpoint
                    .partitions
                    .get(&partition.prefix)
                    .is_some_and(|entry| entry.matches(partition))
            })
            .collect())
    }

    async fn recent_hour_prefixes(&self, window_start: DateTime<Utc>) -> Result<Vec<String>> {
        let root = format!("{}/", self.options.prefix.trim_matches('/'));
        let mut dates = Vec::new();
        self.find_date_prefixes(&root, window_start.date_naive(), 0, &mut dates)
            .await?;
        let mut hours = Vec::new();
        for date_prefix in dates {
            hours.extend(self.list_common_prefixes(&date_prefix).await?);
        }
        Ok(hours)
    }

    async fn find_date_prefixes(
        &self,
        prefix: &str,
        first_date: NaiveDate,
        depth: usize,
        dates: &mut Vec<String>,
    ) -> Result<()> {
        // The producer currently uses exchange/table/date/hour, but walking
        // prefixes keeps this compatible with additional table dimensions.
        let mut pending = vec![(prefix.to_owned(), depth)];
        while let Some((current, current_depth)) = pending.pop() {
            if current_depth > 8 {
                continue;
            }
            for child in self.list_common_prefixes(&current).await? {
                let component = child
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or_default();
                if let Some(date) = parse_date_component(component) {
                    if date >= first_date {
                        dates.push(child);
                    }
                } else {
                    pending.push((child, current_depth + 1));
                }
            }
        }
        Ok(())
    }

    async fn list_common_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.options.bucket)
            .prefix(prefix)
            .delimiter("/")
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            for common in page.context("list S3 prefixes")?.common_prefixes() {
                if let Some(value) = common.prefix() {
                    result.push(value.to_owned());
                }
            }
        }
        Ok(result)
    }

    async fn compact(&self, mut partition: Partition) -> Result<()> {
        partition
            .sources
            .sort_by(|left, right| left.key.cmp(&right.key));
        let success_key = format!("{}/{}", partition.prefix, SUCCESS_NAME);
        let output_key = format!("{}/{}", partition.prefix, OUTPUT_NAME);
        let mut merge_sources = partition.sources.clone();
        if self.exists(&success_key).await? {
            let existing = self
                .client
                .head_object()
                .bucket(&self.options.bucket)
                .key(&output_key)
                .send()
                .await
                .context("completed partition is missing its compacted output")?;
            merge_sources.insert(
                0,
                Source {
                    key: output_key.clone(),
                    bytes: u64::try_from(existing.content_length().unwrap_or_default())
                        .unwrap_or_default(),
                },
            );
        }
        let temporary = tempfile::tempdir().context("create compaction workspace")?;
        let output = temporary.path().join(OUTPUT_NAME);
        let rows = self
            .merge(&merge_sources, temporary.path(), &output)
            .await?;
        verify_output(&output, rows)?;
        self.upload(&output_key, &output).await?;
        self.retain_sources(&partition.sources, &partition.prefix)
            .await?;
        let completion = Completion {
            completed_at: Utc::now(),
            output_key: output_key.clone(),
            input_files: partition.sources.len(),
            input_bytes: partition.sources.iter().map(|source| source.bytes).sum(),
            rows,
        };
        self.client
            .put_object()
            .bucket(&self.options.bucket)
            .key(&success_key)
            .body(ByteStream::from(serde_json::to_vec(&completion)?))
            .send()
            .await
            .context("write compaction success marker")?;
        self.delete_sources(&partition.sources).await?;
        tracing::info!(partition = %partition.prefix, files = partition.sources.len(), rows, output = %output_key, "partition compacted");
        Ok(())
    }

    async fn merge(&self, sources: &[Source], directory: &Path, output: &Path) -> Result<u64> {
        let mut writer = None;
        let mut output_schema = None;
        let mut rows = 0u64;
        for (index, source) in sources.iter().enumerate() {
            let input = directory.join(format!("input-{index:06}.parquet"));
            self.download(&source.key, &input).await?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&input)?)?;
            let schema = builder.schema().clone();
            if output_schema
                .as_ref()
                .is_some_and(|expected| expected != &schema)
            {
                bail!("schema mismatch in {}", source.key);
            }
            if writer.is_none() {
                writer = Some(ArrowWriter::try_new(
                    File::create(output)?,
                    Arc::clone(&schema),
                    Some(writer_properties()?),
                )?);
                output_schema = Some(schema);
            }
            let current = writer.as_mut().context("missing output writer")?;
            let mut reader = builder.with_batch_size(self.options.batch_rows).build()?;
            for batch in &mut reader {
                let batch = batch.with_context(|| format!("read {}", source.key))?;
                rows = rows.saturating_add(batch.num_rows() as u64);
                current.write(&reorder_batch(&batch)?)?;
            }
            std::fs::remove_file(&input)?;
        }
        let Some(writer) = writer else {
            bail!("partition has no readable source files");
        };
        writer.close()?;
        Ok(rows)
    }

    async fn download(&self, key: &str, path: &Path) -> Result<()> {
        let response = self
            .client
            .get_object()
            .bucket(&self.options.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("download {key}"))?;
        let mut reader = response.body.into_async_read();
        let mut file = tokio::fs::File::create(path).await?;
        tokio::io::copy(&mut reader, &mut file).await?;
        file.flush().await?;
        Ok(())
    }

    async fn upload(&self, key: &str, path: &Path) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.options.bucket)
            .key(key)
            .storage_class(StorageClass::IntelligentTiering)
            .body(ByteStream::from_path(path).await?)
            .send()
            .await
            .with_context(|| format!("upload {key}"))?;
        Ok(())
    }

    async fn retain_sources(&self, sources: &[Source], partition: &str) -> Result<()> {
        let root = self.options.prefix.trim_matches('/');
        let relative = partition
            .strip_prefix(root)
            .unwrap_or(partition)
            .trim_matches('/');
        for source in sources {
            let name = source
                .key
                .rsplit('/')
                .next()
                .context("source has no file name")?;
            let destination = format!(
                "{}/{relative}/{name}",
                self.options.retention_prefix.trim_matches('/')
            );
            self.client
                .copy_object()
                .bucket(&self.options.bucket)
                .key(&destination)
                .copy_source(format!("{}/{}", self.options.bucket, source.key))
                .send()
                .await
                .with_context(|| format!("retain {}", source.key))?;
            let retained = self
                .client
                .head_object()
                .bucket(&self.options.bucket)
                .key(&destination)
                .send()
                .await
                .with_context(|| format!("verify retained {destination}"))?;
            if u64::try_from(retained.content_length().unwrap_or_default()).unwrap_or_default()
                != source.bytes
            {
                bail!("retained size mismatch for {}", source.key);
            }
        }
        Ok(())
    }

    async fn delete_sources(&self, sources: &[Source]) -> Result<()> {
        for chunk in sources.chunks(1_000) {
            let objects = chunk
                .iter()
                .map(|source| ObjectIdentifier::builder().key(&source.key).build())
                .collect::<Result<Vec<_>, _>>()?;
            let delete = Delete::builder().set_objects(Some(objects)).build()?;
            let response = self
                .client
                .delete_objects()
                .bucket(&self.options.bucket)
                .delete(delete)
                .send()
                .await
                .context("delete compacted source objects")?;
            if !response.errors().is_empty() {
                bail!("S3 rejected one or more source deletions");
            }
        }
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.options.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|value| value.is_not_found()) =>
            {
                Ok(false)
            }
            Err(error) => Err(error).with_context(|| format!("head {key}")),
        }
    }
}

fn validate(options: &Options) -> Result<()> {
    if options.bucket.is_empty() || options.prefix.trim_matches('/').is_empty() {
        bail!("bucket and prefix must not be empty");
    }
    if options.minimum_files == 0
        || options.minimum_small_files == 0
        || options.small_file_mb == 0
        || options.batch_rows == 0
        || options.settle_minutes < 0
        || options.lookback_hours <= 0
        || options.checkpoint_key.trim_matches('/').is_empty()
    {
        bail!("compaction limits must be positive");
    }
    Ok(())
}

fn writer_properties() -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(1_048_576))
        .build())
}

pub fn reorder_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let Ok(symbol) = batch.schema().index_of("symbol") else {
        return Ok(batch.clone());
    };
    let mut sort_columns = vec![SortColumn {
        values: batch.column(symbol).clone(),
        options: None,
    }];
    for name in ["event_time", "observed_at", "received_at"] {
        let Ok(index) = batch.schema().index_of(name) else {
            continue;
        };
        sort_columns.push(SortColumn {
            values: batch.column(index).clone(),
            options: None,
        });
    }
    let indices = lexsort_to_indices(&sort_columns, None)?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, ArrowError>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

fn verify_output(path: &Path, expected_rows: u64) -> Result<()> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let rows = u64::try_from(builder.metadata().file_metadata().num_rows()).unwrap_or_default();
    if rows != expected_rows {
        bail!("output row count mismatch: expected {expected_rows}, got {rows}");
    }
    Ok(())
}

fn is_source_key(key: &str) -> bool {
    key.ends_with(".parquet")
        && key
            .rsplit('/')
            .next()
            .is_some_and(|name| name.starts_with("segment-"))
}

fn parse_date_component(component: &str) -> Option<NaiveDate> {
    let value = component.strip_prefix("date=").unwrap_or(component);
    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

fn partition_from_key(key: &str) -> Option<(String, DateTime<Utc>)> {
    let (partition, _) = key.rsplit_once('/')?;
    let (day, hour) = partition.rsplit_once('/')?;
    let (_, day) = day.rsplit_once('/')?;
    let parsed = NaiveDateTime::parse_from_str(&format!("{day} {hour}:00:00"), "%Y-%m-%d %H:%M:%S")
        .ok()?
        .and_utc();
    Some((partition.to_owned(), parsed))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn hourly_partition_should_parse() {
        let result = partition_from_key(
            "parquet/rust/binance/usdm/futures_depth_updates/2026-08-04/02/segment-1.parquet",
        );
        assert_eq!(
            result.map(|(prefix, hour)| (prefix, hour.to_rfc3339())),
            Some((
                "parquet/rust/binance/usdm/futures_depth_updates/2026-08-04/02".to_owned(),
                "2026-08-04T02:00:00+00:00".to_owned(),
            ))
        );
    }

    #[test]
    fn compacted_output_should_not_be_a_source() {
        assert!(!is_source_key("path/compacted.parquet"));
    }

    #[test]
    fn reorder_batch_should_sort_by_symbol_then_available_times() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("event_time", DataType::Int64, false),
            Field::new("received_at", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["B", "A", "B", "A", "A"])),
                Arc::new(Int64Array::from(vec![2, 2, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 1, 2, 1])),
                Arc::new(Int64Array::from(vec![20, 12, 10, 13, 11])),
            ],
        )?;

        let sorted = reorder_batch(&batch)?;
        let symbols = sorted
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("symbol column")?;
        let times = sorted
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("time column")?;
        let values = sorted
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("value column")?;
        let actual = (0..sorted.num_rows())
            .map(|index| {
                (
                    symbols.value(index),
                    times.value(index),
                    values.value(index),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                ("A", 1, 11),
                ("A", 1, 13),
                ("A", 2, 12),
                ("B", 1, 10),
                ("B", 2, 20)
            ]
        );
        Ok(())
    }
}
