use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use duckdb::types::Value;
use tokio::sync::mpsc;

use crate::{
    model::Record,
    parquet_sink::{Partition, partition, write_part},
    runtime::Metrics,
    uploader::Uploader,
};

const WRITER_SHARDS: usize = 4;
const MAX_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const TARGET_BUFFER_BYTES: usize = 12 * 1024 * 1024;
const PARTITION_BYTES: usize = 4 * 1024 * 1024;
const PARTITION_ROWS: usize = 16_384;
const MAX_BUFFER_AGE: Duration = Duration::from_secs(5 * 60);

#[derive(Clone)]
pub struct StreamWriter {
    senders: Arc<Vec<mpsc::Sender<Command>>>,
    stats: Arc<RwLock<WriterStats>>,
    metrics: Arc<Metrics>,
}

enum Command {
    Records(Vec<Record>),
    Flush,
}

#[derive(Default)]
struct Buffer {
    records: Vec<Record>,
    bytes: usize,
    created: Option<Instant>,
}

#[derive(Default, serde::Serialize)]
pub struct WriterStats {
    pub parquet_files: u64,
    pub parquet_bytes: u64,
    pub buffered_records: u64,
    pub buffered_bytes: u64,
    pub table_rows: HashMap<String, u64>,
}

impl StreamWriter {
    pub fn start(
        directory: PathBuf,
        capacity: usize,
        metrics: Arc<Metrics>,
        uploader: Uploader,
    ) -> Self {
        let stats = Arc::new(RwLock::new(WriterStats::default()));
        let mut senders = Vec::with_capacity(WRITER_SHARDS);
        for shard in 0..WRITER_SHARDS {
            let (sender, receiver) = mpsc::channel(capacity.div_ceil(WRITER_SHARDS));
            senders.push(sender);
            let worker_stats = Arc::clone(&stats);
            let worker_metrics = Arc::clone(&metrics);
            let worker_directory = directory.clone();
            let worker_uploader = uploader.clone();
            tokio::task::spawn_blocking(move || {
                if let Err(error) = run(
                    shard,
                    worker_directory,
                    receiver,
                    worker_uploader,
                    &worker_stats,
                    &worker_metrics,
                ) {
                    tracing::error!(shard, %error, "Parquet writer stopped");
                }
            });
        }
        let senders = Arc::new(senders);
        let flush = Arc::clone(&senders);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.tick().await;
            loop {
                interval.tick().await;
                for sender in flush.iter() {
                    if sender.send(Command::Flush).await.is_err() {
                        return;
                    }
                }
            }
        });
        Self {
            senders,
            stats,
            metrics,
        }
    }

    pub async fn records(&self, records: Vec<Record>) -> Result<()> {
        self.observe_queue();
        let mut shards: HashMap<usize, Vec<Record>> = HashMap::new();
        for record in records {
            let key = partition(&record)?;
            let shard = partition_shard(&key);
            shards.entry(shard).or_default().push(record);
        }
        for (shard, records) in shards {
            self.senders[shard]
                .send(Command::Records(records))
                .await
                .context("Parquet writer stopped")?;
        }
        self.observe_queue();
        Ok(())
    }

    pub fn stats(&self) -> serde_json::Value {
        let stats = self.stats.read().unwrap_or_else(|error| error.into_inner());
        serde_json::to_value(&*stats).unwrap_or_default()
    }

    fn observe_queue(&self) {
        let used = self.senders.iter().fold(0, |used, sender| {
            used + sender.max_capacity().saturating_sub(sender.capacity())
        });
        self.metrics.observe_writer_queue(used as u64);
    }
}

fn run(
    shard: usize,
    directory: PathBuf,
    mut receiver: mpsc::Receiver<Command>,
    uploader: Uploader,
    stats: &RwLock<WriterStats>,
    metrics: &Metrics,
) -> Result<()> {
    let mut buffers: HashMap<Partition, Buffer> = HashMap::new();
    let mut total_bytes = 0;
    let mut sequence = 0;
    let mut previous_records = 0;
    let mut previous_bytes = 0;
    while let Some(command) = receiver.blocking_recv() {
        match command {
            Command::Records(records) => {
                for record in records {
                    let key = partition(&record)?;
                    let bytes = estimated_bytes(&record);
                    let buffer = buffers.entry(key).or_default();
                    buffer.created.get_or_insert_with(Instant::now);
                    buffer.bytes += bytes;
                    buffer.records.push(record);
                    total_bytes += bytes;
                }
                let ready = buffers
                    .iter()
                    .filter(|(_, buffer)| {
                        buffer.records.len() >= PARTITION_ROWS || buffer.bytes >= PARTITION_BYTES
                    })
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                flush_keys(
                    &directory,
                    &mut buffers,
                    &ready,
                    &mut total_bytes,
                    &mut sequence,
                    shard,
                    &uploader,
                    stats,
                    metrics,
                )?;
                while total_bytes > MAX_BUFFER_BYTES {
                    let Some(key) = buffers
                        .iter()
                        .max_by_key(|(_, buffer)| buffer.bytes)
                        .map(|(key, _)| key.clone())
                    else {
                        break;
                    };
                    flush_keys(
                        &directory,
                        &mut buffers,
                        &[key],
                        &mut total_bytes,
                        &mut sequence,
                        shard,
                        &uploader,
                        stats,
                        metrics,
                    )?;
                    if total_bytes <= TARGET_BUFFER_BYTES {
                        break;
                    }
                }
            }
            Command::Flush => {
                let now = Instant::now();
                let ready = buffers
                    .iter()
                    .filter(|(_, buffer)| {
                        buffer
                            .created
                            .is_some_and(|created| now.duration_since(created) >= MAX_BUFFER_AGE)
                    })
                    .take(64)
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                flush_keys(
                    &directory,
                    &mut buffers,
                    &ready,
                    &mut total_bytes,
                    &mut sequence,
                    shard,
                    &uploader,
                    stats,
                    metrics,
                )?;
            }
        }
        update_buffer_stats(
            stats,
            &buffers,
            total_bytes,
            &mut previous_records,
            &mut previous_bytes,
        );
    }
    let keys = buffers.keys().cloned().collect::<Vec<_>>();
    flush_keys(
        &directory,
        &mut buffers,
        &keys,
        &mut total_bytes,
        &mut sequence,
        shard,
        &uploader,
        stats,
        metrics,
    )
}

#[allow(clippy::too_many_arguments)]
fn flush_keys(
    directory: &std::path::Path,
    buffers: &mut HashMap<Partition, Buffer>,
    keys: &[Partition],
    total_bytes: &mut usize,
    sequence: &mut u64,
    shard: usize,
    uploader: &Uploader,
    stats: &RwLock<WriterStats>,
    metrics: &Metrics,
) -> Result<()> {
    for key in keys {
        let Some(buffer) = buffers.remove(key) else {
            continue;
        };
        *total_bytes = total_bytes.saturating_sub(buffer.bytes);
        *sequence = sequence.wrapping_add(1);
        let file_sequence = ((shard as u64) << 56) | *sequence;
        let rows = buffer.records.len();
        let path = write_part(directory, key, file_sequence, &buffer.records)?;
        let size = std::fs::metadata(&path)?.len();
        // The part is discoverable by the uploader's periodic filesystem scan even if
        // shutdown has already closed its notification channel.
        let _ = uploader.blocking_file(path);
        metrics
            .written_records
            .fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed);
        let mut state = stats.write().unwrap_or_else(|error| error.into_inner());
        state.parquet_files += 1;
        state.parquet_bytes += size;
        *state.table_rows.entry(key.table.to_owned()).or_default() += rows as u64;
    }
    Ok(())
}

fn update_buffer_stats(
    stats: &RwLock<WriterStats>,
    buffers: &HashMap<Partition, Buffer>,
    total_bytes: usize,
    previous_records: &mut u64,
    previous_bytes: &mut u64,
) {
    let records = buffers
        .values()
        .map(|buffer| buffer.records.len() as u64)
        .sum::<u64>();
    let mut state = stats.write().unwrap_or_else(|error| error.into_inner());
    state.buffered_records = state
        .buffered_records
        .saturating_sub(*previous_records)
        .saturating_add(records);
    state.buffered_bytes = state
        .buffered_bytes
        .saturating_sub(*previous_bytes)
        .saturating_add(total_bytes as u64);
    *previous_records = records;
    *previous_bytes = total_bytes as u64;
}

fn partition_shard(partition: &Partition) -> usize {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    partition.hash(&mut hasher);
    (hasher.finish() as usize) % WRITER_SHARDS
}

fn estimated_bytes(record: &Record) -> usize {
    record.values.iter().fold(32, |size, value| {
        size + match value {
            Value::Text(value) => value.len() + 24,
            Value::Blob(value) | Value::Geometry(value) => value.len() + 24,
            _ => 24,
        }
    })
}
