use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Timelike, Utc};
use tokio::sync::{mpsc, oneshot};

use crate::{model::Record, runtime::Metrics, storage::Storage};

pub enum Command {
    Records(Vec<Record>),
    Flush,
    Rotate(DateTime<Utc>),
    Stats(oneshot::Sender<Result<serde_json::Value>>),
    Query {
        sql: String,
        response: oneshot::Sender<Result<serde_json::Value>>,
    },
}

#[derive(Debug, Clone)]
pub struct ClosedDatabase {
    pub hour: DateTime<Utc>,
    pub path: PathBuf,
}

#[derive(Clone)]
pub struct Writer {
    sender: mpsc::Sender<Command>,
    metrics: Arc<Metrics>,
    directory: PathBuf,
    exchange: String,
    active_hour_timestamp: Arc<AtomicI64>,
    checkpointing: Arc<RwLock<HashSet<PathBuf>>>,
}

struct RotationState {
    checkpoint_sender: std::sync::mpsc::Sender<CheckpointJob>,
    active_hour_timestamp: Arc<AtomicI64>,
    checkpointing: Arc<RwLock<HashSet<PathBuf>>>,
}

struct CheckpointJob {
    closed: ClosedDatabase,
    storage: Storage,
}

impl Writer {
    pub fn start(
        database: PathBuf,
        exchange: String,
        capacity: usize,
        batch_size: usize,
        flush_interval: Duration,
        metrics: Arc<Metrics>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<ClosedDatabase>)> {
        let directory = database
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&exchange);
        std::fs::create_dir_all(&directory).context("create hourly database directory")?;
        let hour = floor_hour(Utc::now())?;
        prepare_database(&hourly_path(&directory, hour))?;
        prepare_database(&hourly_path(&directory, hour + chrono::Duration::hours(1)))?;

        let (sender, receiver) = mpsc::channel(capacity);
        let (closed_sender, closed_receiver) = mpsc::unbounded_channel();
        let (checkpoint_sender, checkpoint_receiver) = std::sync::mpsc::channel();
        let active_hour_timestamp = Arc::new(AtomicI64::new(hour.timestamp()));
        let checkpointing = Arc::new(RwLock::new(HashSet::new()));
        let checkpointing_worker = Arc::clone(&checkpointing);
        std::thread::Builder::new()
            .name("duckdb-checkpoint".to_owned())
            .spawn(move || {
                checkpoint_run(checkpoint_receiver, closed_sender, &checkpointing_worker);
            })
            .context("start checkpoint worker")?;
        let run_directory = directory.clone();
        let run_metrics = Arc::clone(&metrics);
        let rotation = RotationState {
            checkpoint_sender,
            active_hour_timestamp: Arc::clone(&active_hour_timestamp),
            checkpointing: Arc::clone(&checkpointing),
        };
        tokio::task::spawn_blocking(move || {
            if let Err(error) = run(
                run_directory,
                hour,
                receiver,
                batch_size,
                &run_metrics,
                rotation,
            ) {
                tracing::error!(error = %format!("{error:#}"), "database writer stopped");
            }
        });

        let flush_sender = sender.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);
            interval.tick().await;
            loop {
                interval.tick().await;
                if flush_sender.send(Command::Flush).await.is_err() {
                    break;
                }
            }
        });
        let rotate_sender = sender.clone();
        tokio::spawn(async move {
            loop {
                let now = Utc::now();
                let Ok(current) = floor_hour(now) else { return };
                let next = current + chrono::Duration::hours(1);
                let wait = (next - now).to_std().unwrap_or_default();
                tokio::time::sleep(wait).await;
                if rotate_sender.send(Command::Rotate(next)).await.is_err() {
                    return;
                }
            }
        });

        Ok((
            Self {
                sender,
                metrics,
                directory,
                exchange,
                active_hour_timestamp,
                checkpointing,
            },
            closed_receiver,
        ))
    }

    pub async fn records(&self, records: Vec<Record>) -> Result<()> {
        self.observe_queue();
        let result = self
            .sender
            .send(Command::Records(records))
            .await
            .context("database writer stopped");
        self.observe_queue();
        result
    }

    pub async fn stats(&self) -> Result<serde_json::Value> {
        let (response, result) = oneshot::channel();
        self.sender.send(Command::Stats(response)).await?;
        result.await.context("database writer dropped response")?
    }

    pub async fn query(&self, sql: String) -> Result<serde_json::Value> {
        let (response, result) = oneshot::channel();
        self.sender.send(Command::Query { sql, response }).await?;
        result.await.context("database writer dropped response")?
    }

    pub fn database_path(&self, hour: DateTime<Utc>) -> PathBuf {
        hourly_path(&self.directory, hour)
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    pub fn active_hour(&self) -> Result<DateTime<Utc>> {
        DateTime::from_timestamp(self.active_hour_timestamp.load(Ordering::Acquire), 0)
            .context("invalid active database hour")
    }

    pub fn is_archive_ready(&self, hour: DateTime<Utc>) -> Result<bool> {
        if hour >= self.active_hour()? {
            return Ok(false);
        }
        let path = self.database_path(hour);
        if path.with_extension("duckdb.wal").exists() {
            return Ok(false);
        }
        let checkpointing = self
            .checkpointing
            .read()
            .map_err(|_| anyhow::anyhow!("checkpoint state lock poisoned"))?;
        Ok(!checkpointing.contains(&path))
    }

    fn observe_queue(&self) {
        let used = self
            .sender
            .max_capacity()
            .saturating_sub(self.sender.capacity());
        self.metrics.observe_writer_queue(used as u64);
    }
}

fn run(
    directory: PathBuf,
    mut hour: DateTime<Utc>,
    mut receiver: mpsc::Receiver<Command>,
    batch_size: usize,
    metrics: &Metrics,
    rotation: RotationState,
) -> Result<()> {
    let mut storage = Storage::open_existing(&hourly_path(&directory, hour))?;
    let mut pending = Vec::with_capacity(batch_size);
    while let Some(command) = receiver.blocking_recv() {
        match command {
            Command::Records(mut records) => {
                pending.append(&mut records);
                if pending.len() >= batch_size {
                    flush(&mut storage, &mut pending, batch_size, metrics)?;
                }
            }
            Command::Flush => flush(&mut storage, &mut pending, batch_size, metrics)?,
            Command::Rotate(next_hour) => {
                if next_hour <= hour {
                    continue;
                }
                flush(&mut storage, &mut pending, batch_size, metrics)?;
                let closed = ClosedDatabase {
                    hour,
                    path: hourly_path(&directory, hour),
                };
                rotation
                    .checkpointing
                    .write()
                    .map_err(|_| anyhow::anyhow!("checkpoint state lock poisoned"))?
                    .insert(closed.path.clone());
                prepare_database(&hourly_path(&directory, next_hour))?;
                prepare_database(&hourly_path(
                    &directory,
                    next_hour + chrono::Duration::hours(1),
                ))?;
                let next_storage = Storage::open_existing(&hourly_path(&directory, next_hour))?;
                let closed_storage = std::mem::replace(&mut storage, next_storage);
                hour = next_hour;
                rotation
                    .active_hour_timestamp
                    .store(hour.timestamp(), Ordering::Release);
                rotation
                    .checkpoint_sender
                    .send(CheckpointJob {
                        closed,
                        storage: closed_storage,
                    })
                    .map_err(|_| anyhow::anyhow!("checkpoint worker stopped"))?;
            }
            Command::Stats(response) => {
                flush(&mut storage, &mut pending, batch_size, metrics)?;
                let mut stats = storage.stats()?;
                if let Some(object) = stats.as_object_mut() {
                    object.insert("hour".to_owned(), hour.to_rfc3339().into());
                    object.insert(
                        "path".to_owned(),
                        hourly_path(&directory, hour).display().to_string().into(),
                    );
                }
                let _ = response.send(Ok(stats));
            }
            Command::Query { sql, response } => {
                flush(&mut storage, &mut pending, batch_size, metrics)?;
                let _ = response.send(storage.query_json(&sql));
            }
        }
    }
    flush(&mut storage, &mut pending, batch_size, metrics)
}

fn checkpoint_run(
    receiver: std::sync::mpsc::Receiver<CheckpointJob>,
    closed_sender: mpsc::UnboundedSender<ClosedDatabase>,
    checkpointing: &RwLock<HashSet<PathBuf>>,
) {
    while let Ok(job) = receiver.recv() {
        let CheckpointJob { closed, storage } = job;
        let result = storage.checkpoint();
        drop(storage);
        if let Ok(mut paths) = checkpointing.write() {
            paths.remove(&closed.path);
        }
        match result {
            Ok(()) => {
                let _ = closed_sender.send(closed);
            }
            Err(error) => {
                tracing::error!(
                    path = %closed.path.display(),
                    error = %format!("{error:#}"),
                    "background checkpoint failed"
                );
            }
        }
    }
}

fn flush(
    storage: &mut Storage,
    pending: &mut Vec<Record>,
    batch_size: usize,
    metrics: &Metrics,
) -> Result<()> {
    let written = storage.insert(pending)?;
    metrics
        .written_records
        .fetch_add(written as u64, std::sync::atomic::Ordering::Relaxed);
    pending.clear();
    if pending.capacity() > batch_size.saturating_mul(2) {
        pending.shrink_to(batch_size);
    }
    Ok(())
}

fn prepare_database(path: &Path) -> Result<()> {
    if !path.exists() {
        drop(Storage::open(path)?);
    }
    Ok(())
}

pub fn floor_hour(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    value
        .with_minute(0)
        .and_then(|v| v.with_second(0))
        .and_then(|v| v.with_nanosecond(0))
        .context("invalid UTC hour")
}

pub fn hourly_path(directory: &Path, hour: DateTime<Utc>) -> PathBuf {
    directory
        .join(format!(
            "{:04}-{:02}-{:02}",
            hour.year(),
            hour.month(),
            hour.day()
        ))
        .join(format!("{:02}.duckdb", hour.hour()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn rotation_closes_current_and_prepares_following_hour() -> Result<()> {
        let temporary = tempdir()?;
        let database = temporary.path().join("market.duckdb");
        let metrics = Arc::new(Metrics::default());
        let (writer, mut closed) = Writer::start(
            database,
            "binance".to_owned(),
            10,
            10,
            Duration::from_secs(60),
            metrics,
        )?;
        let current = floor_hour(Utc::now())?;
        let next = current + chrono::Duration::hours(1);
        writer.sender.send(Command::Rotate(next)).await?;
        let item = tokio::time::timeout(Duration::from_secs(5), closed.recv())
            .await?
            .context("rotation notification missing")?;
        assert_eq!(item.hour, current);
        assert_eq!(writer.active_hour()?, next);
        assert!(writer.is_archive_ready(current)?);
        assert!(!writer.is_archive_ready(next)?);
        assert!(item.path.is_file());
        assert!(writer.database_path(next).is_file());
        assert!(
            writer
                .database_path(next + chrono::Duration::hours(1))
                .is_file()
        );
        Ok(())
    }
}
