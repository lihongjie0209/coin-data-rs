use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::{
    model::Record,
    runtime::Metrics,
    storage::{ExportFile, Storage, UploadRecord},
};

pub enum Command {
    Records(Vec<Record>),
    Stats(oneshot::Sender<Result<Value>>),
    Query {
        sql: String,
        response: oneshot::Sender<Result<Value>>,
    },
    Flush,
    Barrier(oneshot::Sender<Result<()>>),
    Upload {
        uploads: Vec<UploadRecord>,
        response: oneshot::Sender<Result<()>>,
    },
    Cleanup {
        cutoff: DateTime<Utc>,
        bucket: String,
        response: oneshot::Sender<Result<usize>>,
    },
    Shutdown(oneshot::Sender<Result<()>>),
}

#[derive(Clone)]
pub struct Writer {
    sender: mpsc::Sender<Command>,
    database: PathBuf,
    metrics: Arc<Metrics>,
}

impl Writer {
    pub fn start(
        database: PathBuf,
        capacity: usize,
        batch_size: usize,
        flush_interval: Duration,
        metrics: Arc<Metrics>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        let writer_database = database.clone();
        let writer_metrics = Arc::clone(&metrics);
        tokio::task::spawn_blocking(move || {
            if let Err(error) = run(database, receiver, batch_size, &writer_metrics) {
                tracing::error!(%error, "database writer stopped");
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
        Self {
            sender,
            database: writer_database,
            metrics,
        }
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

    pub async fn stats(&self) -> Result<Value> {
        let (response, result) = oneshot::channel();
        self.sender.send(Command::Stats(response)).await?;
        result.await.context("database writer dropped response")?
    }

    pub async fn query(&self, sql: String) -> Result<Value> {
        let (response, result) = oneshot::channel();
        self.sender.send(Command::Query { sql, response }).await?;
        result.await.context("database writer dropped response")?
    }

    pub async fn export(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        directory: PathBuf,
        force: bool,
    ) -> Result<Vec<ExportFile>> {
        let (response, result) = oneshot::channel();
        self.sender.send(Command::Barrier(response)).await?;
        result.await.context("database writer dropped barrier")??;
        let database = self.database.clone();
        tokio::task::spawn_blocking(move || {
            let mut storage = Storage::open(&database)?;
            storage.export_hour(start, end, &directory, force)
        })
        .await
        .context("Parquet export task failed")?
    }

    pub async fn record_uploads(&self, uploads: Vec<UploadRecord>) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.sender
            .send(Command::Upload { uploads, response })
            .await?;
        result.await.context("database writer dropped response")?
    }

    pub async fn cleanup(&self, cutoff: DateTime<Utc>, bucket: String) -> Result<usize> {
        let (response, result) = oneshot::channel();
        self.sender
            .send(Command::Cleanup {
                cutoff,
                bucket,
                response,
            })
            .await?;
        result.await.context("database writer dropped response")?
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
    database: PathBuf,
    mut receiver: mpsc::Receiver<Command>,
    batch_size: usize,
    metrics: &Metrics,
) -> Result<()> {
    let mut storage = Storage::open(&database)?;
    let mut pending = Vec::with_capacity(batch_size);
    while let Some(command) = receiver.blocking_recv() {
        match command {
            Command::Records(mut records) => {
                pending.append(&mut records);
                if pending.len() >= batch_size {
                    flush(&mut storage, &mut pending, metrics)?;
                }
            }
            Command::Flush => flush(&mut storage, &mut pending, metrics)?,
            other => {
                flush(&mut storage, &mut pending, metrics)?;
                if handle(&mut storage, other)? {
                    return Ok(());
                }
            }
        }
    }
    flush(&mut storage, &mut pending, metrics)
}

fn flush(storage: &mut Storage, pending: &mut Vec<Record>, metrics: &Metrics) -> Result<()> {
    let written = storage.insert(pending)?;
    metrics
        .written_records
        .fetch_add(written as u64, std::sync::atomic::Ordering::Relaxed);
    pending.clear();
    Ok(())
}

fn handle(storage: &mut Storage, command: Command) -> Result<bool> {
    match command {
        Command::Stats(response) => {
            let _ = response.send(storage.stats());
        }
        Command::Query { sql, response } => {
            let _ = response.send(storage.query_json(&sql));
        }
        Command::Barrier(response) => {
            let _ = response.send(Ok(()));
        }
        Command::Shutdown(response) => {
            let _ = response.send(Ok(()));
            return Ok(true);
        }
        Command::Flush => {}
        Command::Upload { uploads, response } => {
            let _ = response.send(storage.record_uploads(&uploads));
        }
        Command::Cleanup {
            cutoff,
            bucket,
            response,
        } => {
            let _ = response.send(storage.cleanup(cutoff, &bucket));
        }
        Command::Records(_) => {}
    }
    Ok(false)
}
