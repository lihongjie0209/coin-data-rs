use std::{
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};

use crate::{
    model::Record,
    runtime::Metrics,
    storage::{ExportFile, Storage, UploadRecord},
};

pub enum Command {
    Records(Vec<Record>),
    Flush,
    Barrier(oneshot::Sender<Result<()>>),
    SnapshotBarrier {
        ready: oneshot::Sender<Result<()>>,
        resume: Arc<(Mutex<bool>, Condvar)>,
    },
    Shutdown(oneshot::Sender<Result<()>>),
}

struct SnapshotResumeGuard(Arc<(Mutex<bool>, Condvar)>);

impl Drop for SnapshotResumeGuard {
    fn drop(&mut self) {
        resume_snapshot_writer(&self.0);
    }
}

enum MaintenanceCommand {
    Stats(oneshot::Sender<Result<serde_json::Value>>),
    Query {
        sql: String,
        response: oneshot::Sender<Result<serde_json::Value>>,
    },
    Upload {
        uploads: Vec<UploadRecord>,
        response: oneshot::Sender<Result<()>>,
    },
    Cleanup {
        cutoff: DateTime<Utc>,
        bucket: String,
        response: oneshot::Sender<Result<usize>>,
    },
}

#[derive(Clone)]
pub struct Writer {
    sender: mpsc::Sender<Command>,
    maintenance: mpsc::Sender<MaintenanceCommand>,
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
    ) -> Result<Self> {
        Storage::open(&database).context("initialize database")?;
        let (sender, receiver) = mpsc::channel(capacity);
        let (maintenance, maintenance_receiver) = mpsc::channel(1_024);
        let writer_database = database.clone();
        let maintenance_database = database.clone();
        let writer_metrics = Arc::clone(&metrics);
        tokio::task::spawn_blocking(move || {
            if let Err(error) = run(database, receiver, batch_size, &writer_metrics) {
                tracing::error!(%error, "database writer stopped");
            }
        });
        tokio::task::spawn_blocking(move || {
            if let Err(error) = run_maintenance(maintenance_database, maintenance_receiver) {
                tracing::error!(%error, "database maintenance worker stopped");
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
        Ok(Self {
            sender,
            maintenance,
            database: writer_database,
            metrics,
        })
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
        self.maintenance
            .send(MaintenanceCommand::Stats(response))
            .await?;
        result
            .await
            .context("database maintenance dropped response")?
    }

    pub async fn query(&self, sql: String) -> Result<serde_json::Value> {
        let (response, result) = oneshot::channel();
        self.maintenance
            .send(MaintenanceCommand::Query { sql, response })
            .await?;
        result
            .await
            .context("database maintenance dropped response")?
    }

    pub async fn export(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        directory: PathBuf,
        force: bool,
    ) -> Result<Vec<ExportFile>> {
        let database = self.database.clone();
        let (writer_ready_tx, writer_ready_rx) = oneshot::channel();
        let resume_writer = Arc::new((Mutex::new(false), Condvar::new()));
        self.sender
            .send(Command::SnapshotBarrier {
                ready: writer_ready_tx,
                resume: Arc::clone(&resume_writer),
            })
            .await
            .map_err(|_| anyhow::anyhow!("database writer stopped"))?;
        let resume_guard = SnapshotResumeGuard(resume_writer);
        writer_ready_rx
            .await
            .context("database writer dropped snapshot barrier")??;

        let snapshot = database.with_extension(format!(
            "export-{}-{}.duckdb",
            std::process::id(),
            Utc::now().timestamp_millis()
        ));
        let source = database.clone();
        let target = snapshot.clone();
        tokio::task::spawn_blocking(move || std::fs::copy(&source, &target))
            .await
            .context("DuckDB snapshot copy task failed")?
            .context("copy DuckDB export snapshot")?;
        drop(resume_guard);

        tokio::task::spawn_blocking(move || {
            let mut storage = Storage::open_existing(&snapshot)?;
            let result = storage.export_hour(start, end, &directory, force);
            drop(storage);
            if let Err(error) = std::fs::remove_file(&snapshot) {
                tracing::warn!(path = %snapshot.display(), %error, "remove export snapshot failed");
            }
            result
        })
        .await
        .context("Parquet export task failed")?
    }

    pub async fn record_uploads(&self, uploads: Vec<UploadRecord>) -> Result<()> {
        let (response, result) = oneshot::channel();
        self.maintenance
            .send(MaintenanceCommand::Upload { uploads, response })
            .await?;
        result
            .await
            .context("database maintenance dropped response")?
    }

    pub async fn cleanup(&self, cutoff: DateTime<Utc>, bucket: String) -> Result<usize> {
        let (response, result) = oneshot::channel();
        self.maintenance
            .send(MaintenanceCommand::Cleanup {
                cutoff,
                bucket,
                response,
            })
            .await?;
        result
            .await
            .context("database maintenance dropped response")?
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
    let mut storage = Storage::open_existing(&database)?;
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
                if handle(&storage, other)? {
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

fn handle(storage: &Storage, command: Command) -> Result<bool> {
    match command {
        Command::Barrier(response) => {
            let _ = response.send(Ok(()));
        }
        Command::SnapshotBarrier { ready, resume } => {
            let _ = ready.send(storage.checkpoint());
            let (lock, condition) = &*resume;
            let mut resumed = lock.lock().unwrap_or_else(|error| error.into_inner());
            while !*resumed {
                resumed = condition
                    .wait(resumed)
                    .unwrap_or_else(|error| error.into_inner());
            }
        }
        Command::Shutdown(response) => {
            let _ = response.send(Ok(()));
            return Ok(true);
        }
        Command::Flush => {}
        Command::Records(_) => {}
    }
    Ok(false)
}

fn resume_snapshot_writer(resume: &Arc<(Mutex<bool>, Condvar)>) {
    let (lock, condition) = &**resume;
    let mut resumed = lock.lock().unwrap_or_else(|error| error.into_inner());
    *resumed = true;
    condition.notify_one();
}

fn run_maintenance(
    database: PathBuf,
    mut receiver: mpsc::Receiver<MaintenanceCommand>,
) -> Result<()> {
    while let Some(command) = receiver.blocking_recv() {
        // DuckDB connections opened before an Appender commit can retain an old catalog/data
        // snapshot. Reopen for each low-frequency maintenance request so API queries, gap checks,
        // and cleanup observe the latest committed writer state without pausing ingestion.
        let mut storage = Storage::open_existing(&database)?;
        match command {
            MaintenanceCommand::Stats(response) => {
                let _ = response.send(storage.stats());
            }
            MaintenanceCommand::Query { sql, response } => {
                let _ = response.send(storage.query_json(&sql));
            }
            MaintenanceCommand::Upload { uploads, response } => {
                let _ = response.send(storage.record_uploads(&uploads));
            }
            MaintenanceCommand::Cleanup {
                cutoff,
                bucket,
                response,
            } => {
                let _ = response.send(storage.cleanup(cutoff, &bucket));
            }
        }
    }
    Ok(())
}
