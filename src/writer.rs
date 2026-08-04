use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Timelike, Utc};
use tokio::sync::{mpsc, oneshot};

use crate::{
    config::Market,
    model::Record,
    parquet_store::{Segment, write_segment},
    runtime::Metrics,
};

enum Command {
    Records(Vec<Record>),
    Tick,
    Flush(oneshot::Sender<Result<()>>),
}

#[derive(Clone)]
pub struct Writer {
    spot: mpsc::Sender<Command>,
    usdm: mpsc::Sender<Command>,
    coinm: mpsc::Sender<Command>,
    metrics: Arc<Metrics>,
    directory: PathBuf,
    exchange: String,
    buffered_bytes: Arc<AtomicU64>,
    parquet_segments: Arc<AtomicU64>,
    parquet_bytes: Arc<AtomicU64>,
}

struct WorkerState {
    market: Market,
    hour: DateTime<Utc>,
    sequence: u64,
    tables: HashMap<&'static str, BufferedTable>,
    buffered_bytes: usize,
}

struct BufferedTable {
    records: Vec<Record>,
    bytes: usize,
    first_seen: Instant,
}

impl Writer {
    pub fn start(
        database: PathBuf,
        exchange: String,
        capacity: usize,
        buffer_bytes: usize,
        segment_bytes: usize,
        flush_interval: Duration,
        metrics: Arc<Metrics>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Segment>)> {
        let directory = database
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        std::fs::create_dir_all(directory.join(&exchange))
            .context("create Parquet data directory")?;
        let (segment_sender, segment_receiver) = mpsc::unbounded_channel();
        let buffered_bytes = Arc::new(AtomicU64::new(0));
        let parquet_segments = Arc::new(AtomicU64::new(0));
        let parquet_bytes = Arc::new(AtomicU64::new(0));
        let encoder_lock = Arc::new(Mutex::new(()));
        let spot = start_worker(WorkerOptions {
            market: Market::Spot,
            directory: directory.clone(),
            exchange: exchange.clone(),
            capacity,
            buffer_bytes,
            segment_bytes,
            flush_interval,
            metrics: Arc::clone(&metrics),
            buffered_bytes: Arc::clone(&buffered_bytes),
            parquet_segments: Arc::clone(&parquet_segments),
            parquet_bytes: Arc::clone(&parquet_bytes),
            segment_sender: segment_sender.clone(),
            encoder_lock: Arc::clone(&encoder_lock),
        })?;
        let usdm = start_worker(WorkerOptions {
            market: Market::Usdm,
            directory: directory.clone(),
            exchange: exchange.clone(),
            capacity,
            buffer_bytes,
            segment_bytes,
            flush_interval,
            metrics: Arc::clone(&metrics),
            buffered_bytes: Arc::clone(&buffered_bytes),
            parquet_segments: Arc::clone(&parquet_segments),
            parquet_bytes: Arc::clone(&parquet_bytes),
            segment_sender: segment_sender.clone(),
            encoder_lock: Arc::clone(&encoder_lock),
        })?;
        let coinm = start_worker(WorkerOptions {
            market: Market::Coinm,
            directory: directory.clone(),
            exchange: exchange.clone(),
            capacity,
            buffer_bytes,
            segment_bytes,
            flush_interval,
            metrics: Arc::clone(&metrics),
            buffered_bytes: Arc::clone(&buffered_bytes),
            parquet_segments: Arc::clone(&parquet_segments),
            parquet_bytes: Arc::clone(&parquet_bytes),
            segment_sender,
            encoder_lock,
        })?;
        Ok((
            Self {
                spot,
                usdm,
                coinm,
                metrics,
                directory,
                exchange,
                buffered_bytes,
                parquet_segments,
                parquet_bytes,
            },
            segment_receiver,
        ))
    }

    pub async fn records(&self, market: Market, records: Vec<Record>) -> Result<()> {
        let sender = self.sender(market);
        self.observe_queue();
        sender
            .send(Command::Records(records))
            .await
            .context("Parquet writer stopped")?;
        self.observe_queue();
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        for sender in [&self.spot, &self.usdm, &self.coinm] {
            let (response, result) = oneshot::channel();
            sender
                .send(Command::Flush(response))
                .await
                .context("Parquet writer stopped")?;
            result
                .await
                .context("Parquet writer dropped flush response")??;
        }
        Ok(())
    }

    pub async fn stats(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "format": "parquet_segments",
            "buffered_bytes": self.buffered_bytes.load(Ordering::Relaxed),
            "segments": self.parquet_segments.load(Ordering::Relaxed),
            "parquet_bytes": self.parquet_bytes.load(Ordering::Relaxed),
            "directory": self.directory.join(&self.exchange),
        }))
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    fn sender(&self, market: Market) -> &mpsc::Sender<Command> {
        match market {
            Market::Spot => &self.spot,
            Market::Usdm => &self.usdm,
            Market::Coinm => &self.coinm,
        }
    }

    fn observe_queue(&self) {
        let used = [&self.spot, &self.usdm, &self.coinm]
            .iter()
            .map(|sender| sender.max_capacity().saturating_sub(sender.capacity()))
            .sum::<usize>();
        self.metrics.observe_writer_queue(used as u64);
    }
}

struct WorkerOptions {
    market: Market,
    directory: PathBuf,
    exchange: String,
    capacity: usize,
    buffer_bytes: usize,
    segment_bytes: usize,
    flush_interval: Duration,
    metrics: Arc<Metrics>,
    buffered_bytes: Arc<AtomicU64>,
    parquet_segments: Arc<AtomicU64>,
    parquet_bytes: Arc<AtomicU64>,
    segment_sender: mpsc::UnboundedSender<Segment>,
    encoder_lock: Arc<Mutex<()>>,
}

fn start_worker(options: WorkerOptions) -> Result<mpsc::Sender<Command>> {
    let (sender, receiver) = mpsc::channel(options.capacity);
    let flush_sender = sender.clone();
    let tick_interval = options.flush_interval.min(Duration::from_secs(5));
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tick_interval);
        interval.tick().await;
        loop {
            interval.tick().await;
            if flush_sender.send(Command::Tick).await.is_err() {
                return;
            }
        }
    });
    std::thread::Builder::new()
        .name(format!("parquet-{}", options.market.as_str()))
        .spawn(move || {
            if let Err(error) = worker_run(receiver, options) {
                tracing::error!(error = %format!("{error:#}"), "Parquet writer stopped");
            }
        })
        .context("start Parquet writer thread")?;
    Ok(sender)
}

fn worker_run(mut receiver: mpsc::Receiver<Command>, options: WorkerOptions) -> Result<()> {
    let mut state = WorkerState {
        market: options.market,
        hour: floor_hour(Utc::now())?,
        sequence: u64::try_from(Utc::now().timestamp_micros()).unwrap_or_default(),
        tables: HashMap::new(),
        buffered_bytes: 0,
    };
    while let Some(command) = receiver.blocking_recv() {
        let current_hour = floor_hour(Utc::now())?;
        if current_hour > state.hour {
            flush_all(&mut state, &options)?;
            state.hour = current_hour;
        }
        match command {
            Command::Records(records) => {
                for record in records {
                    let bytes = record
                        .values
                        .iter()
                        .map(|value| value.estimated_bytes())
                        .sum::<usize>()
                        + std::mem::size_of::<Record>();
                    state.buffered_bytes = state.buffered_bytes.saturating_add(bytes);
                    options
                        .buffered_bytes
                        .fetch_add(bytes as u64, Ordering::Relaxed);
                    let table = state
                        .tables
                        .entry(record.table)
                        .or_insert_with(|| BufferedTable {
                            records: Vec::new(),
                            bytes: 0,
                            first_seen: Instant::now(),
                        });
                    table.bytes = table.bytes.saturating_add(bytes);
                    table.records.push(record);
                }
                let full_tables = state
                    .tables
                    .iter()
                    .filter(|(_, table)| table.bytes >= options.segment_bytes)
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>();
                for table in full_tables {
                    flush_table(&mut state, &options, table)?;
                }
                while state.buffered_bytes >= options.buffer_bytes {
                    flush_largest(&mut state, &options)?;
                }
            }
            Command::Tick => flush_expired(&mut state, &options)?,
            Command::Flush(response) => {
                if let Err(error) = flush_all(&mut state, &options) {
                    let _ = response.send(Err(anyhow::anyhow!(format!("{error:#}"))));
                    return Err(error);
                }
                let _ = response.send(Ok(()));
            }
        }
    }
    flush_all(&mut state, &options)
}

fn flush_largest(state: &mut WorkerState, options: &WorkerOptions) -> Result<()> {
    let table = state
        .tables
        .iter()
        .max_by_key(|(_, table)| table.bytes)
        .map(|(name, _)| *name);
    if let Some(table) = table {
        flush_table(state, options, table)?;
    }
    Ok(())
}

fn flush_expired(state: &mut WorkerState, options: &WorkerOptions) -> Result<()> {
    let expired = state
        .tables
        .iter()
        .filter(|(_, table)| table.first_seen.elapsed() >= options.flush_interval)
        .map(|(name, _)| *name)
        .collect::<Vec<_>>();
    for table in expired {
        flush_table(state, options, table)?;
    }
    Ok(())
}

fn flush_all(state: &mut WorkerState, options: &WorkerOptions) -> Result<()> {
    let tables = state.tables.keys().copied().collect::<Vec<_>>();
    for table in tables {
        flush_table(state, options, table)?;
    }
    Ok(())
}

fn flush_table(
    state: &mut WorkerState,
    options: &WorkerOptions,
    table: &'static str,
) -> Result<()> {
    let Some(buffer) = state.tables.remove(table) else {
        return Ok(());
    };
    if buffer.records.is_empty() {
        return Ok(());
    }
    state.sequence = state.sequence.wrapping_add(1);
    let _encoder = options
        .encoder_lock
        .lock()
        .map_err(|_| anyhow::anyhow!("Parquet encoder lock poisoned"))?;
    let segment = write_segment(
        &options.directory,
        &options.exchange,
        state.market,
        state.hour,
        state.sequence,
        table,
        &buffer.records,
    )?;
    state.buffered_bytes = state.buffered_bytes.saturating_sub(buffer.bytes);
    options
        .buffered_bytes
        .fetch_sub(buffer.bytes as u64, Ordering::Relaxed);
    options
        .metrics
        .written_records
        .fetch_add(segment.rows as u64, Ordering::Relaxed);
    options.parquet_segments.fetch_add(1, Ordering::Relaxed);
    options
        .parquet_bytes
        .fetch_add(segment.bytes, Ordering::Relaxed);
    let _ = options.segment_sender.send(segment);
    Ok(())
}

pub fn floor_hour(value: DateTime<Utc>) -> Result<DateTime<Utc>> {
    value
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .context("invalid UTC hour")
}
