use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

#[derive(Debug, Default)]
pub struct Metrics {
    pub received_messages: AtomicU64,
    pub parsed_records: AtomicU64,
    pub written_records: AtomicU64,
    pub parse_errors: AtomicU64,
    pub reconnects: AtomicU64,
    pub dropped_messages: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct MetricsSnapshot {
    received_messages: u64,
    parsed_records: u64,
    written_records: u64,
    parse_errors: u64,
    reconnects: u64,
    dropped_messages: u64,
}

impl Metrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            received_messages: self.received_messages.load(Ordering::Relaxed),
            parsed_records: self.parsed_records.load(Ordering::Relaxed),
            written_records: self.written_records.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            dropped_messages: self.dropped_messages.load(Ordering::Relaxed),
        }
    }
}
