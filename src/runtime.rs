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
    pub writer_queue_depth: AtomicU64,
    pub writer_queue_high_watermark: AtomicU64,
    pub last_message_unix_ms: AtomicU64,
}

#[derive(Debug, Serialize)]
pub struct MetricsSnapshot {
    pub received_messages: u64,
    pub parsed_records: u64,
    pub written_records: u64,
    pub parse_errors: u64,
    pub reconnects: u64,
    pub dropped_messages: u64,
    pub writer_queue_depth: u64,
    pub writer_queue_high_watermark: u64,
    pub last_message_unix_ms: u64,
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
            writer_queue_depth: self.writer_queue_depth.load(Ordering::Relaxed),
            writer_queue_high_watermark: self.writer_queue_high_watermark.load(Ordering::Relaxed),
            last_message_unix_ms: self.last_message_unix_ms.load(Ordering::Relaxed),
        }
    }

    pub fn observe_writer_queue(&self, depth: u64) {
        self.writer_queue_depth.store(depth, Ordering::Relaxed);
        self.writer_queue_high_watermark
            .fetch_max(depth, Ordering::Relaxed);
    }
}
