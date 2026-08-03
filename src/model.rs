use chrono::{DateTime, Utc};
use duckdb::types::Value;

#[derive(Debug)]
pub struct Record {
    pub table: &'static str,
    pub values: Vec<Value>,
}

pub fn timestamp(value: DateTime<Utc>) -> Value {
    Value::Timestamp(
        duckdb::types::TimeUnit::Microsecond,
        value.timestamp_micros(),
    )
}

pub fn text(value: impl Into<String>) -> Value {
    Value::Text(value.into())
}
