use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    TimestampMicros(i64),
    Text(String),
    U64(u64),
    I64(i64),
    Decimal(i128),
    Boolean(bool),
}

impl Value {
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Text(value) => value.capacity(),
                Self::Null
                | Self::TimestampMicros(_)
                | Self::U64(_)
                | Self::I64(_)
                | Self::Decimal(_)
                | Self::Boolean(_) => 0,
            }
    }
}

#[derive(Debug)]
pub struct Record {
    pub table: &'static str,
    pub values: Vec<Value>,
}

pub fn timestamp(value: DateTime<Utc>) -> Value {
    Value::TimestampMicros(value.timestamp_micros())
}

pub fn text(value: impl Into<String>) -> Value {
    Value::Text(value.into())
}

pub const fn decimal(value: i128) -> Value {
    Value::Decimal(value)
}
