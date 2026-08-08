use chrono::{DateTime, Utc};
use compact_str::CompactString;

use crate::config::Market;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    TimestampMicros(i64),
    Text(CompactString),
    StaticText(&'static str),
    U64(u64),
    I64(i64),
    Decimal(i128),
    Boolean(bool),
}

impl Value {
    pub fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::Text(value) if value.is_heap_allocated() => value.capacity(),
                Self::Text(_) => 0,
                Self::StaticText(value) => value.len(),
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
    pub target_market: Option<Market>,
}

pub fn timestamp(value: DateTime<Utc>) -> Value {
    Value::TimestampMicros(value.timestamp_micros())
}

pub fn text(value: impl AsRef<str>) -> Value {
    Value::Text(CompactString::new(value.as_ref()))
}

pub const fn static_text(value: &'static str) -> Value {
    Value::StaticText(value)
}

pub const fn decimal(value: i128) -> Value {
    Value::Decimal(value)
}

pub fn parse_decimal(raw: &str) -> Option<i128> {
    if raw.is_empty() {
        return None;
    }
    let (negative, unsigned) = raw
        .strip_prefix('-')
        .map_or((false, raw), |value| (true, value));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }

    let mut scaled = 0i128;
    for digit in whole.bytes() {
        if !digit.is_ascii_digit() {
            return None;
        }
        scaled = scaled
            .checked_mul(10)?
            .checked_add(i128::from(digit - b'0'))?;
    }
    for digit in fraction.bytes().take(18) {
        if !digit.is_ascii_digit() {
            return None;
        }
        scaled = scaled
            .checked_mul(10)?
            .checked_add(i128::from(digit - b'0'))?;
    }
    for _ in fraction.len().min(18)..18 {
        scaled = scaled.checked_mul(10)?;
    }
    if negative {
        scaled.checked_neg()
    } else {
        Some(scaled)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_decimal;

    #[test]
    fn parse_decimal_should_scale_without_rounding() {
        assert_eq!(parse_decimal("-12.34"), Some(-12_340_000_000_000_000_000));
    }

    #[test]
    fn parse_decimal_should_reject_invalid_input() {
        assert_eq!(parse_decimal("12x"), None);
    }
}
