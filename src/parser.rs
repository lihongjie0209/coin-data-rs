use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use duckdb::types::{Decimal, Value as DuckValue};
use serde_json::Value;

use crate::model::{Record, text, timestamp};

pub fn parse(payload: &[u8], received: DateTime<Utc>, source: &str) -> Result<Vec<Record>> {
    let envelope: Value = serde_json::from_slice(payload).context("decode websocket event")?;
    let stream = envelope
        .get("stream")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let data = envelope.get("data").unwrap_or(&envelope);
    let event = string(data, "e");
    match event {
        "depthUpdate" => parse_depth(data, received, source),
        "aggTrade" => Ok(vec![record(
            "aggregate_trades",
            vec![
                event_time(data, received),
                timestamp(received),
                text(string(data, "s")),
                uint(data, "a"),
                decimal(data, "p"),
                decimal(data, "q"),
                uint(data, "f"),
                uint(data, "l"),
                millis(data, "T", received),
                boolean(data, "m"),
                optional_boolean(data, "M", true),
                text(source),
            ],
        )]),
        "trade" => Ok(vec![record(
            "trades",
            vec![
                event_time(data, received),
                timestamp(received),
                text(string(data, "s")),
                uint(data, "t"),
                decimal(data, "p"),
                decimal(data, "q"),
                millis(data, "T", received),
                boolean(data, "m"),
                optional_boolean(data, "M", true),
                text(source),
            ],
        )]),
        "kline" => Ok(vec![parse_kline(data, received, source)]),
        "24hrMiniTicker" => Ok(vec![record(
            "mini_tickers",
            vec![
                event_time(data, received),
                timestamp(received),
                text(string(data, "s")),
                decimal(data, "c"),
                decimal(data, "o"),
                decimal(data, "h"),
                decimal(data, "l"),
                decimal(data, "v"),
                decimal(data, "q"),
                text(source),
            ],
        )]),
        "24hrTicker" => Ok(vec![parse_ticker(data, received, source)]),
        "1hTicker" | "4hTicker" | "1dTicker" => {
            Ok(vec![parse_rolling_ticker(data, received, source)])
        }
        "avgPrice" => Ok(vec![record(
            "average_prices",
            vec![
                event_time(data, received),
                timestamp(received),
                text(string(data, "s")),
                text(string(data, "i")),
                decimal(data, "w"),
                millis(data, "T", received),
                text(source),
            ],
        )]),
        "" if stream.contains("@bookTicker") => Ok(vec![record(
            "book_tickers",
            vec![
                timestamp(received),
                text(string(data, "s")),
                uint(data, "u"),
                decimal(data, "b"),
                decimal(data, "B"),
                decimal(data, "a"),
                decimal(data, "A"),
                text(source),
            ],
        )]),
        _ => Ok(Vec::new()),
    }
}

pub fn parse_rest_aggregate_trade(symbol: &str, data: &Value, received: DateTime<Utc>) -> Record {
    record(
        "aggregate_trades",
        vec![
            millis(data, "T", received),
            timestamp(received),
            text(symbol),
            uint(data, "a"),
            decimal(data, "p"),
            decimal(data, "q"),
            uint(data, "f"),
            uint(data, "l"),
            millis(data, "T", received),
            boolean(data, "m"),
            optional_boolean(data, "M", true),
            text("rest_backfill"),
        ],
    )
}

fn parse_depth(data: &Value, received: DateTime<Utc>, source: &str) -> Result<Vec<Record>> {
    let event_at = event_time(data, received);
    let symbol = string(data, "s");
    let first = uint(data, "U");
    let final_id = uint(data, "u");
    let mut records = vec![record(
        "depth_updates",
        vec![
            event_at.clone(),
            timestamp(received),
            text(symbol),
            first.clone(),
            final_id.clone(),
            text(source),
        ],
    )];
    for (side, key) in [("bid", "b"), ("ask", "a")] {
        for (index, level) in data
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let values = level.as_array().context("depth level must be an array")?;
            records.push(record(
                "depth_levels",
                vec![
                    event_at.clone(),
                    timestamp(received),
                    text(symbol),
                    first.clone(),
                    final_id.clone(),
                    text(side),
                    DuckValue::Int((index + 1) as i32),
                    text(values.first().and_then(Value::as_str).unwrap_or_default()),
                    text(values.get(1).and_then(Value::as_str).unwrap_or_default()),
                    text(source),
                ],
            ));
        }
    }
    Ok(records)
}

fn parse_ticker(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    record(
        "tickers",
        vec![
            event_time(data, received),
            timestamp(received),
            text(string(data, "s")),
            decimal(data, "p"),
            decimal(data, "P"),
            decimal(data, "w"),
            decimal(data, "x"),
            decimal(data, "c"),
            decimal(data, "Q"),
            decimal(data, "b"),
            decimal(data, "B"),
            decimal(data, "a"),
            decimal(data, "A"),
            decimal(data, "o"),
            decimal(data, "h"),
            decimal(data, "l"),
            decimal(data, "v"),
            decimal(data, "q"),
            millis(data, "O", received),
            millis(data, "C", received),
            integer(data, "F"),
            integer(data, "L"),
            uint(data, "n"),
            text(source),
        ],
    )
}

fn parse_rolling_ticker(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    let window = string(data, "e").trim_end_matches("Ticker");
    record(
        "rolling_tickers",
        vec![
            event_time(data, received),
            timestamp(received),
            text(string(data, "s")),
            text(window),
            decimal(data, "p"),
            decimal(data, "P"),
            decimal(data, "o"),
            decimal(data, "h"),
            decimal(data, "l"),
            decimal(data, "c"),
            decimal(data, "w"),
            decimal(data, "v"),
            decimal(data, "q"),
            millis(data, "O", received),
            millis(data, "C", received),
            integer(data, "F"),
            integer(data, "L"),
            uint(data, "n"),
            text(source),
        ],
    )
}

fn parse_kline(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    let kline = data.get("k").unwrap_or(&Value::Null);
    record(
        "klines",
        vec![
            event_time(data, received),
            timestamp(received),
            text(string(data, "s")),
            millis(kline, "t", received),
            millis(kline, "T", received),
            text(string(kline, "s")),
            text(string(kline, "i")),
            integer(kline, "f"),
            integer(kline, "L"),
            decimal(kline, "o"),
            decimal(kline, "c"),
            decimal(kline, "h"),
            decimal(kline, "l"),
            decimal(kline, "v"),
            uint(kline, "n"),
            boolean(kline, "x"),
            decimal(kline, "q"),
            decimal(kline, "V"),
            decimal(kline, "Q"),
            decimal(kline, "B"),
            text(source),
        ],
    )
}

fn record(table: &'static str, values: Vec<DuckValue>) -> Record {
    Record { table, values }
}
fn string<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}
fn decimal(value: &Value, key: &str) -> DuckValue {
    let raw = string(value, key);
    let (negative, unsigned) = raw
        .strip_prefix('-')
        .map_or((false, raw), |value| (true, value));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(whole.len() + 18);
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    digits.extend(fraction.chars().take(18));
    digits.extend(std::iter::repeat_n(
        '0',
        18usize.saturating_sub(fraction.len()),
    ));
    let scaled = digits
        .parse::<i128>()
        .ok()
        .map(|value| if negative { -value } else { value })
        .unwrap_or_default();
    Decimal::new(38, 18, scaled).map_or(DuckValue::Null, DuckValue::Decimal)
}
fn uint(value: &Value, key: &str) -> DuckValue {
    DuckValue::UBigInt(value.get(key).and_then(Value::as_u64).unwrap_or_default())
}
fn integer(value: &Value, key: &str) -> DuckValue {
    DuckValue::BigInt(value.get(key).and_then(Value::as_i64).unwrap_or_default())
}
fn boolean(value: &Value, key: &str) -> DuckValue {
    DuckValue::Boolean(value.get(key).and_then(Value::as_bool).unwrap_or_default())
}
fn optional_boolean(value: &Value, key: &str, default: bool) -> DuckValue {
    DuckValue::Boolean(value.get(key).and_then(Value::as_bool).unwrap_or(default))
}
fn event_time(value: &Value, fallback: DateTime<Utc>) -> DuckValue {
    millis(value, "E", fallback)
}
fn millis(value: &Value, key: &str, fallback: DateTime<Utc>) -> DuckValue {
    let milliseconds = value.get(key).and_then(Value::as_i64).unwrap_or_default();
    let value = if milliseconds == 0 {
        fallback
    } else {
        Utc.timestamp_millis_opt(milliseconds)
            .single()
            .unwrap_or(fallback)
    };
    timestamp(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_should_expand_levels_without_raw_event() -> Result<()> {
        let payload = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1767225600000,"s":"BTCUSDT","U":1,"u":2,"b":[["100","1"]],"a":[["101","2"]]}}"#;
        let records = parse(payload, Utc::now(), "websocket")?;
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].table, "depth_updates");
        assert_eq!(records[1].table, "depth_levels");
        Ok(())
    }
}
