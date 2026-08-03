use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use duckdb::types::{Decimal, Value as DuckValue};
use serde_json::Value;

use crate::{
    model::{Record, text, timestamp},
    parser,
};

pub fn parse(payload: &[u8], received: DateTime<Utc>, source: &str) -> Result<Vec<Record>> {
    let envelope: Value =
        serde_json::from_slice(payload).context("decode futures websocket event")?;
    let data = envelope.get("data").unwrap_or(&envelope);
    match string(data, "e") {
        "depthUpdate" => parse_depth(data, received, source),
        "aggTrade" => Ok(vec![aggregate_trade(data, received, source)]),
        "bookTicker" => Ok(vec![book_ticker(data, received, source)]),
        "markPriceUpdate" => Ok(vec![mark_price(data, received, source)]),
        "forceOrder" => Ok(vec![liquidation(data, received, source)]),
        "24hrMiniTicker" => Ok(vec![mini_ticker(data, received, source)]),
        "24hrTicker" => Ok(vec![ticker(data, received, source)]),
        "kline" => Ok(vec![kline(data, received, source)]),
        _ => parser::parse(payload, received, source),
    }
}

pub fn parse_open_interest(
    symbol: &str,
    data: &Value,
    received: DateTime<Utc>,
    source: &str,
) -> Record {
    Record {
        table: "futures_open_interest",
        values: vec![
            millis(data, "time", received),
            timestamp(received),
            text(data.get("symbol").and_then(Value::as_str).unwrap_or(symbol)),
            text(string(data, "pair")),
            text(string(data, "contractType")),
            decimal(data, "openInterest"),
            text(source),
        ],
    }
}

fn parse_depth(data: &Value, received: DateTime<Utc>, source: &str) -> Result<Vec<Record>> {
    let event_time = millis(data, "E", received);
    let transaction_time = millis(data, "T", received);
    let first = uint(data, "U");
    let final_id = uint(data, "u");
    let previous = uint(data, "pu");
    let symbol = string(data, "s");
    let pair = string(data, "ps");
    Ok(vec![Record {
        table: "futures_depth_updates",
        values: vec![
            event_time,
            transaction_time,
            timestamp(received),
            text(symbol),
            text(pair),
            first,
            final_id,
            previous,
            text(source),
            json_array(data, "b")?,
            json_array(data, "a")?,
        ],
    }])
}

fn json_array(value: &Value, key: &str) -> Result<DuckValue> {
    let array = value
        .get(key)
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    Ok(text(serde_json::to_string(array)?))
}

fn aggregate_trade(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    Record {
        table: "futures_aggregate_trades",
        values: vec![
            millis(data, "E", received),
            timestamp(received),
            text(string(data, "s")),
            text(string(data, "ps")),
            uint(data, "a"),
            decimal(data, "p"),
            decimal(data, "q"),
            uint(data, "f"),
            uint(data, "l"),
            millis(data, "T", received),
            boolean(data, "m"),
            text(source),
        ],
    }
}

fn book_ticker(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    Record {
        table: "futures_book_tickers",
        values: vec![
            millis(data, "E", received),
            millis(data, "T", received),
            timestamp(received),
            text(string(data, "s")),
            uint(data, "u"),
            decimal(data, "b"),
            decimal(data, "B"),
            decimal(data, "a"),
            decimal(data, "A"),
            text(string(data, "ps")),
            text(source),
        ],
    }
}

fn mark_price(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    Record {
        table: "futures_mark_prices",
        values: vec![
            millis(data, "E", received),
            timestamp(received),
            text(string(data, "s")),
            text(string(data, "ps")),
            decimal(data, "p"),
            decimal(data, "i"),
            decimal(data, "P"),
            decimal(data, "r"),
            millis(data, "T", received),
            text(source),
        ],
    }
}

fn liquidation(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    let order = data.get("o").unwrap_or(&Value::Null);
    Record {
        table: "futures_liquidations",
        values: vec![
            millis(data, "E", received),
            timestamp(received),
            text(string(order, "s")),
            text(string(order, "S")),
            text(string(order, "o")),
            text(string(order, "f")),
            decimal(order, "q"),
            decimal(order, "p"),
            decimal(order, "ap"),
            text(string(order, "X")),
            decimal(order, "l"),
            decimal(order, "z"),
            millis(order, "T", received),
            text(string(order, "ps")),
            text(source),
        ],
    }
}

fn mini_ticker(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    Record {
        table: "futures_mini_tickers",
        values: vec![
            millis(data, "E", received),
            timestamp(received),
            text(string(data, "s")),
            text(string(data, "ps")),
            decimal(data, "c"),
            decimal(data, "o"),
            decimal(data, "h"),
            decimal(data, "l"),
            decimal(data, "v"),
            decimal(data, "q"),
            text(source),
        ],
    }
}

fn ticker(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    Record {
        table: "futures_tickers",
        values: vec![
            millis(data, "E", received),
            timestamp(received),
            text(string(data, "s")),
            text(string(data, "ps")),
            decimal(data, "p"),
            decimal(data, "P"),
            decimal(data, "w"),
            decimal(data, "c"),
            decimal(data, "Q"),
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
    }
}

fn kline(data: &Value, received: DateTime<Utc>, source: &str) -> Record {
    let value = data.get("k").unwrap_or(&Value::Null);
    Record {
        table: "futures_klines",
        values: vec![
            millis(data, "E", received),
            timestamp(received),
            text(string(data, "s")),
            text(string(value, "ps")),
            millis(value, "t", received),
            millis(value, "T", received),
            text(string(value, "i")),
            integer(value, "f"),
            integer(value, "L"),
            decimal(value, "o"),
            decimal(value, "c"),
            decimal(value, "h"),
            decimal(value, "l"),
            decimal(value, "v"),
            uint(value, "n"),
            boolean(value, "x"),
            decimal(value, "q"),
            decimal(value, "V"),
            decimal(value, "Q"),
            decimal(value, "B"),
            text(source),
        ],
    }
}

fn string<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn decimal(value: &Value, key: &str) -> DuckValue {
    decimal_value(string(value, key))
}

fn decimal_value(raw: &str) -> DuckValue {
    if raw.is_empty() {
        return DuckValue::Null;
    }
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
    let Some(scaled) = digits.parse::<i128>().ok() else {
        return DuckValue::Null;
    };
    Decimal::new(38, 18, if negative { -scaled } else { scaled })
        .map_or(DuckValue::Null, DuckValue::Decimal)
}

fn uint(value: &Value, key: &str) -> DuckValue {
    let number = value
        .get(key)
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default();
    DuckValue::UBigInt(number)
}

fn integer(value: &Value, key: &str) -> DuckValue {
    let number = value
        .get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default();
    DuckValue::BigInt(number)
}

fn boolean(value: &Value, key: &str) -> DuckValue {
    DuckValue::Boolean(value.get(key).and_then(Value::as_bool).unwrap_or(false))
}

fn millis(value: &Value, key: &str, fallback: DateTime<Utc>) -> DuckValue {
    let raw = value
        .get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()));
    timestamp(
        raw.and_then(|value| Utc.timestamp_millis_opt(value).single())
            .unwrap_or(fallback),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn futures_depth_should_preserve_previous_update_id() -> Result<()> {
        let payload = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1785731400000,"T":1785731400001,"s":"BTCUSDT","U":10,"u":12,"pu":9,"b":[["100","1"]],"a":[["101","2"]]}}"#;
        let records = parse(payload, Utc::now(), "binance_usdm_websocket")?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].table, "futures_depth_updates");
        assert_eq!(records[0].values.len(), 11);
        Ok(())
    }

    #[test]
    fn liquidation_should_map_all_order_fields() -> Result<()> {
        let payload = br#"{"stream":"btcusdt@forceOrder","data":{"e":"forceOrder","E":1785731400000,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"1","p":"100","ap":"99","X":"FILLED","l":"1","z":"1","T":1785731400001}}}"#;
        let records = parse(payload, Utc::now(), "binance_usdm_websocket")?;
        assert_eq!(records.len(), 1);
        Ok(())
    }
}
