use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};

use crate::{
    binance_json::{self, Event},
    model::{
        Record, Value as DataValue, decimal as decimal_value, parse_decimal, static_text, text,
        timestamp,
    },
};

pub fn parse(payload: &[u8], received: DateTime<Utc>, source: &'static str) -> Result<Vec<Record>> {
    let (stream, events) = binance_json::decode(payload)?;
    let mut records = Vec::with_capacity(events.len());
    for data in events {
        records.extend(parse_event(stream, &data, received, source)?);
    }
    Ok(records)
}

fn parse_event(
    stream: &str,
    data: &Event<'_>,
    received: DateTime<Utc>,
    source: &'static str,
) -> Result<Vec<Record>> {
    match binance_json::string(data.e) {
        "depthUpdate" => Ok(vec![parse_depth(data, received, source)]),
        "aggTrade" => Ok(vec![record(
            "aggregate_trades",
            vec![
                event_time(data, received),
                timestamp(received),
                text(binance_json::string(data.s)),
                uint(data.a),
                decimal(data.p),
                decimal(data.q),
                uint(data.f),
                uint(data.l),
                millis(data.transaction_time, received),
                boolean(data.m, false),
                boolean(data.upper_m, true),
                static_text(source),
            ],
        )]),
        "trade" => Ok(vec![record(
            "trades",
            vec![
                event_time(data, received),
                timestamp(received),
                text(binance_json::string(data.s)),
                uint(data.t),
                decimal(data.p),
                decimal(data.q),
                millis(data.transaction_time, received),
                boolean(data.m, false),
                boolean(data.upper_m, true),
                static_text(source),
            ],
        )]),
        "kline" => Ok(vec![parse_kline(data, received, source)?]),
        "24hrMiniTicker" => Ok(vec![record(
            "mini_tickers",
            vec![
                event_time(data, received),
                timestamp(received),
                text(binance_json::string(data.s)),
                decimal(data.c),
                decimal(data.o),
                decimal(data.h),
                decimal(data.l),
                decimal(data.v),
                decimal(data.q),
                static_text(source),
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
                text(binance_json::string(data.s)),
                text(binance_json::string(data.i)),
                decimal(data.w),
                millis(data.transaction_time, received),
                static_text(source),
            ],
        )]),
        "" if stream.contains("@bookTicker") => Ok(vec![record(
            "book_tickers",
            vec![
                timestamp(received),
                text(binance_json::string(data.s)),
                uint(data.u),
                decimal(data.b),
                decimal(data.upper_b),
                decimal(data.a),
                decimal(data.upper_a),
                static_text(source),
            ],
        )]),
        _ => Ok(Vec::new()),
    }
}

fn parse_depth(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Record {
    record(
        "depth_updates",
        vec![
            event_time(data, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            uint(data.first_update),
            uint(data.u),
            static_text(source),
            text(binance_json::json(data.b)),
            text(binance_json::json(data.a)),
        ],
    )
}

fn parse_ticker(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Record {
    record(
        "tickers",
        vec![
            event_time(data, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            decimal(data.p),
            decimal(data.upper_p),
            decimal(data.w),
            decimal(data.x),
            decimal(data.c),
            decimal(data.upper_q),
            decimal(data.b),
            decimal(data.upper_b),
            decimal(data.a),
            decimal(data.upper_a),
            decimal(data.o),
            decimal(data.h),
            decimal(data.l),
            decimal(data.v),
            decimal(data.q),
            millis(data.open_time, received),
            millis(data.close_time, received),
            integer(data.upper_f),
            integer(data.upper_l),
            uint(data.n),
            static_text(source),
        ],
    )
}

fn parse_rolling_ticker(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Record {
    let window = binance_json::string(data.e).trim_end_matches("Ticker");
    record(
        "rolling_tickers",
        vec![
            event_time(data, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            text(window),
            decimal(data.p),
            decimal(data.upper_p),
            decimal(data.o),
            decimal(data.h),
            decimal(data.l),
            decimal(data.c),
            decimal(data.w),
            decimal(data.v),
            decimal(data.q),
            millis(data.open_time, received),
            millis(data.close_time, received),
            integer(data.upper_f),
            integer(data.upper_l),
            uint(data.n),
            static_text(source),
        ],
    )
}

fn parse_kline(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Result<Record> {
    let kline = binance_json::decode_nested(data.k)?;
    Ok(record(
        "klines",
        vec![
            event_time(data, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            millis(kline.t, received),
            millis(kline.transaction_time, received),
            text(binance_json::string(kline.s)),
            text(binance_json::string(kline.i)),
            integer(kline.f),
            integer(kline.upper_l),
            decimal(kline.o),
            decimal(kline.c),
            decimal(kline.h),
            decimal(kline.l),
            decimal(kline.v),
            uint(kline.n),
            boolean(kline.x, false),
            decimal(kline.q),
            decimal(kline.upper_v),
            decimal(kline.upper_q),
            decimal(kline.upper_b),
            static_text(source),
        ],
    ))
}

fn record(table: &'static str, values: Vec<DataValue>) -> Record {
    Record { table, values }
}

fn decimal(raw: Option<&serde_json::value::RawValue>) -> DataValue {
    let raw = binance_json::string(raw);
    decimal_value(parse_decimal(raw).unwrap_or_default())
}

fn uint(raw: Option<&serde_json::value::RawValue>) -> DataValue {
    DataValue::U64(binance_json::u64(raw))
}

fn integer(raw: Option<&serde_json::value::RawValue>) -> DataValue {
    DataValue::I64(binance_json::i64(raw))
}

fn boolean(raw: Option<&serde_json::value::RawValue>, default: bool) -> DataValue {
    DataValue::Boolean(binance_json::boolean(raw, default))
}

fn event_time(value: &Event<'_>, fallback: DateTime<Utc>) -> DataValue {
    millis(value.event_time, fallback)
}

fn millis(raw: Option<&serde_json::value::RawValue>, fallback: DateTime<Utc>) -> DataValue {
    let milliseconds = binance_json::i64(raw);
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
    fn depth_should_store_levels_as_structured_arrays() -> Result<()> {
        let payload = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1767225600000,"s":"BTCUSDT","U":1,"u":2,"b":[["100","1"]],"a":[["101","2"]]}}"#;
        let records = parse(payload, Utc::now(), "websocket")?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].table, "depth_updates");
        assert_eq!(records[0].values.len(), 8);
        Ok(())
    }
}
