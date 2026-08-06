use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, value::RawValue};

use crate::{
    binance_json::{self, Event},
    model::{
        Record, Value as DataValue, decimal as data_decimal, parse_decimal, static_text, text,
        timestamp,
    },
    parser,
};

pub fn parse(payload: &[u8], received: DateTime<Utc>, source: &'static str) -> Result<Vec<Record>> {
    let (_, events) = binance_json::decode(payload)?;
    let mut records = Vec::with_capacity(events.len());
    for data in events {
        records.extend(parse_event(&data, received, source)?);
    }
    if records.is_empty() {
        return parser::parse(payload, received, source);
    }
    Ok(records)
}

fn parse_event(
    data: &Event<'_>,
    received: DateTime<Utc>,
    source: &'static str,
) -> Result<Vec<Record>> {
    match binance_json::string(data.e) {
        "depthUpdate" => Ok(vec![parse_depth(data, received, source)]),
        "aggTrade" => Ok(vec![aggregate_trade(data, received, source)]),
        "bookTicker" => Ok(vec![book_ticker(data, received, source)]),
        "markPriceUpdate" => Ok(vec![mark_price(data, received, source)]),
        "forceOrder" => Ok(vec![liquidation(data, received, source)?]),
        "24hrMiniTicker" => Ok(vec![mini_ticker(data, received, source)]),
        "24hrTicker" => Ok(vec![ticker(data, received, source)]),
        "kline" => Ok(vec![kline(data, received, source)?]),
        _ => Ok(Vec::new()),
    }
}

pub fn parse_open_interest(
    symbol: &str,
    data: &Value,
    received: DateTime<Utc>,
    source: &'static str,
) -> Record {
    Record {
        table: "futures_open_interest",
        values: vec![
            rest_millis(data, "time", received),
            timestamp(received),
            text(data.get("symbol").and_then(Value::as_str).unwrap_or(symbol)),
            text(rest_string(data, "pair")),
            text(rest_string(data, "contractType")),
            decimal_value(rest_string(data, "openInterest")),
            static_text(source),
        ],
    }
}

fn parse_depth(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Record {
    Record {
        table: "futures_depth_updates",
        values: vec![
            millis(data.event_time, received),
            millis(data.transaction_time, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            text(binance_json::string(data.ps)),
            uint(data.first_update),
            uint(data.u),
            uint(data.pu),
            static_text(source),
            text(binance_json::json(data.b)),
            text(binance_json::json(data.a)),
        ],
    }
}

fn aggregate_trade(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Record {
    Record {
        table: "futures_aggregate_trades",
        values: vec![
            millis(data.event_time, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            text(binance_json::string(data.ps)),
            uint(data.a),
            decimal(data.p),
            decimal(data.q),
            uint(data.f),
            uint(data.l),
            millis(data.transaction_time, received),
            boolean(data.m),
            static_text(source),
        ],
    }
}

fn book_ticker(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Record {
    Record {
        table: "futures_book_tickers",
        values: vec![
            millis(data.event_time, received),
            millis(data.transaction_time, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            uint(data.u),
            decimal(data.b),
            decimal(data.upper_b),
            decimal(data.a),
            decimal(data.upper_a),
            text(binance_json::string(data.ps)),
            static_text(source),
        ],
    }
}

fn mark_price(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Record {
    Record {
        table: "futures_mark_prices",
        values: vec![
            millis(data.event_time, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            text(binance_json::string(data.ps)),
            decimal(data.p),
            decimal(data.i),
            decimal(data.upper_p),
            decimal(data.r),
            millis(data.transaction_time, received),
            static_text(source),
        ],
    }
}

fn liquidation(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Result<Record> {
    let order = binance_json::decode_nested(data.o)?;
    Ok(Record {
        table: "futures_liquidations",
        values: vec![
            millis(data.event_time, received),
            timestamp(received),
            text(binance_json::string(order.s)),
            text(binance_json::string(order.side)),
            text(binance_json::string(order.o)),
            text(binance_json::string(order.f)),
            decimal(order.q),
            decimal(order.p),
            decimal(order.ap),
            text(binance_json::string(order.status)),
            decimal(order.l),
            decimal(order.z),
            millis(order.transaction_time, received),
            text(binance_json::string(order.ps)),
            static_text(source),
        ],
    })
}

fn mini_ticker(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Record {
    Record {
        table: "futures_mini_tickers",
        values: vec![
            millis(data.event_time, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            text(binance_json::string(data.ps)),
            decimal(data.c),
            decimal(data.o),
            decimal(data.h),
            decimal(data.l),
            decimal(data.v),
            decimal(data.q),
            static_text(source),
        ],
    }
}

fn ticker(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Record {
    Record {
        table: "futures_tickers",
        values: vec![
            millis(data.event_time, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            text(binance_json::string(data.ps)),
            decimal(data.p),
            decimal(data.upper_p),
            decimal(data.w),
            decimal(data.c),
            decimal(data.upper_q),
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
    }
}

fn kline(data: &Event<'_>, received: DateTime<Utc>, source: &'static str) -> Result<Record> {
    let value = binance_json::decode_nested(data.k)?;
    Ok(Record {
        table: "futures_klines",
        values: vec![
            millis(data.event_time, received),
            timestamp(received),
            text(binance_json::string(data.s)),
            text(binance_json::string(value.ps)),
            millis(value.t, received),
            millis(value.transaction_time, received),
            text(binance_json::string(value.i)),
            integer(value.f),
            integer(value.upper_l),
            decimal(value.o),
            decimal(value.c),
            decimal(value.h),
            decimal(value.l),
            decimal(value.v),
            uint(value.n),
            boolean(value.x),
            decimal(value.q),
            decimal(value.upper_v),
            decimal(value.upper_q),
            decimal(value.upper_b),
            static_text(source),
        ],
    })
}

fn decimal(raw: Option<&RawValue>) -> DataValue {
    decimal_value(binance_json::string(raw))
}

fn decimal_value(raw: &str) -> DataValue {
    let Some(scaled) = parse_decimal(raw) else {
        return DataValue::Null;
    };
    data_decimal(scaled)
}

fn uint(raw: Option<&RawValue>) -> DataValue {
    DataValue::U64(binance_json::u64(raw))
}

fn integer(raw: Option<&RawValue>) -> DataValue {
    DataValue::I64(binance_json::i64(raw))
}

fn boolean(raw: Option<&RawValue>) -> DataValue {
    DataValue::Boolean(binance_json::boolean(raw, false))
}

fn millis(raw: Option<&RawValue>, fallback: DateTime<Utc>) -> DataValue {
    let value = binance_json::i64(raw);
    timestamp(Utc.timestamp_millis_opt(value).single().unwrap_or(fallback))
}

fn rest_string<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or_default()
}

fn rest_millis(value: &Value, key: &str, fallback: DateTime<Utc>) -> DataValue {
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
