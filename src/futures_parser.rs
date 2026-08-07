use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Value, value::RawValue};

use crate::{
    binance_json::{self, Event},
    config::Market,
    model::{
        Record, Value as DataValue, decimal as data_decimal, parse_decimal, static_text, text,
        timestamp,
    },
    parser,
};

pub fn parse(payload: &[u8], received: DateTime<Utc>, source: &'static str) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    parse_into(payload, received, source, &mut records)?;
    Ok(records)
}

pub fn parse_into(
    payload: &[u8],
    received: DateTime<Utc>,
    source: &'static str,
    records: &mut Vec<Record>,
) -> Result<()> {
    let (_, events) = binance_json::decode(payload)?;
    records.reserve(events.len());
    let initial_len = records.len();
    for data in events {
        if let Some(record) = parse_event(&data, received, source)? {
            records.push(record);
        }
    }
    if records.len() == initial_len {
        parser::parse_into(payload, received, source, records)?;
    }
    Ok(())
}

fn parse_event(
    data: &Event<'_>,
    received: DateTime<Utc>,
    source: &'static str,
) -> Result<Option<Record>> {
    match binance_json::string(data.e) {
        "depthUpdate" => Ok(Some(parse_depth(data, received, source))),
        "aggTrade" => Ok(Some(aggregate_trade(data, received, source))),
        "bookTicker" => Ok(Some(book_ticker(data, received, source))),
        "markPriceUpdate" => Ok(Some(mark_price(data, received, source))),
        "forceOrder" => Ok(Some(liquidation(data, received, source)?)),
        "24hrMiniTicker" => Ok(Some(mini_ticker(data, received, source))),
        "24hrTicker" => Ok(Some(ticker(data, received, source))),
        "kline" => Ok(Some(kline(data, received, source)?)),
        _ => Ok(None),
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
        target_market: None,
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
            text(binance_json::json(data.b)),
            text(binance_json::json(data.a)),
            optional_integer(data.st),
            static_text(source),
        ],
        target_market: target_market(data),
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
            decimal(data.nq),
            uint(data.f),
            uint(data.l),
            millis(data.transaction_time, received),
            boolean(data.m),
            optional_integer(data.st),
            static_text(source),
        ],
        target_market: target_market(data),
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
            optional_integer(data.st),
            static_text(source),
        ],
        target_market: target_market(data),
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
            optional_integer(data.st),
            static_text(source),
        ],
        target_market: target_market(data),
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
            optional_integer(data.st.or(order.st)),
            static_text(source),
        ],
        target_market: target_market(data).or_else(|| target_market(&order)),
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
            optional_integer(data.st),
            static_text(source),
        ],
        target_market: target_market(data),
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
            optional_integer(data.st),
            static_text(source),
        ],
        target_market: target_market(data),
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
            optional_integer(data.st.or(value.st)),
            static_text(source),
        ],
        target_market: target_market(data).or_else(|| target_market(&value)),
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

fn optional_integer(raw: Option<&RawValue>) -> DataValue {
    raw.map_or(DataValue::Null, |value| {
        DataValue::I64(binance_json::i64(Some(value)))
    })
}

fn target_market(data: &Event<'_>) -> Option<Market> {
    match binance_json::i64(data.st) {
        1 => Some(Market::Usdm),
        2 => Some(Market::Coinm),
        _ => None,
    }
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
        let payload = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1785731400000,"T":1785731400001,"s":"BTCUSDT","U":10,"u":12,"pu":9,"b":[["100","1"]],"a":[["101","2"]],"st":2}}"#;
        let records = parse(payload, Utc::now(), "binance_usdm_websocket")?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].table, "futures_depth_updates");
        assert_eq!(records[0].values.len(), 12);
        assert_eq!(records[0].target_market, Some(Market::Coinm));
        Ok(())
    }

    #[test]
    fn aggregate_trade_should_preserve_normal_quantity_and_symbol_type() -> Result<()> {
        let payload = br#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1785731400000,"s":"BTCUSDT","a":1,"p":"100","q":"2","nq":"1.5","f":1,"l":2,"T":1785731400001,"m":true,"st":1}}"#;
        let records = parse(payload, Utc::now(), "binance_usdm_websocket")?;

        assert_eq!(records[0].values.len(), 14);
        assert_eq!(records[0].target_market, Some(Market::Usdm));
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
