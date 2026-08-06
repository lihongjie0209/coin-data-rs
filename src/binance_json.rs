use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(default, borrow)]
    stream: &'a str,
    #[serde(default, borrow)]
    data: Option<&'a RawValue>,
}

#[derive(Deserialize)]
pub(crate) struct Event<'a> {
    #[serde(default, borrow)]
    pub e: Option<&'a RawValue>,
    #[serde(default, rename = "E", borrow)]
    pub event_time: Option<&'a RawValue>,
    #[serde(default, rename = "T", borrow)]
    pub transaction_time: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub s: Option<&'a RawValue>,
    #[serde(default, rename = "S", borrow)]
    pub side: Option<&'a RawValue>,
    #[serde(default, rename = "U", borrow)]
    pub first_update: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub u: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub pu: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub a: Option<&'a RawValue>,
    #[serde(default, rename = "A", borrow)]
    pub upper_a: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub b: Option<&'a RawValue>,
    #[serde(default, rename = "B", borrow)]
    pub upper_b: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub p: Option<&'a RawValue>,
    #[serde(default, rename = "P", borrow)]
    pub upper_p: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub q: Option<&'a RawValue>,
    #[serde(default, rename = "Q", borrow)]
    pub upper_q: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub f: Option<&'a RawValue>,
    #[serde(default, rename = "F", borrow)]
    pub upper_f: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub l: Option<&'a RawValue>,
    #[serde(default, rename = "L", borrow)]
    pub upper_l: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub m: Option<&'a RawValue>,
    #[serde(default, rename = "M", borrow)]
    pub upper_m: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub t: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub c: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub o: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub h: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub v: Option<&'a RawValue>,
    #[serde(default, rename = "V", borrow)]
    pub upper_v: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub w: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub i: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub x: Option<&'a RawValue>,
    #[serde(default, rename = "X", borrow)]
    pub status: Option<&'a RawValue>,
    #[serde(default, rename = "O", borrow)]
    pub open_time: Option<&'a RawValue>,
    #[serde(default, rename = "C", borrow)]
    pub close_time: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub n: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub ps: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub r: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub ap: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub z: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub k: Option<&'a RawValue>,
}

pub(crate) fn decode(payload: &[u8]) -> Result<(&str, Vec<Event<'_>>)> {
    let envelope: Envelope<'_> =
        serde_json::from_slice(payload).context("decode websocket envelope")?;
    let event_payload = envelope.data.map_or(payload, |data| data.get().as_bytes());
    let events = if event_payload.first() == Some(&b'[') {
        serde_json::from_slice(event_payload).context("decode websocket event array")?
    } else {
        vec![serde_json::from_slice(event_payload).context("decode websocket event")?]
    };
    Ok((envelope.stream, events))
}

pub(crate) fn decode_nested(raw: Option<&RawValue>) -> Result<Event<'_>> {
    serde_json::from_str(raw.map_or("{}", RawValue::get)).context("decode nested websocket event")
}

pub(crate) fn string(raw: Option<&RawValue>) -> &str {
    let value = raw.map_or("", RawValue::get);
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or("")
}

pub(crate) fn u64(raw: Option<&RawValue>) -> u64 {
    number(raw).parse().unwrap_or_default()
}

pub(crate) fn i64(raw: Option<&RawValue>) -> i64 {
    number(raw).parse().unwrap_or_default()
}

pub(crate) fn boolean(raw: Option<&RawValue>, default: bool) -> bool {
    match raw.map(RawValue::get) {
        Some("true") => true,
        Some("false") => false,
        _ => default,
    }
}

pub(crate) fn json(raw: Option<&RawValue>) -> &str {
    raw.map_or("[]", RawValue::get)
}

fn number(raw: Option<&RawValue>) -> &str {
    let value = raw.map_or("", RawValue::get);
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_should_borrow_combined_stream_fields() -> Result<()> {
        let (stream, events) = decode(
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":12,"s":"BTCUSDT","p":"1.25"}}"#,
        )?;

        assert_eq!(
            (stream, string(events[0].e), string(events[0].s)),
            ("btcusdt@trade", "trade", "BTCUSDT")
        );
        Ok(())
    }

    #[test]
    fn decode_should_support_raw_events() -> Result<()> {
        let (stream, events) = decode(br#"{"e":"bookTicker","u":7,"s":"BTCUSDT"}"#)?;

        assert_eq!((stream, u64(events[0].u)), ("", 7));
        Ok(())
    }

    #[test]
    fn decode_should_support_all_market_arrays() -> Result<()> {
        let (_, events) = decode(
            br#"{"stream":"!miniTicker@arr","data":[{"e":"24hrMiniTicker","s":"BTCUSDT"},{"e":"24hrMiniTicker","s":"ETHUSDT"}]}"#,
        )?;

        assert_eq!(events.len(), 2);
        Ok(())
    }
}
