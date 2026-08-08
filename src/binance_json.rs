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
    #[serde(default, borrow)]
    pub nq: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub st: Option<&'a RawValue>,
}

pub(crate) struct Events<'a> {
    one: Option<Event<'a>>,
    many: std::vec::IntoIter<Event<'a>>,
}

impl<'a> Iterator for Events<'a> {
    type Item = Event<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.one.take().or_else(|| self.many.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let length = self.len();
        (length, Some(length))
    }
}

impl ExactSizeIterator for Events<'_> {
    fn len(&self) -> usize {
        usize::from(self.one.is_some()) + self.many.len()
    }
}

pub(crate) fn decode(payload: &[u8]) -> Result<(&str, Events<'_>)> {
    let (stream, event_payload) = websocket_payload(payload)?;
    let events = if event_payload.first() == Some(&b'[') {
        let events: Vec<Event<'_>> =
            serde_json::from_slice(event_payload).context("decode websocket event array")?;
        Events {
            one: None,
            many: events.into_iter(),
        }
    } else {
        Events {
            one: Some(serde_json::from_slice(event_payload).context("decode websocket event")?),
            many: Vec::new().into_iter(),
        }
    };
    Ok((stream, events))
}

fn websocket_payload(payload: &[u8]) -> Result<(&str, &[u8])> {
    if let Some(combined) = combined_payload(payload) {
        return Ok(combined);
    }
    let envelope: Envelope<'_> =
        serde_json::from_slice(payload).context("decode websocket envelope")?;
    let event_payload = envelope.data.map_or(payload, |data| data.get().as_bytes());
    Ok((envelope.stream, event_payload))
}

fn combined_payload(payload: &[u8]) -> Option<(&str, &[u8])> {
    const PREFIX: &[u8] = br#"{"stream":""#;
    const DATA_SEPARATOR: &[u8] = br#"","data":"#;

    let rest = payload.strip_prefix(PREFIX)?;
    let stream_end = rest.iter().position(|byte| *byte == b'"')?;
    let stream = std::str::from_utf8(&rest[..stream_end]).ok()?;
    let event_payload = rest[stream_end..]
        .strip_prefix(DATA_SEPARATOR)?
        .strip_suffix(b"}")?;
    (!event_payload.is_empty()).then_some((stream, event_payload))
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
        let (stream, mut events) = decode(
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":12,"s":"BTCUSDT","p":"1.25"}}"#,
        )?;
        let event = events.next().context("missing event")?;

        assert_eq!(
            (stream, string(event.e), string(event.s)),
            ("btcusdt@trade", "trade", "BTCUSDT")
        );
        Ok(())
    }

    #[test]
    fn decode_should_support_raw_events() -> Result<()> {
        let (stream, mut events) = decode(br#"{"e":"bookTicker","u":7,"s":"BTCUSDT"}"#)?;
        let event = events.next().context("missing event")?;

        assert_eq!((stream, u64(event.u)), ("", 7));
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

    #[test]
    fn combined_payload_should_borrow_fixed_binance_envelope() {
        let payload = br#"{"stream":"btcusdt@trade","data":{"e":"trade"}}"#;

        assert_eq!(
            combined_payload(payload),
            Some(("btcusdt@trade", br#"{"e":"trade"}"#.as_slice()))
        );
    }

    #[test]
    fn decode_should_fall_back_for_envelope_with_whitespace() -> Result<()> {
        let (stream, mut events) =
            decode(br#"{ "data": {"e":"trade"}, "stream": "btcusdt@trade" }"#)?;
        let event = events.next().context("missing event")?;

        assert_eq!((stream, string(event.e)), ("btcusdt@trade", "trade"));
        Ok(())
    }
}
