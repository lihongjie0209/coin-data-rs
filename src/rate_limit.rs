use std::sync::atomic::{AtomicI64, Ordering};

use chrono::Utc;

static BINANCE_REST_BLOCKED_UNTIL: AtomicI64 = AtomicI64::new(0);

pub fn remaining_seconds() -> u64 {
    let remaining = BINANCE_REST_BLOCKED_UNTIL
        .load(Ordering::Relaxed)
        .saturating_sub(Utc::now().timestamp());
    u64::try_from(remaining).unwrap_or_default()
}

pub fn block_for(seconds: u64) {
    let until = Utc::now()
        .timestamp()
        .saturating_add(i64::try_from(seconds).unwrap_or(i64::MAX));
    BINANCE_REST_BLOCKED_UNTIL.fetch_max(until, Ordering::Relaxed);
}

pub fn observe_response(response: &reqwest::Response) -> bool {
    if !matches!(response.status().as_u16(), 418 | 429) {
        return false;
    }
    let fallback = if response.status().as_u16() == 418 {
        300
    } else {
        60
    };
    let seconds = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(fallback)
        .clamp(1, 86_400);
    block_for(seconds);
    true
}
