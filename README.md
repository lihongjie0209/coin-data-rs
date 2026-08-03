# coin-data-rs

Rust implementation of a Binance Spot real-time market-data collector. It consumes sharded combined WebSocket streams, stores every documented field in structured DuckDB tables, exports hourly Parquet files, and uploads them to S3.

DuckDB is dynamically linked. Release archives contain the matching `libduckdb.so`; install it under `/usr/local/lib/coin-data-rs` (the systemd unit sets `LD_LIBRARY_PATH`). The server does not need GCC or a Rust toolchain.

The receiver and database writer are separated by a bounded asynchronous channel. DuckDB writes run
on a dedicated blocking thread. An export first crosses an ordered flush barrier, then uses a separate
DuckDB connection so live writes continue while Parquet files are generated and uploaded.

## Run

```bash
cargo run --release -- \
  --database data/market.duckdb
```

Use `--help` for all settings. The default `--symbols ALL` discovers every currently tradable Binance Spot pair from `exchangeInfo`. `--ws-connections 0` automatically uses at least `ceil(symbols × streams / 1024)` connections; an explicitly larger value is also accepted. Subscriptions are sent after the WebSocket handshake, avoiding combined-stream URL length limits.

Local structured data is normally retained for at most eight hours. When free disk falls below 20%, uploaded rows older than four hours are reclaimed; data inside the four-hour safety window is never pressure-deleted. All three thresholds are configurable.

## API

The API binds to `127.0.0.1:8081` by default.

```bash
curl http://127.0.0.1:8081/healthz
curl http://127.0.0.1:8081/v1/stats
curl -X POST http://127.0.0.1:8081/v1/sql \
  -H 'content-type: application/json' \
  -d '{"sql":"select symbol,count(*) from aggregate_trades group by symbol"}'
curl -X POST http://127.0.0.1:8081/v1/archive \
  -H 'content-type: application/json' \
  -d '{"hour":"2026-08-03T12:00:00Z","force":true}'
```

The SQL endpoint intentionally accepts arbitrary SQL and must remain private.

Hourly Parquet objects are partitioned by table and symbol:

```text
parquet/rust/BTCUSDT/aggregate_trades/2026-08-03/04/data.parquet
```

Only table/symbol/hour partitions containing rows produce files. Every source field, including
`symbol`, remains present in each Parquet file.

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` to receive a success or failure report after every
automatic or manually triggered archive. Reports include file count, bytes, duration, collector
counters, load average, memory, and disk usage.

## Tables

`depth_updates`, `depth_levels`, `aggregate_trades`, `trades`, `book_tickers`, `tickers`, `rolling_tickers`, `mini_tickers`, `klines`, and `average_prices`. Raw JSON is not retained.
