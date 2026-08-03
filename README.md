# coin-data-rs

Rust implementation of a Binance Spot real-time market-data collector. It consumes sharded combined WebSocket streams, stores every documented field in structured DuckDB tables, exports hourly Parquet files, and uploads them to S3.

DuckDB is dynamically linked. Release archives contain the matching `libduckdb.so`; install it under `/usr/local/lib/coin-data-rs` (the systemd unit sets `LD_LIBRARY_PATH`). The server does not need GCC or a Rust toolchain.

The receiver and database writer are separated by a bounded asynchronous channel. DuckDB work runs on a dedicated blocking thread. Query/export requests enter the same ordered command stream, so an export sees every write accepted before its barrier without waiting for live traffic to stop.

## Run

```bash
cargo run --release -- \
  --symbols BTCUSDT,ETHUSDT \
  --ws-connections 4 \
  --database data/market.duckdb
```

Use `--help` for all settings. Defaults collect ten liquid USDT pairs with four WebSocket connections and eleven stream types per symbol.

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

## Tables

`depth_updates`, `depth_levels`, `aggregate_trades`, `trades`, `book_tickers`, `tickers`, `rolling_tickers`, `mini_tickers`, `klines`, and `average_prices`. Raw JSON is not retained.
