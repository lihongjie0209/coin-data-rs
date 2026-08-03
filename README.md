# coin-data-rs

Rust implementation of a Binance Spot, USDⓈ-M, and COIN-M real-time market-data collector. One process supervises all three markets while each market retains an independent DuckDB file. It consumes sharded WebSocket streams, stores documented fields in structured tables, exports hourly Parquet files, and uploads them to S3.

DuckDB is dynamically linked. Release archives contain the matching `libduckdb.so`; install it under `/usr/local/lib/coin-data-rs` (the systemd unit sets `LD_LIBRARY_PATH`). The server does not need GCC or a Rust toolchain.

The receiver and database writer are separated by a bounded asynchronous channel. DuckDB writes run
on a dedicated blocking thread. Export uses a checkpointed snapshot so live ingestion resumes while
Parquet files are generated and uploaded.

## Run

```bash
cargo run --release -- \
  --database data/market.duckdb
```

Use `--help` for all settings. By default the process runs `spot`, `usdm`, and `coinm` together and creates `binance-spot.duckdb`, `binance-usdm.duckdb`, and `binance-coinm.duckdb` next to the `--database` path. Set `--all-markets=false --market usdm` to run one market only. The default `--symbols ALL` discovers all currently tradable instruments in each market. The desired connection count defaults to four per market and is automatically increased when Binance's 1024-stream limit requires it. USDⓈ-M high-frequency public streams and regular market streams are routed to their separate endpoints.

Aggregate-trade gaps are checked every ten minutes and immediately before export. Spot uses `/api/v3/aggTrades`; futures use their corresponding `/fapi` or `/dapi` endpoint. Futures open interest is sampled once per minute.

Local structured data is normally retained for at most eight hours. When free disk falls below 20%, uploaded rows older than four hours are reclaimed; data inside the four-hour safety window is never pressure-deleted. All three thresholds are configurable.

## API

The API binds to `127.0.0.1:8081` by default.

```bash
curl http://127.0.0.1:8081/healthz
curl http://127.0.0.1:8081/v1/stats
curl -X POST http://127.0.0.1:8081/v1/sql \
  -H 'content-type: application/json' \
  -d '{"market":"usdm","sql":"select symbol,count(*) from futures_aggregate_trades group by symbol"}'
curl -X POST http://127.0.0.1:8081/v1/archive \
  -H 'content-type: application/json' \
  -d '{"market":"coinm","hour":"2026-08-03T12:00:00Z","force":true}'
```

The SQL endpoint intentionally accepts arbitrary SQL and must remain private.

Hourly Parquet objects use exchange, market, symbol, table, date, and hour:

```text
parquet/rust/binance/usdm/BTCUSDT/futures_aggregate_trades/2026-08-03/04/data.parquet
```

Only table/symbol/hour partitions containing rows produce files. Every source field, including
`symbol`, remains present in each Parquet file.

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` to receive a success or failure report after every
automatic or manually triggered archive. Reports include file count, bytes, duration, collector
counters, load average, memory, and disk usage.

## Tables

Spot tables include `depth_updates`, `depth_levels`, `aggregate_trades`, `trades`, `book_tickers`, `tickers`, `rolling_tickers`, `mini_tickers`, `klines`, and `average_prices`. Futures use dedicated `futures_*` tables for depth, aggregate trades, book ticker, mark/index/funding price, liquidation, open interest, mini ticker, ticker, and kline data. Raw JSON is not retained.
