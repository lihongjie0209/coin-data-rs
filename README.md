# coin-data-rs

Rust implementation of a Binance Spot, USDⓈ-M, and COIN-M real-time market-data collector. One process supervises all three markets. It consumes sharded WebSocket streams and writes every documented field directly to structured Parquet parts.

DuckDB is dynamically linked. Release archives contain the matching `libduckdb.so`; install it under `/usr/local/lib/coin-data-rs` (the systemd unit sets `LD_LIBRARY_PATH`). The server does not need GCC or a Rust toolchain.

The receiver and Parquet writers are separated by bounded asynchronous channels. Four dedicated
blocking writer shards keep WebSocket ingestion responsive and cap the total pending record buffer
at 64 MiB. Completed local parts are streamed into larger Parquet objects and uploaded by a separate
background worker every 30 minutes; merging does not load a full partition into memory. DuckDB is
used only by the private SQL endpoint to query immutable local Parquet files.

## Run

```bash
cargo run --release -- \
  --parquet-dir data/parquet
```

Use `--help` for all settings. By default the process runs `spot`, `usdm`, and `coinm` together under independent dataset directories. Set `--all-markets=false --market usdm` to run one market only. The default `--symbols ALL` discovers all currently tradable instruments in each market. The desired connection count defaults to four per market and is automatically increased when Binance's 1024-stream limit requires it. USDⓈ-M high-frequency public streams and regular market streams are routed to their separate endpoints.

Aggregate-trade gaps are checked every ten minutes, with a check at minute 29 and 59 before each upload. Spot uses `/api/v3/aggTrades`; futures use their corresponding `/fapi` or `/dapi` endpoint. Futures open interest is sampled once per minute.

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

S3 objects use exchange, market, symbol, table, date, and hour. Each populated partition normally
gets `data-00.parquet` and `data-30.parquet`:

```text
parquet/rust/binance/usdm/BTCUSDT/futures_aggregate_trades/2026-08-03/04/data-30.parquet
```

Only table/symbol/hour partitions containing rows produce files. Every source field, including
`symbol`, remains present in each Parquet file.

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` to receive a success or failure report after every
automatic or manually triggered upload. Reports include merged/source file counts, bytes, duration, collector
counters, load average, memory, and disk usage.

## Tables

Spot tables include `depth_updates`, `depth_levels`, `aggregate_trades`, `trades`, `book_tickers`, `tickers`, `rolling_tickers`, `mini_tickers`, `klines`, and `average_prices`. Futures use dedicated `futures_*` tables for depth, aggregate trades, book ticker, mark/index/funding price, liquidation, open interest, mini ticker, ticker, and kline data. Raw JSON is not retained.
