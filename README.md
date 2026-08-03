# coin-data-rs

Rust implementation of a Binance Spot, USDⓈ-M, and COIN-M real-time market-data collector. One process supervises all three markets and writes one DuckDB file per exchange per UTC hour. It consumes sharded WebSocket streams, stores documented fields in structured tables, and uploads each closed hourly database directly to S3.

DuckDB is dynamically linked. Release archives contain the matching `libduckdb.so`; install it under `/usr/local/lib/coin-data-rs` (the systemd unit sets `LD_LIBRARY_PATH`). The server does not need GCC or a Rust toolchain.

The receiver and database writer are separated by a bounded asynchronous channel. DuckDB writes run
on one dedicated blocking thread. The following hour's database is initialized in advance; at the
hour boundary the writer flushes and checkpoints the old file, switches to the prepared file, and
hands the closed file to the asynchronous S3 uploader. The connection is limited to 160 MiB and the
bounded writer queue prevents an extended database stall from exhausting host memory.

## Run

```bash
cargo run --release -- \
  --database data/market.duckdb
```

Use `--help` for all settings. By default the process runs `spot`, `usdm`, and `coinm` together. All markets for one exchange share one hourly DuckDB file under `<database parent>/binance/YYYY-MM-DD/HH.duckdb`; the next hour is prepared in advance and the writer switches files at the UTC hour boundary. Closed files are uploaded directly to S3 as `<prefix>/binance/YYYY-MM-DD/HH.duckdb`, without a Parquet conversion step. Set `--all-markets=false --market usdm` to run one market only. The default `--symbols ALL` discovers all currently tradable instruments in each market. The desired connection count defaults to four per market and is automatically increased when Binance's 1024-stream limit requires it. USDⓈ-M high-frequency public streams and regular market streams are routed to their separate endpoints.

Aggregate trades come only from real-time WebSocket streams; REST historical backfill is disabled.
Futures open interest is sampled once per minute and shares a global Binance REST backoff after
HTTP 418 or 429 responses.

Uploaded hourly DuckDB files are normally retained locally for at most eight hours. When free disk falls below 20%, uploaded files older than four hours are deleted early; active, future, unuploaded, and files inside the four-hour safety window are never pressure-deleted. All three thresholds are configurable.

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

Hourly DuckDB objects use exchange, date, and hour:

```text
parquet/rust/binance/2026-08-03/04.duckdb
```

The single file contains all spot, USD-M, and COIN-M tables for the hour. Market-specific `source`
values distinguish the feeds while all structured fields remain queryable in DuckDB.

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` to receive a success or failure report after every
automatic or manually triggered archive. Reports include file count, bytes, duration, collector
counters, load average, memory, and disk usage.

## Tables

Spot tables include `depth_updates`, `depth_levels`, `aggregate_trades`, `trades`, `book_tickers`, `tickers`, `rolling_tickers`, `mini_tickers`, `klines`, and `average_prices`. Futures use dedicated `futures_*` tables for depth, aggregate trades, book ticker, mark/index/funding price, liquidation, open interest, mini ticker, ticker, and kline data. Raw JSON is not retained.
