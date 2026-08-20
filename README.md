# coin-data-rs

Rust Binance Spot, USDⓈ-M, and COIN-M real-time market-data collector. One process supervises all three markets, preserves documented fields in typed Parquet columns, and asynchronously uploads completed segments to S3.

DuckDB is not used. Spot, USD-M, and COIN-M each have a dedicated writer thread and bounded queue. By default, the service derives safe per-market and per-table memory budgets from the host or cgroup memory limit. A table flushes when its target is reached, when its market reaches the memory limit, after five minutes, or at an UTC hour boundary. Every table becomes an independently closed Parquet segment through a `.tmp` file followed by an atomic rename. S3 failures retain the local segment for retry and never block WebSocket ingestion.

## Run

```bash
cargo run --release -- --database data/market
```

The parent of `--database` is the data root. `--buffer-mb` and `--segment-mb` default to automatic sizing; a positive value forces an explicit size. Queue and time limits can be changed with `--queue-capacity` and `--flush-seconds`. All currently tradable instruments and all three Binance markets are enabled by default. Aggregate-trade REST backfill remains disabled.

The Binance depth snapshot scheduler is enabled by default and runs independently of WebSocket ingestion. It staggers symbols across each interval, limits concurrent REST calls with `--snapshot-concurrency`, and accounts for Binance request weights (`--snapshot-depth-limit`). A weighted token bucket reserves 20% headroom and adapts to the `X-MBX-USED-WEIGHT-1M` response header; HTTP 429/418 responses pause the scheduler without stopping WebSocket writers. Set `--snapshot-enabled=false` to disable it, or tune `--snapshot-interval-seconds`, `--snapshot-depth-limit`, and `--snapshot-prefix`.

Snapshots are compressed JSON objects containing the complete REST response plus `captured_at`, `requested_at`, `last_update_id`, market, symbol, and requested depth. They are stored independently from Parquet increments:

```text
snapshots/rust/binance/spot/BTCUSDT/date=2026-08-04/hour=00/snapshot-20260804T001500.123Z-u123456789.json.zst
```

To reconstruct an order book for a target time, select the newest snapshot whose `captured_at` is at or before the target, apply depth-update rows after its `last_update_id`, and verify Binance `U/u/pu` continuity. If the first update does not bridge the snapshot ID, that window requires a newer snapshot; the collector does not claim historical completeness without this continuity check.

Segments use the following local and S3 layout:

```text
<root>/binance/spot/aggregate_trades/2026-08-04/00/segment-0000000001.parquet
<prefix>/binance/usdm/futures_depth_updates/2026-08-04/00/segment-0000000002.parquet
```

Only successfully uploaded segments receive `.uploaded` markers. The retention task removes marked segments after the configured retention period and may shorten retention when free disk falls below the threshold. Unuploaded data is never deleted.

## API

The API binds to `127.0.0.1:8081` by default.

```bash
curl http://127.0.0.1:8081/healthz
curl http://127.0.0.1:8081/v1/runtime
curl http://127.0.0.1:8081/v1/stats
curl -X POST http://127.0.0.1:8081/v1/archive \
  -H 'content-type: application/json' \
  -d '{"market":"spot","hour":"2026-08-04T00:00:00Z","force":true}'
```

Arbitrary SQL is no longer exposed because there is no embedded database. Query S3 Parquet with DuckDB, Athena, Spark, Polars, or another Parquet engine.

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` for manual archive success/failure reports containing file count, bytes, duration, collector counters, load, memory, and disk usage.

## Historical benchmark

The release includes `parquet-bench`. It repeatedly reads historical Parquet batches and rewrites them with the production ZSTD level to measure encoding throughput without touching production files.

```bash
parquet-bench --source /path/to/history --output /tmp/parquet-bench --seconds 60
```

Parser changes can be measured locally against fixed Spot and Futures websocket payloads with Criterion:

```bash
cargo bench --bench parser_replay
```

## Scheduled compaction

`parquet-compact` scans completed hourly S3 partitions and merges eligible `segment-*.parquet` objects with bounded Arrow batches. It reorders each bounded batch by symbol and event time, writes one-million-row groups with ZSTD level 3, and remains dry-run by default; `--execute` enables writes. Successful runs store `compacted.parquet` in S3 Intelligent-Tiering, copy source objects to `parquet/compaction-source/`, publish a success marker, and then remove the small objects from the production prefix. S3 Lifecycle retains the copied sources for one day.

The Fargate resources are defined in `deploy/compactor-cloudformation.yml`. EventBridge Scheduler starts one ARM64 task at 00:15, 08:15, and 16:15 UTC.

Spot tables include depth updates, aggregate trades, trades, book tickers, tickers, rolling tickers, mini tickers, klines, and average prices. Futures use dedicated `futures_*` tables for depth, aggregate trades, book tickers, mark/index/funding prices, liquidations, open interest, mini tickers, tickers, and klines. Raw event envelopes are not retained.
