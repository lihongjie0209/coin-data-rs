use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use duckdb::{Connection, appender_params_from_iter, params, types::ValueRef};

use crate::model::Record;

pub struct Storage {
    connection: Connection,
}

#[derive(Debug)]
pub struct ExportFile {
    pub table: String,
    pub symbol: String,
    pub path: PathBuf,
}

pub struct UploadRecord {
    pub table: String,
    pub symbol: String,
    pub start: DateTime<Utc>,
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub size: u64,
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("create database directory")?;
        }
        let connection = Connection::open(path).context("open DuckDB")?;
        connection.execute_batch(
            "SET TimeZone='UTC'; SET memory_limit='512MB'; SET threads=1; SET preserve_insertion_order=false;",
        )?;
        connection
            .execute_batch(include_str!("schema.sql"))
            .context("initialize schema")?;
        Ok(Self { connection })
    }

    pub fn open_existing(path: &Path) -> Result<Self> {
        let connection = Connection::open(path).context("open DuckDB")?;
        connection.execute_batch(
            "SET TimeZone='UTC'; SET memory_limit='512MB'; SET threads=1; SET preserve_insertion_order=false;",
        )?;
        Ok(Self { connection })
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.connection
            .execute_batch("CHECKPOINT")
            .context("checkpoint DuckDB before export snapshot")
    }

    pub fn insert(&mut self, records: &[Record]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }
        for table in TABLES {
            let mut appender = self.connection.appender(table)?;
            for record in records.iter().filter(|record| record.table == *table) {
                appender.append_row(appender_params_from_iter(record.values.iter()))?;
            }
            appender.flush().with_context(|| format!("flush {table}"))?;
        }
        Ok(records.len())
    }

    pub fn stats(&self) -> Result<serde_json::Value> {
        let mut tables = serde_json::Map::new();
        for table in TABLES {
            let count: i64 =
                self.connection
                    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })?;
            tables.insert((*table).to_owned(), count.into());
        }
        let symbols: i64 = self.connection.query_row(
            "SELECT count(DISTINCT symbol) FROM (SELECT symbol FROM depth_updates UNION ALL SELECT symbol FROM futures_depth_updates)",
            [],
            |row| row.get(0),
        )?;
        Ok(serde_json::json!({"symbols": symbols, "tables": tables}))
    }

    pub fn query_json(&self, sql: &str) -> Result<serde_json::Value> {
        let mut statement = self.connection.prepare(sql).context("prepare SQL")?;
        let mut rows = statement.query([]).context("query SQL")?;
        let names = rows
            .as_ref()
            .map(duckdb::Statement::column_names)
            .unwrap_or_default();
        if names.is_empty() {
            return Ok(serde_json::json!({"rows": []}));
        }
        let mut result = Vec::new();
        while let Some(row) = rows.next().context("read SQL row")? {
            let mut object = serde_json::Map::with_capacity(names.len());
            for (index, name) in names.iter().enumerate() {
                object.insert(name.clone(), json_value(row.get_ref(index)?));
            }
            result.push(serde_json::Value::Object(object));
            if result.len() >= 10_000 {
                break;
            }
        }
        Ok(
            serde_json::json!({"columns": names, "rows": result, "truncated": result.len() == 10_000}),
        )
    }

    pub fn export_hour(
        &mut self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        directory: &Path,
        force: bool,
    ) -> Result<Vec<ExportFile>> {
        std::fs::create_dir_all(directory).context("create parquet directory")?;
        let mut files = Vec::new();
        for table in TABLES {
            let time_column = time_column(table);
            let symbols = self.symbols_to_export(table, time_column, start, end, force)?;
            if symbols.is_empty() {
                continue;
            }
            let staging_directory = directory
                .join(".staging")
                .join(format!("{table}_{}", start.format("%Y%m%dT%H")));
            if staging_directory.exists() {
                std::fs::remove_dir_all(&staging_directory)?;
            }
            std::fs::create_dir_all(&staging_directory)?;
            let safe_path = staging_directory.to_string_lossy().replace('\'', "''");
            for symbols in symbols.chunks(100) {
                let symbol_list = symbols
                    .iter()
                    .map(|symbol| format!("'{}'", symbol.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!(
                    "COPY (SELECT *, strftime({time_column}, '%Y-%m-%d') AS date, strftime({time_column}, '%H') AS hour FROM {table} WHERE {time_column} >= ? AND {time_column} < ? AND symbol IN ({symbol_list})) TO '{safe_path}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (symbol, date, hour), WRITE_PARTITION_COLUMNS, OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'data_{{i}}')"
                );
                self.connection
                    .execute(&sql, params![start, end])
                    .with_context(|| format!("export {table} symbol batch"))?;
                for symbol in symbols {
                    let partition_symbol =
                        url::form_urlencoded::byte_serialize(symbol.as_bytes()).collect::<String>();
                    let staged_path = staging_directory
                        .join(format!("symbol={partition_symbol}"))
                        .join(format!("date={}", start.format("%Y-%m-%d")))
                        .join(format!("hour={}", start.format("%H")))
                        .join("data_0.parquet");
                    if !staged_path.is_file() {
                        anyhow::bail!("DuckDB did not create {}", staged_path.display());
                    }
                    let path = directory
                        .join(symbol)
                        .join(*table)
                        .join(start.format("%Y-%m-%d").to_string())
                        .join(start.format("%H").to_string())
                        .join("data.parquet");
                    let parent = path.parent().context("Parquet path has no parent")?;
                    std::fs::create_dir_all(parent)?;
                    std::fs::rename(&staged_path, &path)?;
                    self.connection.execute(
                        "INSERT OR REPLACE INTO parquet_symbol_exports VALUES (?, ?, ?, now())",
                        params![table, symbol, start],
                    )?;
                    files.push(ExportFile {
                        table: (*table).to_owned(),
                        symbol: symbol.clone(),
                        path,
                    });
                }
            }
            std::fs::remove_dir_all(&staging_directory)?;
        }
        Ok(files)
    }

    fn symbols_to_export(
        &self,
        table: &str,
        time_column: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        force: bool,
    ) -> Result<Vec<String>> {
        let exported_filter = if force {
            String::new()
        } else {
            "AND NOT EXISTS (SELECT 1 FROM parquet_symbol_exports exported WHERE exported.table_name=? AND exported.symbol=data.symbol AND exported.hour_start=?)".to_owned()
        };
        let sql = format!(
            "SELECT DISTINCT symbol FROM {table} data WHERE {time_column} >= ? AND {time_column} < ? {exported_filter} ORDER BY symbol"
        );
        let mut statement = self.connection.prepare(&sql)?;
        let mut symbols = Vec::new();
        if force {
            let rows = statement.query_map(params![start, end], |row| row.get(0))?;
            for symbol in rows {
                symbols.push(symbol?);
            }
        } else {
            let rows = statement.query_map(params![start, end, table, start], |row| row.get(0))?;
            for symbol in rows {
                symbols.push(symbol?);
            }
        }
        Ok(symbols)
    }

    pub fn record_uploads(&mut self, uploads: &[UploadRecord]) -> Result<()> {
        for uploads in uploads.chunks(500) {
            let transaction = self.connection.transaction()?;
            for upload in uploads {
                transaction.execute(
                    "INSERT OR REPLACE INTO parquet_symbol_uploads VALUES (?, ?, ?, ?, ?, ?, ?, now())",
                    params![
                        &upload.table,
                        &upload.symbol,
                        upload.start,
                        &upload.bucket,
                        &upload.key,
                        &upload.etag,
                        upload.size
                    ],
                )?;
            }
            transaction.commit()?;
        }
        Ok(())
    }

    pub fn cleanup(&mut self, cutoff: DateTime<Utc>, bucket: &str) -> Result<usize> {
        let transaction = self.connection.transaction()?;
        let mut deleted = 0;
        for table in TABLES {
            let time_column = time_column(table);
            let sql = format!(
                "DELETE FROM {table} AS data WHERE {time_column} < ? AND EXISTS (SELECT 1 FROM parquet_symbol_uploads AS upload WHERE upload.table_name=? AND upload.symbol=data.symbol AND upload.bucket=? AND upload.hour_start=date_trunc('hour', data.{time_column}))"
            );
            deleted += transaction.execute(&sql, params![cutoff, table, bucket])?;
        }
        transaction.commit()?;
        Ok(deleted)
    }
}

fn time_column(table: &str) -> &'static str {
    match table {
        "book_tickers" => "received_at",
        "futures_open_interest" => "observed_at",
        _ => "event_time",
    }
}

fn json_value(value: ValueRef<'_>) -> serde_json::Value {
    match value {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Boolean(value) => value.into(),
        ValueRef::TinyInt(value) => value.into(),
        ValueRef::SmallInt(value) => value.into(),
        ValueRef::Int(value) => value.into(),
        ValueRef::BigInt(value) => value.into(),
        ValueRef::UTinyInt(value) => value.into(),
        ValueRef::USmallInt(value) => value.into(),
        ValueRef::UInt(value) => value.into(),
        ValueRef::UBigInt(value) => value.into(),
        ValueRef::Float(value) => value.into(),
        ValueRef::Double(value) => value.into(),
        ValueRef::Text(value) => String::from_utf8_lossy(value).into_owned().into(),
        other => format!("{other:?}").into(),
    }
}

pub const TABLES: &[&str] = &[
    "depth_updates",
    "depth_levels",
    "aggregate_trades",
    "trades",
    "book_tickers",
    "tickers",
    "rolling_tickers",
    "mini_tickers",
    "klines",
    "average_prices",
    "futures_depth_updates",
    "futures_depth_levels",
    "futures_aggregate_trades",
    "futures_book_tickers",
    "futures_mark_prices",
    "futures_liquidations",
    "futures_open_interest",
    "futures_mini_tickers",
    "futures_tickers",
    "futures_klines",
];

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use tempfile::tempdir;

    use super::*;
    use crate::parser;

    #[test]
    fn export_hour_should_partition_by_symbol_table_date_and_hour() -> Result<()> {
        let temporary = tempdir()?;
        let database = temporary.path().join("market.duckdb");
        let parquet = temporary.path().join("parquet");
        let start = Utc
            .with_ymd_and_hms(2026, 8, 3, 4, 0, 0)
            .single()
            .context("invalid test time")?;
        let mut storage = Storage::open(&database)?;
        let mut records = Vec::new();
        for symbol in ["BTCUSDT", "ETHUSDT", "币安人生USDT"] {
            let payload = format!(
                r#"{{"stream":"{}@aggTrade","data":{{"e":"aggTrade","E":1785731400000,"s":"{symbol}","a":1,"p":"1","q":"2","f":1,"l":1,"T":1785731400000,"m":false,"M":true}}}}"#,
                symbol.to_ascii_lowercase()
            );
            records.extend(parser::parse(payload.as_bytes(), start, "websocket")?);
        }
        storage.insert(&records)?;

        let files =
            storage.export_hour(start, start + chrono::Duration::hours(1), &parquet, false)?;

        assert_eq!(files.len(), 3);
        for symbol in ["BTCUSDT", "ETHUSDT", "币安人生USDT"] {
            let path = parquet
                .join(symbol)
                .join("aggregate_trades")
                .join("2026-08-03")
                .join("04")
                .join("data.parquet");
            assert!(path.is_file(), "missing {}", path.display());
            let exported_symbol: String = storage.connection.query_row(
                &format!(
                    "SELECT symbol FROM read_parquet('{}')",
                    path.to_string_lossy().replace('\'', "''")
                ),
                [],
                |row| row.get(0),
            )?;
            assert_eq!(exported_symbol, symbol);
        }
        Ok(())
    }

    #[test]
    fn export_hour_should_skip_already_exported_symbol_partitions() -> Result<()> {
        let temporary = tempdir()?;
        let database = temporary.path().join("market.duckdb");
        let parquet = temporary.path().join("parquet");
        let start = Utc
            .with_ymd_and_hms(2026, 8, 3, 4, 0, 0)
            .single()
            .context("invalid test time")?;
        let mut storage = Storage::open(&database)?;
        let payload = br#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1785731400000,"s":"BTCUSDT","a":1,"p":"1","q":"2","f":1,"l":1,"T":1785731400000,"m":false,"M":true}}"#;
        storage.insert(&parser::parse(payload, start, "websocket")?)?;
        storage.export_hour(start, start + chrono::Duration::hours(1), &parquet, false)?;

        let files =
            storage.export_hour(start, start + chrono::Duration::hours(1), &parquet, false)?;

        assert!(files.is_empty());
        Ok(())
    }

    #[test]
    fn export_hour_should_write_multiple_symbol_batches() -> Result<()> {
        let temporary = tempdir()?;
        let database = temporary.path().join("market.duckdb");
        let parquet = temporary.path().join("parquet");
        let start = Utc
            .with_ymd_and_hms(2026, 8, 3, 4, 0, 0)
            .single()
            .context("invalid test time")?;
        let mut storage = Storage::open(&database)?;
        let mut records = Vec::new();
        for index in 0..101 {
            let symbol = format!("TEST{index}USDT");
            let payload = format!(
                r#"{{"stream":"{}@aggTrade","data":{{"e":"aggTrade","E":1785731400000,"s":"{symbol}","a":1,"p":"1","q":"2","f":1,"l":1,"T":1785731400000,"m":false,"M":true}}}}"#,
                symbol.to_ascii_lowercase()
            );
            records.extend(parser::parse(payload.as_bytes(), start, "websocket")?);
        }
        storage.insert(&records)?;

        let files =
            storage.export_hour(start, start + chrono::Duration::hours(1), &parquet, false)?;

        assert_eq!(files.len(), 101);
        Ok(())
    }
}
