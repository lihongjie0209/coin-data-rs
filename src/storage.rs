use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use duckdb::{Connection, params, params_from_iter, types::ValueRef};

use crate::model::Record;

pub struct Storage {
    connection: Connection,
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("create database directory")?;
        }
        let connection = Connection::open(path).context("open DuckDB")?;
        connection.execute_batch("SET TimeZone='UTC';")?;
        connection
            .execute_batch(include_str!("schema.sql"))
            .context("initialize schema")?;
        Ok(Self { connection })
    }

    pub fn insert(&mut self, records: &[Record]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }
        let transaction = self.connection.transaction().context("begin write batch")?;
        for table in TABLES {
            let mut statement = transaction.prepare(insert_sql(table)?)?;
            for record in records.iter().filter(|record| record.table == *table) {
                statement
                    .execute(params_from_iter(record.values.iter()))
                    .with_context(|| format!("insert {}", record.table))?;
            }
        }
        transaction.commit().context("commit write batch")?;
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
            "SELECT count(DISTINCT symbol) FROM depth_updates",
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
    ) -> Result<Vec<PathBuf>> {
        std::fs::create_dir_all(directory).context("create parquet directory")?;
        let stamp = start.format("%Y%m%dT%H0000Z");
        let mut files = Vec::with_capacity(TABLES.len());
        for table in TABLES {
            let already_exported: bool = self.connection.query_row(
                "SELECT count(*) > 0 FROM parquet_exports WHERE table_name=? AND hour_start=?",
                params![table, start],
                |row| row.get(0),
            )?;
            if already_exported && !force {
                continue;
            }
            let path = directory.join(format!("{table}_{stamp}.parquet"));
            let time_column = if *table == "book_tickers" {
                "received_at"
            } else {
                "event_time"
            };
            let safe_path = path.to_string_lossy().replace('\'', "''");
            let sql = format!(
                "COPY (SELECT * FROM {table} WHERE {time_column} >= ? AND {time_column} < ? ORDER BY {time_column}) TO '{safe_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            );
            self.connection.execute(&sql, params![start, end])?;
            self.connection.execute(
                "INSERT OR REPLACE INTO parquet_exports VALUES (?, ?, now())",
                params![table, start],
            )?;
            files.push(path);
        }
        Ok(files)
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
];

fn insert_sql(table: &str) -> Result<&'static str> {
    let sql = match table {
        "depth_updates" => "INSERT INTO depth_updates VALUES (?, ?, ?, ?, ?, ?)",
        "depth_levels" => {
            "INSERT INTO depth_levels VALUES (?, ?, ?, ?, ?, ?, ?, ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?)"
        }
        "aggregate_trades" => {
            "INSERT INTO aggregate_trades VALUES (?, ?, ?, ?, ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?, ?, ?, ?, ?, ?)"
        }
        "trades" => {
            "INSERT INTO trades VALUES (?, ?, ?, ?, ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?, ?, ?, ?)"
        }
        "book_tickers" => {
            "INSERT INTO book_tickers VALUES (?, ?, ?, ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?)"
        }
        "tickers" => {
            "INSERT INTO tickers VALUES (?, ?, ?, ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?, ?, ?, ?, ?, ?)"
        }
        "rolling_tickers" => {
            "INSERT INTO rolling_tickers VALUES (?, ?, ?, ?, ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?, ?, ?, ?, ?, ?)"
        }
        "mini_tickers" => {
            "INSERT INTO mini_tickers VALUES (?, ?, ?, ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?)"
        }
        "klines" => {
            "INSERT INTO klines VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?, ?, ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?::DECIMAL(38,18), ?)"
        }
        "average_prices" => {
            "INSERT INTO average_prices VALUES (?, ?, ?, ?, ?::DECIMAL(38,18), ?, ?)"
        }
        _ => bail!("unknown table {table}"),
    };
    Ok(sql)
}
