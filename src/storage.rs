use std::{collections::HashMap, path::Path};

use anyhow::{Context, Result};
use duckdb::{Connection, appender_params_from_iter, types::ValueRef};

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
        configure(&connection)?;
        connection
            .execute_batch(include_str!("schema.sql"))
            .context("initialize schema")?;
        Ok(Self { connection })
    }

    pub fn open_existing(path: &Path) -> Result<Self> {
        let connection = Connection::open(path).context("open DuckDB")?;
        configure(&connection)?;
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
        let mut grouped: HashMap<&str, Vec<&Record>> = HashMap::new();
        for record in records {
            grouped.entry(record.table).or_default().push(record);
        }
        let transaction = self.connection.transaction()?;
        for (table, records) in grouped {
            let mut appender = transaction.appender(table)?;
            for record in records {
                appender.append_row(appender_params_from_iter(record.values.iter()))?;
            }
            appender.flush().with_context(|| format!("flush {table}"))?;
        }
        transaction.commit()?;
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
}

fn configure(connection: &Connection) -> Result<()> {
    // Hourly rotation performs an explicit checkpoint. A larger automatic threshold avoids
    // repeatedly checkpointing a multi-gigabyte active database during ingestion bursts.
    connection.execute_batch(
        "SET TimeZone='UTC';
         SET memory_limit='160MB';
         SET threads=1;
         SET preserve_insertion_order=false;
         SET checkpoint_threshold='1GB';",
    )?;
    Ok(())
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
