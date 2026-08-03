use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result, bail};
use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Decimal128Builder, Int32Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder, UInt64Builder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use chrono::{DateTime, TimeZone, Utc};
use duckdb::types::{TimeUnit as DuckTimeUnit, Value};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::model::Record;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Partition {
    pub symbol: String,
    pub table: &'static str,
    pub hour: DateTime<Utc>,
}

pub fn partition(record: &Record) -> Result<Partition> {
    let table = table(record.table)?;
    let symbol_index = table
        .fields
        .iter()
        .position(|field| field.name() == "symbol")
        .context("table has no symbol column")?;
    let time_name = match record.table {
        "book_tickers" => "received_at",
        "futures_open_interest" => "observed_at",
        _ => "event_time",
    };
    let time_index = table
        .fields
        .iter()
        .position(|field| field.name() == time_name)
        .context("table has no partition timestamp")?;
    let symbol = match record.values.get(symbol_index) {
        Some(Value::Text(value)) => value.clone(),
        _ => bail!("record has no symbol value"),
    };
    let timestamp = timestamp_micros(
        record
            .values
            .get(time_index)
            .context("record has no partition timestamp value")?,
    )?;
    let instant = Utc
        .timestamp_micros(timestamp)
        .single()
        .context("invalid partition timestamp")?;
    let hour = instant
        .date_naive()
        .and_hms_opt(instant.hour(), 0, 0)
        .map(|value| value.and_utc())
        .context("invalid partition hour")?;
    Ok(Partition {
        symbol,
        table: record.table,
        hour,
    })
}

pub fn write_part(
    directory: &Path,
    partition: &Partition,
    sequence: u64,
    records: &[Record],
) -> Result<PathBuf> {
    let definition = table(partition.table)?;
    let batch = record_batch(definition, records)?;
    let parent = directory
        .join(&partition.symbol)
        .join(partition.table)
        .join(partition.hour.format("%Y-%m-%d").to_string())
        .join(partition.hour.format("%H").to_string());
    std::fs::create_dir_all(&parent).context("create Parquet partition")?;
    let name = format!(
        "part-{}-{sequence:016x}.parquet",
        Utc::now().timestamp_micros()
    );
    let path = parent.join(name);
    let temporary = path.with_extension("parquet.tmp");
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
        .set_max_row_group_row_count(Some(records.len().max(1)))
        .build();
    let file = File::create(&temporary).context("create temporary Parquet file")?;
    let mut writer = ArrowWriter::try_new(file, definition.schema.clone(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    std::fs::rename(&temporary, &path).context("publish completed Parquet part")?;
    Ok(path)
}

struct TableDefinition {
    fields: Vec<Field>,
    schema: Arc<Schema>,
}

fn table(name: &str) -> Result<&'static TableDefinition> {
    definitions()
        .get(name)
        .with_context(|| format!("unknown table {name}"))
}

fn definitions() -> &'static HashMap<String, TableDefinition> {
    static DEFINITIONS: OnceLock<HashMap<String, TableDefinition>> = OnceLock::new();
    DEFINITIONS.get_or_init(|| parse_schema(include_str!("schema.sql")))
}

fn parse_schema(sql: &str) -> HashMap<String, TableDefinition> {
    const PREFIX: &str = "CREATE TABLE IF NOT EXISTS ";
    let mut result = HashMap::new();
    for statement in sql.lines().filter(|line| line.starts_with(PREFIX)) {
        let Some((name, columns)) = statement[PREFIX.len()..].split_once(" (") else {
            continue;
        };
        if !crate::storage::TABLES.contains(&name) {
            continue;
        }
        let columns = columns.trim_end_matches(';').trim_end_matches(')');
        let fields = split_columns(columns)
            .into_iter()
            .filter_map(|column| {
                let (name, kind) = column.trim().split_once(' ')?;
                let data_type = if kind.starts_with("TIMESTAMPTZ") {
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                } else if kind.starts_with("VARCHAR") {
                    DataType::Utf8
                } else if kind.starts_with("BOOLEAN") {
                    DataType::Boolean
                } else if kind.starts_with("UBIGINT") {
                    DataType::UInt64
                } else if kind.starts_with("BIGINT") {
                    DataType::Int64
                } else if kind.starts_with("INTEGER") {
                    DataType::Int32
                } else if kind.starts_with("DECIMAL") {
                    DataType::Decimal128(38, 18)
                } else {
                    return None;
                };
                Some(Field::new(name, data_type, true))
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields.clone()));
        result.insert(name.to_owned(), TableDefinition { fields, schema });
    }
    result
}

fn split_columns(value: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (index, character) in value.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                result.push(&value[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    result.push(&value[start..]);
    result
}

fn record_batch(definition: &TableDefinition, records: &[Record]) -> Result<RecordBatch> {
    if records
        .iter()
        .any(|record| record.values.len() != definition.fields.len())
    {
        bail!("record column count does not match Parquet schema");
    }
    let mut arrays = Vec::with_capacity(definition.fields.len());
    for (index, field) in definition.fields.iter().enumerate() {
        arrays.push(array(field.data_type(), records, index)?);
    }
    RecordBatch::try_new(definition.schema.clone(), arrays).context("build Arrow record batch")
}

fn array(kind: &DataType, records: &[Record], index: usize) -> Result<ArrayRef> {
    macro_rules! primitive {
        ($builder:ty, $variant:ident) => {{
            let mut builder = <$builder>::with_capacity(records.len());
            for record in records {
                match &record.values[index] {
                    Value::$variant(value) => builder.append_value(*value),
                    Value::Null => builder.append_null(),
                    other => bail!("unexpected value {other:?} for {kind:?}"),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }};
    }
    Ok(match kind {
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for record in records {
                match &record.values[index] {
                    Value::Text(value) => builder.append_value(value),
                    Value::Null => builder.append_null(),
                    other => bail!("unexpected value {other:?} for Utf8"),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => primitive!(BooleanBuilder, Boolean),
        DataType::Int32 => primitive!(Int32Builder, Int),
        DataType::Int64 => primitive!(Int64Builder, BigInt),
        DataType::UInt64 => primitive!(UInt64Builder, UBigInt),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut builder =
                TimestampMicrosecondBuilder::with_capacity(records.len()).with_timezone("UTC");
            for record in records {
                match &record.values[index] {
                    Value::Timestamp(unit, value) => {
                        builder.append_value(to_micros(*unit, *value)?)
                    }
                    Value::Null => builder.append_null(),
                    other => bail!("unexpected value {other:?} for timestamp"),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Decimal128(38, 18) => {
            let mut builder =
                Decimal128Builder::with_capacity(records.len()).with_precision_and_scale(38, 18)?;
            for record in records {
                match &record.values[index] {
                    Value::Decimal(value) => builder.append_value(value.value()),
                    Value::Text(value) => builder.append_value(decimal_scaled(value)?),
                    Value::Null => builder.append_null(),
                    other => bail!("unexpected value {other:?} for decimal"),
                }
            }
            Arc::new(builder.finish())
        }
        other => bail!("unsupported Arrow type {other:?}"),
    })
}

fn decimal_scaled(raw: &str) -> Result<i128> {
    let (negative, unsigned) = raw
        .strip_prefix('-')
        .map_or((false, raw), |value| (true, value));
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let mut digits = String::with_capacity(whole.len() + 18);
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    digits.extend(fraction.chars().take(18));
    digits.extend(std::iter::repeat_n(
        '0',
        18usize.saturating_sub(fraction.len()),
    ));
    let value = digits.parse::<i128>().context("parse decimal value")?;
    Ok(if negative { -value } else { value })
}

fn timestamp_micros(value: &Value) -> Result<i64> {
    match value {
        Value::Timestamp(unit, value) => to_micros(*unit, *value),
        _ => bail!("value is not a timestamp"),
    }
}

fn to_micros(unit: DuckTimeUnit, value: i64) -> Result<i64> {
    Ok(match unit {
        DuckTimeUnit::Second => value.saturating_mul(1_000_000),
        DuckTimeUnit::Millisecond => value.saturating_mul(1_000),
        DuckTimeUnit::Microsecond => value,
        DuckTimeUnit::Nanosecond => value / 1_000,
    })
}

use chrono::Timelike;

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::parser;

    #[test]
    fn writes_atomic_symbol_table_hour_part() -> Result<()> {
        let directory = tempdir()?;
        let received = Utc
            .with_ymd_and_hms(2026, 8, 3, 9, 1, 0)
            .single()
            .context("invalid test timestamp")?;
        let payload = br#"{"e":"aggTrade","E":1785747660000,"s":"BTCUSDT","a":1,"p":"1","q":"2","f":1,"l":1,"T":1785747660000,"m":false,"M":true}"#;
        let records = parser::parse(payload, received, "test")?;
        let key = partition(&records[0])?;
        let path = write_part(directory.path(), &key, 1, &records)?;
        assert!(path.is_file());
        assert!(
            path.to_string_lossy()
                .contains("BTCUSDT/aggregate_trades/2026-08-03/09/part-")
        );
        Ok(())
    }
}
