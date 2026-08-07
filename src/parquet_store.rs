use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result, bail};
use arrow_array::{
    ArrayRef, BooleanArray, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Datelike, Timelike, Utc};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

use crate::{
    config::Market,
    model::{Record, Value},
};

#[derive(Debug, Clone)]
pub struct Segment {
    pub hour: DateTime<Utc>,
    pub market: Market,
    pub table: &'static str,
    pub path: PathBuf,
    pub rows: usize,
    pub bytes: u64,
}

pub fn write_segment(
    root: &Path,
    exchange: &str,
    market: Market,
    hour: DateTime<Utc>,
    sequence: u64,
    table: &'static str,
    records: &[Record],
) -> Result<Segment> {
    let schema = schema_for(table)?;
    let batch = record_batch(schema.clone(), records)?;
    let directory = root
        .join(exchange)
        .join(market.as_str())
        .join(table)
        .join(format!(
            "{:04}-{:02}-{:02}",
            hour.year(),
            hour.month(),
            hour.day()
        ))
        .join(format!("{:02}", hour.hour()));
    std::fs::create_dir_all(&directory).context("create Parquet segment directory")?;
    let name = format!("segment-{sequence:010}.parquet");
    let path = directory.join(name);
    let temporary = path.with_extension("parquet.tmp");
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
        .build();
    let file = File::create(&temporary).context("create temporary Parquet segment")?;
    let mut writer =
        ArrowWriter::try_new(file, schema, Some(properties)).context("create Parquet writer")?;
    writer.write(&batch).context("write Parquet row group")?;
    writer.close().context("close Parquet segment")?;
    std::fs::rename(&temporary, &path).context("publish Parquet segment")?;
    let bytes = std::fs::metadata(&path)?.len();
    Ok(Segment {
        hour,
        market,
        table,
        path,
        rows: records.len(),
        bytes,
    })
}

fn schema_for(table: &str) -> Result<Arc<Schema>> {
    static SCHEMAS: OnceLock<HashMap<String, Arc<Schema>>> = OnceLock::new();
    if let Some(schema) = SCHEMAS.get().and_then(|schemas| schemas.get(table)) {
        return Ok(Arc::clone(schema));
    }
    let schemas = parse_schemas()?;
    let schema = schemas
        .get(table)
        .cloned()
        .with_context(|| format!("unknown table schema {table}"));
    let _ = SCHEMAS.set(schemas);
    schema
}

fn parse_schemas() -> Result<HashMap<String, Arc<Schema>>> {
    let mut schemas = HashMap::new();
    for statement in include_str!("schema.sql").split(';') {
        let statement = statement.trim();
        let Some(rest) = statement.strip_prefix("CREATE TABLE IF NOT EXISTS ") else {
            continue;
        };
        let Some((table, columns)) = rest.split_once(" (") else {
            continue;
        };
        let columns = columns.trim_end_matches(')');
        let mut fields = Vec::new();
        for column in split_columns(columns) {
            let Some((name, sql_type)) = column.trim().split_once(' ') else {
                bail!("invalid schema column {column}");
            };
            fields.push(Field::new(name, arrow_type(sql_type)?, true));
        }
        schemas.insert(table.to_owned(), Arc::new(Schema::new(fields)));
    }
    Ok(schemas)
}

fn split_columns(value: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, character) in value.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
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

fn arrow_type(value: &str) -> Result<DataType> {
    Ok(match value {
        "TIMESTAMPTZ" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "VARCHAR" | "JSON" => DataType::Utf8,
        "UBIGINT" => DataType::UInt64,
        "BIGINT" => DataType::Int64,
        "INTEGER" => DataType::Int32,
        "DECIMAL(38,18)" => DataType::Decimal128(38, 18),
        "BOOLEAN" => DataType::Boolean,
        other => bail!("unsupported schema type {other}"),
    })
}

fn record_batch(schema: Arc<Schema>, records: &[Record]) -> Result<RecordBatch> {
    for record in records {
        if record.values.len() != schema.fields().len() {
            bail!("{} field count mismatch", record.table);
        }
    }
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| column_array(records, index, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).context("build Arrow record batch")
}

fn column_array(records: &[Record], index: usize, data_type: &DataType) -> Result<ArrayRef> {
    let array: ArrayRef = match data_type {
        DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
            TimestampMicrosecondArray::from(
                records
                    .iter()
                    .map(|record| timestamp(&record.values[index]))
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        DataType::Utf8 => Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| string(&record.values[index]))
                .collect::<Vec<_>>(),
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from(
            records
                .iter()
                .map(|record| unsigned(&record.values[index]))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| signed(&record.values[index]))
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            records
                .iter()
                .map(|record| integer(&record.values[index]))
                .collect::<Vec<_>>(),
        )),
        DataType::Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from(
                records
                    .iter()
                    .map(|record| decimal(&record.values[index]))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(*precision, *scale)?,
        ),
        DataType::Boolean => Arc::new(BooleanArray::from(
            records
                .iter()
                .map(|record| boolean(&record.values[index]))
                .collect::<Vec<_>>(),
        )),
        other => bail!("unsupported Arrow type {other}"),
    };
    Ok(array)
}

fn timestamp(value: &Value) -> Option<i64> {
    match value {
        Value::TimestampMicros(v) => Some(*v),
        Value::Null => None,
        _ => None,
    }
}
fn string(value: &Value) -> Option<&str> {
    match value {
        Value::Text(v) => Some(v),
        Value::StaticText(v) => Some(v),
        Value::Null => None,
        _ => None,
    }
}
fn unsigned(value: &Value) -> Option<u64> {
    match value {
        Value::U64(v) => Some(*v),
        Value::Null => None,
        _ => None,
    }
}
fn signed(value: &Value) -> Option<i64> {
    match value {
        Value::I64(v) => Some(*v),
        Value::Null => None,
        _ => None,
    }
}
fn integer(value: &Value) -> Option<i32> {
    signed(value).and_then(|value| i32::try_from(value).ok())
}
fn decimal(value: &Value) -> Option<i128> {
    match value {
        Value::Decimal(v) => Some(*v),
        Value::Null => None,
        _ => None,
    }
}
fn boolean(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(v) => Some(*v),
        Value::Null => None,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    use crate::model::{decimal as data_decimal, text, timestamp};

    #[test]
    fn schema_registry_contains_every_created_table() -> Result<()> {
        let schemas = parse_schemas()?;
        assert_eq!(schemas.len(), 20);
        Ok(())
    }

    #[test]
    fn decimal_comma_does_not_split_a_column() {
        assert_eq!(
            split_columns("price DECIMAL(38,18), quantity UBIGINT").len(),
            2
        );
    }

    #[test]
    fn written_segment_can_be_read_back() -> Result<()> {
        let directory = tempdir()?;
        let hour = Utc
            .timestamp_opt(1_785_715_200, 0)
            .single()
            .context("test hour")?;
        let records = vec![Record {
            table: "trades",
            values: vec![
                timestamp(hour),
                timestamp(hour),
                text("BTCUSDT"),
                Value::U64(42),
                data_decimal(100_000_000_000_000_000_000),
                data_decimal(1_000_000_000_000_000_000),
                timestamp(hour),
                Value::Boolean(true),
                Value::Boolean(true),
                text("history-test"),
            ],
            target_market: None,
        }];
        let segment = write_segment(
            directory.path(),
            "binance",
            Market::Spot,
            hour,
            1,
            "trades",
            &records,
        )?;
        let file = File::open(segment.path)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let batch = reader.next().context("missing record batch")??;
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }
}
