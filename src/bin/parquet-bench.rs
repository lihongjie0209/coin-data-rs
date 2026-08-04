use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use arrow_schema::Schema;
use clap::Parser;
use coin_data_rs::compactor::reorder_batch;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};

#[derive(Parser)]
struct Options {
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 60)]
    seconds: u64,
    #[arg(long, default_value_t = 1)]
    zstd_level: i32,
    #[arg(long, default_value_t = 131_072)]
    row_group_rows: usize,
    #[arg(long)]
    sort_by_symbol: bool,
}

fn main() -> Result<()> {
    let options = Options::parse();
    if options.row_group_rows == 0 {
        bail!("row-group-rows must be positive");
    }
    let sources = parquet_files(&options.source)?;
    if sources.is_empty() {
        bail!("no Parquet history found in {}", options.source.display());
    }
    std::fs::create_dir_all(&options.output)?;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(options.seconds);
    let mut rows = 0u64;
    let mut input_bytes = 0u64;
    let mut output_bytes = 0u64;
    let mut files = 0u64;
    while Instant::now() < deadline {
        for source in &sources {
            if Instant::now() >= deadline {
                break;
            }
            let result = rewrite(
                source,
                &options.output,
                files,
                options.zstd_level,
                options.row_group_rows,
                options.sort_by_symbol,
            )?;
            rows = rows.saturating_add(result.rows);
            input_bytes = input_bytes.saturating_add(result.input_bytes);
            output_bytes = output_bytes.saturating_add(result.output_bytes);
            files = files.saturating_add(1);
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "{}",
        serde_json::json!({
            "elapsed_seconds": elapsed,
            "files": files,
            "rows": rows,
            "rows_per_second": rows as f64 / elapsed,
            "input_bytes": input_bytes,
            "output_bytes": output_bytes,
            "output_mib_per_second": output_bytes as f64 / 1_048_576.0 / elapsed,
            "zstd_level": options.zstd_level,
            "row_group_rows": options.row_group_rows,
            "sort_by_symbol": options.sort_by_symbol,
        })
    );
    Ok(())
}

struct RewriteResult {
    rows: u64,
    input_bytes: u64,
    output_bytes: u64,
}

fn rewrite(
    source: &Path,
    output: &Path,
    sequence: u64,
    zstd_level: i32,
    row_group_rows: usize,
    sort_by_symbol: bool,
) -> Result<RewriteResult> {
    let input = File::open(source)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(input)?;
    let schema: Arc<Schema> = builder.schema().clone();
    let mut reader = builder.with_batch_size(65_536).build()?;
    let path = output.join(format!("bench-{sequence:08}.parquet"));
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(zstd_level)?))
        .set_max_row_group_row_count(Some(row_group_rows))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(&path)?, schema, Some(properties))?;
    let mut rows = 0u64;
    for batch in &mut reader {
        let batch = batch.context("read historical Parquet batch")?;
        rows = rows.saturating_add(batch.num_rows() as u64);
        if sort_by_symbol {
            writer.write(&reorder_batch(&batch)?)?;
        } else {
            writer.write(&batch)?;
        }
    }
    writer.close()?;
    Ok(RewriteResult {
        rows,
        input_bytes: std::fs::metadata(source)?.len(),
        output_bytes: std::fs::metadata(path)?.len(),
    })
}

fn parquet_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("parquet") {
                files.push(path);
            }
        }
    }
    files.sort_unstable();
    Ok(files)
}
