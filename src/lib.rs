#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod api;
pub mod archive;
mod binance_json;
pub mod collector;
pub mod compactor;
pub mod config;
pub mod futures_parser;
pub mod futures_poll;
pub mod model;
pub mod notify;
pub mod parquet_store;
pub mod parser;
pub mod rate_limit;
pub mod runtime;
pub mod writer;
