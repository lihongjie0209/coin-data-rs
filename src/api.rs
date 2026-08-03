use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{runtime::Metrics, stream_writer::StreamWriter, uploader::Uploader};

#[derive(Clone)]
pub struct ApiState {
    pub datasets: Arc<BTreeMap<String, DatasetState>>,
}

#[derive(Clone)]
pub struct DatasetState {
    pub writer: StreamWriter,
    pub metrics: Arc<Metrics>,
    pub uploader: Uploader,
    pub directory: std::path::PathBuf,
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/stats", get(stats))
        .route("/v1/sql", post(sql))
        .route("/v1/archive", post(archive))
        .with_state(state)
}

async fn health(State(state): State<ApiState>) -> Json<Value> {
    Json(json!({"status": "ok", "datasets": state.datasets.keys().collect::<Vec<_>>()}))
}

async fn stats(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let mut result = serde_json::Map::new();
    for (name, dataset) in state.datasets.iter() {
        result.insert(
            name.clone(),
            json!({
                "runtime": dataset.metrics.snapshot(),
                "parquet": dataset.writer.stats(),
            }),
        );
    }
    Ok(Json(Value::Object(result)))
}

#[derive(Deserialize)]
struct SQLRequest {
    #[serde(default = "default_market")]
    market: String,
    sql: String,
}

fn default_market() -> String {
    "spot".to_owned()
}

async fn sql(
    State(state): State<ApiState>,
    Json(request): Json<SQLRequest>,
) -> Result<Json<Value>, ApiError> {
    let dataset = state
        .datasets
        .get(&request.market)
        .ok_or_else(|| anyhow::anyhow!("unknown market {}", request.market))?;
    let directory = dataset.directory.clone();
    let result = tokio::task::spawn_blocking(move || {
        crate::storage::Storage::query_parquet(&directory, &request.sql)
    })
    .await
    .map_err(anyhow::Error::from)??;
    Ok(Json(result))
}

#[derive(Deserialize)]
struct ArchiveRequest {
    #[serde(default = "default_market")]
    market: String,
    hour: DateTime<Utc>,
    #[serde(default)]
    force: bool,
}

async fn archive(
    State(state): State<ApiState>,
    Json(request): Json<ArchiveRequest>,
) -> Result<Json<Value>, ApiError> {
    let dataset = state
        .datasets
        .get(&request.market)
        .ok_or_else(|| anyhow::anyhow!("unknown market {}", request.market))?;
    let uploaded = dataset
        .uploader
        .scan_window(Some(request.hour), request.force)
        .await?;
    Ok(Json(
        json!({"upload": uploaded, "hour": request.hour, "force": request.force}),
    ))
}

struct ApiError(anyhow::Error);

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": self.0.to_string()})),
        )
            .into_response()
    }
}
