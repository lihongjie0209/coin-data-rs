use std::sync::Arc;

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

use crate::{archive::Archiver, runtime::Metrics, writer::Writer};

#[derive(Clone)]
pub struct ApiState {
    pub writer: Writer,
    pub metrics: Arc<Metrics>,
    pub archiver: Arc<Archiver>,
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/stats", get(stats))
        .route("/v1/sql", post(sql))
        .route("/v1/archive", post(archive))
        .with_state(state)
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

async fn stats(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!({
        "runtime": state.metrics.snapshot(),
        "database": state.writer.stats().await?,
    })))
}

#[derive(Deserialize)]
struct SQLRequest {
    sql: String,
}

async fn sql(
    State(state): State<ApiState>,
    Json(request): Json<SQLRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.writer.query(request.sql).await?))
}

#[derive(Deserialize)]
struct ArchiveRequest {
    hour: DateTime<Utc>,
    #[serde(default)]
    force: bool,
}

async fn archive(
    State(state): State<ApiState>,
    Json(request): Json<ArchiveRequest>,
) -> Result<Json<Value>, ApiError> {
    let keys = state.archiver.export(request.hour, request.force).await?;
    Ok(Json(json!({"uploaded": keys})))
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
