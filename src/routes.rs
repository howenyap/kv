use crate::memtable::{Key, Value};
use crate::server::AppState;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct PutKeyRequest {
    value: Value,
}

pub async fn put_key(
    Path(key): Path<Key>,
    State(state): State<AppState>,
    Json(payload): Json<PutKeyRequest>,
) -> StatusCode {
    match state.buckets().write().unwrap().put(key, payload.value) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[derive(Serialize)]
pub struct ValueResponse {
    pub value: Value,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub async fn get_key(Path(key): Path<Key>, State(state): State<AppState>) -> impl IntoResponse {
    if let Some(value) = state.buckets().read().unwrap().get(&key) {
        (StatusCode::OK, Json(ValueResponse { value })).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Not found: {key}"),
            }),
        )
            .into_response()
    }
}

pub async fn hello() -> &'static str {
    "Hello world"
}
