use crate::{AppState, Key, Value};
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
    state
        .keys
        .entry(key)
        .and_modify(|entry| *entry = payload.value)
        .or_insert(payload.value);

    StatusCode::OK
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
    match state.keys.get(&key) {
        Some(value) => (
            StatusCode::OK,
            Json(ValueResponse {
                value: *value.value(),
            }),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Not found: {key}"),
            }),
        )
            .into_response(),
    }
}

pub async fn hello() -> &'static str {
    "Hello world"
}
