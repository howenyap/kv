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

#[derive(Serialize)]
pub struct ValueResponse {
    pub value: Value,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub async fn put_key(
    Path(key): Path<Key>,
    State(state): State<AppState>,
    Json(payload): Json<PutKeyRequest>,
) -> StatusCode {
    match state.buckets().write().unwrap().put(key, payload.value) {
        Ok(_) => StatusCode::OK,
        Err(e) => {
            println!("[ERROR] put: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

pub async fn get_key(Path(key): Path<Key>, State(state): State<AppState>) -> impl IntoResponse {
    let Ok(value) = state.buckets().read().unwrap().get(&key) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "Internal server error".to_string(),
            }),
        )
            .into_response();
    };

    match value {
        Some(value) => (StatusCode::OK, Json(ValueResponse { value })).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Not found: {key}"),
            }),
        )
            .into_response(),
    }
}

pub async fn delete_key(Path(key): Path<Key>, State(state): State<AppState>) -> StatusCode {
    match state.buckets().write().unwrap().delete(&key) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
