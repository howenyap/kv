use axum::{
    Router,
    routing::{get, put},
};
use dashmap::DashMap;
use routes::{get_key, hello, put_key};
use std::sync::Arc;
use tokio::net::TcpListener;

pub mod routes;

pub type Key = String;
pub type Value = u32;

#[derive(Clone, Default)]
pub struct AppState {
    keys: Arc<DashMap<Key, Value>>,
}

#[derive(Default)]
pub struct Server {
    router: Router,
    address: String,
}

impl Server {
    #[allow(dead_code)]
    fn new() -> Self {
        Self::with_port(0)
    }

    pub fn with_port(port: u16) -> Self {
        let router = Self::router();
        let address = format!("127.0.0.1:{port}");

        Server { router, address }
    }

    pub fn router() -> Router {
        Router::new()
            .route("/", get(hello))
            .route("/{key}", get(get_key))
            .route("/{key}", put(put_key))
            .with_state(AppState::default())
    }

    pub async fn run(self) {
        let listener = TcpListener::bind(self.address)
            .await
            .expect("failed to bind to address");

        axum::serve(listener, self.router)
            .await
            .expect("failed to serve");
    }
}
