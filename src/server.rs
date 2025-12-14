use crate::routes::{get_key, hello, put_key};
use crate::{error::Result, memtable::MemTable};
use axum::{
    Router,
    routing::{get, put},
};
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;

#[derive(Clone, Default)]
pub struct AppState {
    buckets: Arc<RwLock<MemTable>>,
}

impl AppState {
    pub fn buckets(&self) -> &Arc<RwLock<MemTable>> {
        &self.buckets
    }

    pub fn startup(&self) -> Result<()> {
        self.buckets.write().unwrap().startup()
    }
}

#[derive(Default)]
pub struct Server;

impl Server {
    pub fn router() -> Result<Router> {
        let app_state = AppState::default();
        app_state.startup()?;

        Ok(Router::new()
            .route("/", get(hello))
            .route("/{key}", get(get_key))
            .route("/{key}", put(put_key))
            .with_state(app_state))
    }

    pub async fn run(port: u16) -> Result<()> {
        let address = format!("127.0.0.1:{port}");
        let listener = TcpListener::bind(address).await?;

        let router = Self::router()?;

        axum::serve(listener, router).await?;

        Ok(())
    }
}
