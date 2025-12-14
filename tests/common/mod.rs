use kv::error::Result;
use kv::server::Server;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub const SERVER_ADDRESS: &str = "127.0.0.1:3000";

async fn spawn_server() -> Result<JoinHandle<()>> {
    let listener = TcpListener::bind(SERVER_ADDRESS).await?;
    let router = Server::router()?;

    let handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("error serving when spawning server")
    });

    Ok(handle)
}

pub async fn spawn_server_with_random_restarts() -> Result<()> {
    tokio::spawn(async move {
        loop {
            let handle = spawn_server().await.expect("failed to spawn server");
            let random_number = rand::random_range(2..5);
            let duration = Duration::from_secs(random_number);
            tokio::time::sleep(duration).await;

            let time = Instant::now();
            println!("[{time:?}]Restarting server");
            handle.abort();
            let _ = handle.await;
        }
    });

    Ok(())
}
