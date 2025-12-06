use kv::Server;
use tokio::net::TcpListener;

pub async fn spawn_server() -> Result<String, std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let router = Server::router();

    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("error serving when spawning server")
    });

    Ok(address)
}
