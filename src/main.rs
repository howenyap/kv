use kv::server::Server;

#[tokio::main]
async fn main() {
    if let Err(e) = Server::run(3000).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
