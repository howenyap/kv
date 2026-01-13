use kv::error::Result;
use kv::server::Server;
use reqwest::StatusCode;
use serde_json::json;
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

pub async fn setup_server(random_restarts: bool) -> Result<()> {
    tokio::spawn(async move {
        loop {
            let handle = spawn_server().await.expect("failed to spawn server");

            if !random_restarts {
                break;
            }

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

pub async fn run_test(input: &str) -> Result<Vec<Duration>> {
    let client = reqwest::Client::new();
    let mut latencies = vec![];

    for line in input.lines() {
        let parts: Vec<_> = line.split_whitespace().collect();
        let method = parts[0];
        let key = parts[1];
        let url = format!("http://{SERVER_ADDRESS}/{key}");

        let latency = loop {
            match method {
                "PUT" => {
                    let value: u32 = parts[2].parse().expect("invalid number");
                    let body = json!({"value": value});
                    let start = Instant::now();
                    let Ok(response) = client.put(&url).json(&body).send().await else {
                        continue;
                    };

                    let latency = start.elapsed();

                    if response.status() == StatusCode::OK {
                        break latency;
                    }
                }
                "GET" => {
                    let expected_response = parts[2];
                    let start = Instant::now();
                    let Ok(response) = client.get(&url).send().await else {
                        continue;
                    };
                    let latency = start.elapsed();

                    match expected_response {
                        "NOT_FOUND" => {
                            if response.status() == StatusCode::NOT_FOUND {
                                break latency;
                            }
                        }
                        expected_value => {
                            if response.status() != StatusCode::OK {
                                continue;
                            }

                            let json: serde_json::Value =
                                response.json().await.expect("failed to deserialize json");

                            let value = json
                                .get("value")
                                .expect("missing value field")
                                .as_u64()
                                .expect("value is not a number")
                                .to_string();

                            if expected_value != value {
                                continue;
                            }

                            break latency;
                        }
                    }
                }
                "DELETE" => {
                    let start = Instant::now();

                    let Ok(response) = client.delete(&url).send().await else {
                        continue;
                    };

                    let latency = start.elapsed();

                    if response.status() == StatusCode::OK {
                        break latency;
                    }

                    continue;
                }
                _ => unreachable!(),
            };
        };

        latencies.push(latency);
    }

    latencies.sort();

    Ok(latencies)
}

pub fn display_percentiles(latencies: &[Duration]) {
    let n = latencies.len();
    let p50 = latencies[(n * 50) / 100];
    let p95 = latencies[(n * 95) / 100];
    let p99 = latencies[(n * 99) / 100];

    println!("Latency percentiles:");
    println!("  p50: {:?}", p50);
    println!("  p95: {:?}", p95);
    println!("  p99: {:?}", p99);
}
