use crate::common::{SERVER_ADDRESS, spawn_server, spawn_server_with_random_restarts};
use reqwest::StatusCode;
use serde_json::json;
use std::time::Instant;

mod common;

#[tokio::test]
async fn test_get_and_put() {
    spawn_server_with_random_restarts()
        .await
        .expect("failed to spawn server");

    let client = reqwest::Client::new();
    let file = include_str!("put.txt");
    let mut latencies = vec![];

    for line in file.lines() {
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
                _ => unreachable!(),
            };
        };

        latencies.push(latency);
    }

    latencies.sort();
    let n = latencies.len();
    let p50 = latencies[(n * 50) / 100];
    let p95 = latencies[(n * 95) / 100];
    let p99 = latencies[(n * 99) / 100];

    println!("Latency percentiles:");
    println!("  p50: {:?}", p50);
    println!("  p95: {:?}", p95);
    println!("  p99: {:?}", p99);
}
