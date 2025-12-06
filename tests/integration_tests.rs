use crate::common::spawn_server;
use reqwest::StatusCode;
use serde_json::json;
use std::time::Instant;

mod common;

#[tokio::test]
async fn test_get_and_put() {
    let address = spawn_server().await.expect("failed to spawn server");
    let client = reqwest::Client::new();
    let file = include_str!("put.txt");
    let mut latencies = vec![];

    for line in file.lines() {
        let mut parts = line.split_whitespace();
        let method = parts.next().expect("missing method");
        let key = parts.next().expect("missing key");
        let url = format!("http://{address}/{key}");

        let latency = match method {
            "PUT" => {
                let value: u32 = parts
                    .next()
                    .expect("missing value")
                    .parse()
                    .expect("invalid number");
                let body = json!({"value": value});
                let start = Instant::now();
                let response = client
                    .put(url)
                    .json(&body)
                    .send()
                    .await
                    .expect("failed to sent put request");
                let latency = start.elapsed();

                assert_eq!(StatusCode::OK, response.status());
                latency
            }
            "GET" => {
                let expected_response = parts.next().expect("missing response");
                let start = Instant::now();
                let response = client
                    .get(url)
                    .send()
                    .await
                    .expect("failed to sent put request");
                let latency = start.elapsed();

                match expected_response {
                    "NOT_FOUND" => {
                        assert_eq!(StatusCode::NOT_FOUND, response.status());
                    }
                    expected_value => {
                        assert_eq!(StatusCode::OK, response.status());

                        let json: serde_json::Value =
                            response.json().await.expect("failed to deserialize json");

                        let value = json
                            .get("value")
                            .expect("missing value field")
                            .as_u64()
                            .expect("value is not a number")
                            .to_string();

                        assert_eq!(expected_value, value);
                    }
                }

                latency
            }
            _ => unreachable!(),
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
