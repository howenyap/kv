use crate::common::{display_percentiles, run_test, setup_server};

mod common;

#[tokio::test]
async fn test_put_get() {
    setup_server(false).await.expect("failed to spawn server");

    let file = include_str!("put.txt");

    let latencies = run_test(file).await.expect("failed to run test");

    display_percentiles(&latencies);
}

// #[tokio::test]
async fn test_put_get_delete() {
    setup_server(false).await.expect("failed to spawn server");

    let file = include_str!("put-delete.txt");

    let latencies = run_test(file).await.expect("failed to run test");

    display_percentiles(&latencies);
}
