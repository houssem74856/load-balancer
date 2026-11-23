mod common;

use common::{MockBackend, start_lb, start_lb_with_config};
use load_balancer::Config;

use std::time::Duration;

fn total_attempts(backends: &[MockBackend]) -> usize {
    backends.iter().map(|b| b.request_count()).sum()
}

#[tokio::test]
async fn get_is_retried_on_another_backend() {
    let closing_backend = MockBackend::start_a_closing_backend("closing_backend").await;
    let backend_2 = MockBackend::start_a_normal_backend("backend_2").await;

    let lb_addr = start_lb(vec![
        closing_backend.address.clone(),
        backend_2.address.clone(),
    ])
    .await;

    let client = reqwest::Client::new();
    for _ in 0..10 {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    assert!(closing_backend.request_count() >= 1);
    assert_eq!(backend_2.request_count(), 10);
}

#[tokio::test]
async fn post_is_not_retried() {
    let closing_backend = MockBackend::start_a_closing_backend("closing_backend").await;
    let backend_2 = MockBackend::start_a_normal_backend("backend_2").await;

    let lb_addr = start_lb(vec![
        closing_backend.address.clone(),
        backend_2.address.clone(),
    ])
    .await;

    let client = reqwest::Client::new();
    let res = client
        .post(format!("http://{lb_addr}/data"))
        .body("hello")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 502);
    assert_eq!(closing_backend.request_count(), 1);
    assert_eq!(backend_2.request_count(), 0);
}

#[tokio::test]
async fn retries_stop_when_retry_budget_is_exhausted() {
    let mut backends = Vec::new();
    for _ in 0..10 {
        backends.push(MockBackend::start_a_closing_backend("closing_backend").await);
    }

    let lb_addr = start_lb(backends.iter().map(|b| b.address.clone()).collect()).await;

    let client = reqwest::Client::new();

    /*budget starts at 100 tokens, a retry costs 10 and every request deposits 1 (apart from the first cuz budget is at its max),
    so the first 5 requests can retry twice each (3 attempts each so 2 retries)*/
    for _ in 0..5 {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 502);
    }
    assert_eq!(total_attempts(&backends), 15);

    //only 4 tokens left, so a request can't afford a retry and gets a single attempt
    for _ in 0..5 {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 502);
    }
    assert_eq!(total_attempts(&backends), 20);
}

#[tokio::test]
async fn hanging_backend_returns_504() {
    let hanging_backend = MockBackend::start_a_hanging_backend("hanging_backend").await;

    let config = Config {
        request_timeout: Duration::from_millis(200),
        ..Config::default()
    };
    let (lb_addr, _) = start_lb_with_config(vec![hanging_backend.address.clone()], config).await;

    let client = reqwest::Client::new();
    let res = client
        .post(format!("http://{lb_addr}/data"))
        .body("hello")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 504);
    assert_eq!(hanging_backend.request_count(), 1);
}

#[tokio::test]
async fn backend_is_marked_unhealthy_after_three_failures() {
    let closing_backend = MockBackend::start_a_closing_backend("closing_backend").await;
    let backend_2 = MockBackend::start_a_normal_backend("backend_2").await;

    let lb_addr = start_lb(vec![
        closing_backend.address.clone(),
        backend_2.address.clone(),
    ])
    .await;

    let client = reqwest::Client::new();
    for _ in 0..10 {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    assert_eq!(closing_backend.request_count(), 3);
    assert_eq!(backend_2.request_count(), 10);
}
