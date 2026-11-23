mod common;

use common::{MockBackend, start_lb_with_config, wait_until};
use load_balancer::Config;

use std::time::Duration;

#[tokio::test]
async fn failing_health_checks_take_a_backend_out_and_passing_ones_bring_it_back() {
    let backend_1 = MockBackend::start_a_normal_backend("backend_1").await;

    backend_1.fail_health_checks();

    let config = Config {
        health_check_interval: Duration::from_millis(100),
        ..Config::default()
    };
    let (lb_addr, _) = start_lb_with_config(vec![backend_1.address.clone()], config).await;

    let client = reqwest::Client::new();

    wait_until(async || {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        res.status() == 503
    })
    .await;

    //no traffic reaches it while it is unhealthy
    let requests_before = backend_1.request_count();
    for _ in 0..5 {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 503);
    }
    assert_eq!(backend_1.request_count(), requests_before);

    //it takes 3 passing health checks in a row to come back
    backend_1.pass_health_checks();
    wait_until(async || {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        res.status() == 200
    })
    .await;
}
