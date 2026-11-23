mod common;

use common::{MockBackend, start_lb};

#[tokio::test]
async fn backend_connection_is_reused_across_requests() {
    let backend_1 = MockBackend::start_a_normal_backend("backend_1").await;

    let lb_addr = start_lb(vec![backend_1.address.clone()]).await;

    let client = reqwest::Client::new();
    for _ in 0..100 {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    assert_eq!(backend_1.request_count(), 100);
    assert_eq!(backend_1.connection_count(), 1);
}

#[tokio::test]
async fn backend_closing_after_each_response_is_handled() {
    let backend_1 = MockBackend::start_a_close_after_response_backend("backend_1").await;

    let lb_addr = start_lb(vec![backend_1.address.clone()]).await;

    let client = reqwest::Client::new();
    for _ in 0..10 {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    assert_eq!(backend_1.request_count(), 10);
    assert_eq!(backend_1.connection_count(), 10);
}
