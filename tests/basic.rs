mod common;

use common::{MockBackend, start_lb};

use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::test]
async fn round_robin_splits_evenly() {
    let backend_1 = MockBackend::start_a_normal_backend("backend_1").await;
    let backend_2 = MockBackend::start_a_normal_backend("backend_2").await;

    let lb_addr = start_lb(vec![backend_1.address.clone(), backend_2.address.clone()]).await;

    let client = reqwest::Client::new();
    for _ in 0..10 {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }

    assert_eq!(backend_1.request_count(), 5);
    assert_eq!(backend_2.request_count(), 5);
}

#[tokio::test]
async fn dead_backend_is_skipped() {
    let backend_1 = MockBackend::start_a_normal_backend("backend_1").await;
    //bind to get a free port, then drop the listener so nothing is listening there anymore
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let dead_listener = TcpListener::bind(addr).await.unwrap();
    let dead_address = dead_listener.local_addr().unwrap().to_string();
    drop(dead_listener);

    let lb_addr = start_lb(vec![dead_address, backend_1.address.clone()]).await;

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
}
