mod common;

use common::{MockBackend, start_lb_with_config};
use load_balancer::Config;

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn start_lb(backends_addresses: Vec<String>) -> (SocketAddr, SocketAddr) {
    start_lb_with_config(backends_addresses, Config::default()).await
}

async fn send_admin_command(admin_addr: SocketAddr, command: &str) -> String {
    let mut stream = TcpStream::connect(admin_addr).await.unwrap();
    stream.write_all(command.as_bytes()).await.unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

async fn send_requests(lb_addr: SocketAddr, count: usize) {
    let client = reqwest::Client::new();
    for _ in 0..count {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
    }
}

#[tokio::test]
async fn disabled_backend_gets_no_traffic_until_reenabled() {
    let backend_1 = MockBackend::start_a_normal_backend("backend_1").await;
    let backend_2 = MockBackend::start_a_normal_backend("backend_2").await;

    let (lb_addr, admin_addr) =
        start_lb(vec![backend_1.address.clone(), backend_2.address.clone()]).await;

    let reply = send_admin_command(admin_addr, &format!("disable {}", backend_1.address)).await;
    assert!(reply.starts_with("success"));

    send_requests(lb_addr, 10).await;
    assert_eq!(backend_1.request_count(), 0);
    assert_eq!(backend_2.request_count(), 10);

    let reply = send_admin_command(admin_addr, &format!("enable {}", backend_1.address)).await;
    assert!(reply.starts_with("success"));

    send_requests(lb_addr, 10).await;
    assert_eq!(backend_1.request_count(), 5);
    assert_eq!(backend_2.request_count(), 15);
}

#[tokio::test]
async fn all_backends_disabled_returns_503() {
    let backend_1 = MockBackend::start_a_normal_backend("backend_1").await;

    let (lb_addr, admin_addr) = start_lb(vec![backend_1.address.clone()]).await;

    send_admin_command(admin_addr, &format!("disable {}", backend_1.address)).await;

    let res = reqwest::get(format!("http://{lb_addr}/data"))
        .await
        .unwrap();
    assert_eq!(res.status(), 503);
    assert_eq!(backend_1.request_count(), 0);
}

#[tokio::test]
async fn added_backend_receives_traffic() {
    let backend_1 = MockBackend::start_a_normal_backend("backend_1").await;
    let backend_2 = MockBackend::start_a_normal_backend("backend_2").await;

    let (lb_addr, admin_addr) = start_lb(vec![backend_1.address.clone()]).await;

    send_requests(lb_addr, 4).await;
    assert_eq!(backend_1.request_count(), 4);

    let reply = send_admin_command(admin_addr, &format!("add {}", backend_2.address)).await;
    assert!(reply.starts_with("success"));

    send_requests(lb_addr, 10).await;
    assert_eq!(backend_1.request_count(), 9);
    assert_eq!(backend_2.request_count(), 5);
}

#[tokio::test]
async fn removed_backend_stops_receiving_traffic() {
    let backend_1 = MockBackend::start_a_normal_backend("backend_1").await;
    let backend_2 = MockBackend::start_a_normal_backend("backend_2").await;

    let (lb_addr, admin_addr) =
        start_lb(vec![backend_1.address.clone(), backend_2.address.clone()]).await;

    send_requests(lb_addr, 4).await;
    assert_eq!(backend_1.request_count(), 2);
    assert_eq!(backend_2.request_count(), 2);

    let reply = send_admin_command(admin_addr, &format!("remove {}", backend_1.address)).await;
    assert!(reply.starts_with("success"));

    send_requests(lb_addr, 10).await;
    assert_eq!(backend_1.request_count(), 2);
    assert_eq!(backend_2.request_count(), 12);
}
