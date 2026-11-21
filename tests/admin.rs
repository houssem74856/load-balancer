mod common;

use common::MockBackend;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start_lb(backends_addresses: Vec<String>) -> (SocketAddr, SocketAddr) {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let lb_listener = TcpListener::bind(addr).await.unwrap();
    let admin_listener = TcpListener::bind(addr).await.unwrap();

    let lb_addr = lb_listener.local_addr().unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();

    tokio::spawn(load_balancer::run(
        lb_listener,
        admin_listener,
        backends_addresses,
    ));

    (lb_addr, admin_addr)
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
    let backend_1 = MockBackend::start("backend_1").await;
    let backend_2 = MockBackend::start("backend_2").await;

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
    let backend_1 = MockBackend::start("backend_1").await;

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
    let backend_1 = MockBackend::start("backend_1").await;
    let backend_2 = MockBackend::start("backend_2").await;

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
    let backend_1 = MockBackend::start("backend_1").await;
    let backend_2 = MockBackend::start("backend_2").await;

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
