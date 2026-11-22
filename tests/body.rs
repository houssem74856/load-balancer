mod common;

use common::MockBackend;
use reqwest::Method;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start_lb(backends_addresses: Vec<String>) -> SocketAddr {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let lb_listener = TcpListener::bind(addr).await.unwrap();
    let admin_listener = TcpListener::bind(addr).await.unwrap();

    let lb_addr = lb_listener.local_addr().unwrap();

    tokio::spawn(load_balancer::run(
        lb_listener,
        admin_listener,
        backends_addresses,
    ));

    lb_addr
}

#[tokio::test]
async fn request_body_arrives_intact() {
    let echo_backend = MockBackend::start_an_echo_backend("echo_backend").await;

    let lb_addr = start_lb(vec![echo_backend.address.clone()]).await;

    /*lb buffers incoming bodies under 1MB in memory, else it streams the body to a backend directly, we
    are testing here different sizes around the 1MB cap (this only apply to retriable methods though like
    put, for non retriable like post it gets streamed directly no matter what, the reason is idempotency)*/
    let sizes: [usize; 5] = [0, 1, 1024 * 1024, 1024 * 1024 + 1, 5 * 1024 * 1024];

    let client = reqwest::Client::new();
    for method in [Method::PUT, Method::POST] {
        for size in sizes {
            //not all the same byte, so reordered or cut data gets noticed (clever)
            let body = (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>();

            let res = client
                .request(method.clone(), format!("http://{lb_addr}/data"))
                .body(body.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(res.status(), 200);

            //assert! and not assert_eq! so a failure doesn't print 5MB of bytes
            assert!(
                res.bytes().await.unwrap() == body,
                "{method} body of {size} bytes came back different"
            );
        }
    }
}

#[tokio::test]
async fn retried_put_sends_the_full_body_again() {
    let closing_backend = MockBackend::start_a_closing_backend("closing_backend").await;
    let echo_backend = MockBackend::start_an_echo_backend("echo_backend").await;

    let lb_addr = start_lb(vec![
        closing_backend.address.clone(),
        echo_backend.address.clone(),
    ])
    .await;

    let client = reqwest::Client::new();
    let res = client
        .put(format!("http://{lb_addr}/data"))
        .body("hello world")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
    assert_eq!(res.text().await.unwrap(), "hello world");
    assert_eq!(closing_backend.request_count(), 1);
    assert_eq!(echo_backend.request_count(), 1);
}

#[tokio::test]
async fn chunked_request_body_arrives_intact() {
    let echo_backend = MockBackend::start_an_echo_backend("echo_backend").await;

    let lb_addr = start_lb(vec![echo_backend.address.clone()]).await;

    let mut stream = TcpStream::connect(lb_addr).await.unwrap();
    stream
        .write_all(
            b"PUT /data HTTP/1.1\r\n\
              Host: localhost\r\n\
              Transfer-Encoding: chunked\r\n\
              Connection: close\r\n\
              \r\n\
              5\r\nhello\r\n\
              6\r\n world\r\n\
              0\r\n\r\n",
        )
        .await
        .unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();

    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.ends_with("hello world"));
}

#[tokio::test]
async fn headers_are_forwarded_both_ways() {
    let echo_backend = MockBackend::start_an_echo_backend("echo_backend").await;

    let lb_addr = start_lb(vec![echo_backend.address.clone()]).await;

    let client = reqwest::Client::new();
    let res = client
        .get(format!("http://{lb_addr}/data"))
        .header("x-echo", "hello")
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
    assert_eq!(res.headers().get("x-echo").unwrap(), "hello");
}

#[tokio::test]
async fn backend_error_status_is_passed_through_and_not_retried() {
    let failing_backend = MockBackend::start_a_backend_with_status("failing_backend", 500).await;
    let backend_2 = MockBackend::start_a_normal_backend("backend_2").await;

    let lb_addr = start_lb(vec![
        failing_backend.address.clone(),
        backend_2.address.clone(),
    ])
    .await;

    let client = reqwest::Client::new();
    let mut status_500_count = 0;
    for _ in 0..10 {
        let res = client
            .get(format!("http://{lb_addr}/data"))
            .send()
            .await
            .unwrap();
        if res.status() == 500 {
            status_500_count += 1;
        } else {
            assert_eq!(res.status(), 200);
        }
    }

    //a 500 is a valid response so it shouldn't count toward marking the backend unhealthy,and round robin should keep splitting evenly
    assert_eq!(status_500_count, 5);
    assert_eq!(failing_backend.request_count(), 5);
    assert_eq!(backend_2.request_count(), 5);
}
