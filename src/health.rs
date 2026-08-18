use crate::{backend::Backend, pool::ConnectionPools};

use http_body_util::{BodyExt, Empty};
use hyper::body::{Bytes, Incoming};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::TcpStream;

const HEALTH_CHECK_PATH: &str = "/";
const HEALTH_CHECK_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const HEALTH_CHECK_RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);
const CONSECUTIVE_SUCCESSES_TO_MARK_HEALTHY: u8 = 3;
const CONSECUTIVE_FAILURES_TO_MARK_UNHEALTHY: u8 = 1;

fn response_validator(res: Response<Incoming>) -> bool {
    res.status().is_success()
}

async fn http_health_check(backend_addr: &str) -> bool {
    let stream = match tokio::time::timeout(
        HEALTH_CHECK_CONNECT_TIMEOUT,
        TcpStream::connect(backend_addr),
    )
    .await
    {
        Ok(Ok(s)) => s,
        _ => return false,
    };
    let io = TokioIo::new(stream);
    let (mut sender, conn) = match hyper::client::conn::http1::handshake(io).await {
        Ok((s, conn)) => (s, conn),
        Err(_) => return false,
    };

    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            eprintln!("Connection failed: {:?}", err);
        }
    });

    let req = Request::builder()
        .method("GET")
        .uri(HEALTH_CHECK_PATH)
        .header("Host", backend_addr)
        .body(Empty::<Bytes>::new().map_err(|e| match e {}).boxed())
        .unwrap();

    match tokio::time::timeout(HEALTH_CHECK_RESPONSE_TIMEOUT, sender.send_request(req)).await {
        Ok(Ok(res)) => response_validator(res),
        _ => false,
    }
}

pub async fn start_health_checker(
    backends: Arc<Vec<Backend>>,
    interval: Duration,
    connection_pools: Arc<ConnectionPools>,
) {
    let mut ticker = tokio::time::interval(interval);

    loop {
        ticker.tick().await;

        let mut handles = vec![];

        for backend_addr in backends.iter().map(|backend| backend.addr.clone()) {
            let handle = tokio::spawn(async move {
                let healthy = http_health_check(&backend_addr).await;

                healthy
            });

            handles.push(handle);
        }

        for (backend, handle) in backends.iter().zip(handles) {
            let passed = handle.await.unwrap();
            let was_healthy = backend.healthy.load(Ordering::Relaxed);
            let consecutive_successes = backend
                .health_check_consecutive_successes
                .load(Ordering::Relaxed);
            let consecutive_failures = backend
                .health_check_consecutive_failures
                .load(Ordering::Relaxed);

            if passed {
                if !was_healthy {
                    let new_consecutive_successes = consecutive_successes.saturating_add(1);
                    backend
                        .health_check_consecutive_successes
                        .store(new_consecutive_successes, Ordering::Relaxed);
                    if new_consecutive_successes >= CONSECUTIVE_SUCCESSES_TO_MARK_HEALTHY {
                        println!("backend {} {}", backend.addr, "is back up");

                        backend.healthy.store(true, Ordering::Relaxed);
                        backend
                            .health_check_consecutive_successes
                            .store(0, Ordering::Relaxed);
                        backend
                            .health_check_consecutive_failures
                            .store(0, Ordering::Relaxed);
                        backend.consecutive_failures.store(0, Ordering::Relaxed);
                    }
                }
            } else {
                if was_healthy {
                    let new_consecutive_failures = consecutive_failures.saturating_add(1);
                    backend
                        .health_check_consecutive_failures
                        .store(new_consecutive_failures, Ordering::Relaxed);
                    if new_consecutive_failures >= CONSECUTIVE_FAILURES_TO_MARK_UNHEALTHY {
                        eprintln!("backend {} {}", backend.addr, "went down");

                        backend.healthy.store(false, Ordering::Relaxed);
                        backend
                            .health_check_consecutive_successes
                            .store(0, Ordering::Relaxed);
                        backend
                            .health_check_consecutive_failures
                            .store(0, Ordering::Relaxed);
                        connection_pools
                            .empty_backend_connections(&backend.addr)
                            .await
                    }
                }
            }
        }
    }
}
