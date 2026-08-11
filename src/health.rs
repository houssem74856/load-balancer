use crate::{backend::Backend, pool::ConnectionPools};

use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::TcpStream;

async fn http_health_check(backend_addr: &str) -> bool {
    let stream = match tokio::time::timeout(
        Duration::from_secs(3),
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
        .uri("/")
        .header("Host", backend_addr)
        .body(Empty::<Bytes>::new().map_err(|e| match e {}).boxed())
        .unwrap();

    match tokio::time::timeout(Duration::from_secs(5), sender.send_request(req)).await {
        Ok(Ok(res)) => res.status().is_success(),
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
            let healthy = handle.await.unwrap();
            let was_healthy = backend.healthy.load(Ordering::Relaxed);
            if was_healthy != healthy {
                if healthy {
                    println!("backend {} {}", backend.addr, "is back up");

                    backend.consecutive_failures.store(0, Ordering::Relaxed);
                } else {
                    eprintln!("backend {} {}", backend.addr, "went down");

                    connection_pools
                        .empty_backend_connections(&backend.addr)
                        .await
                }
            }
            backend.healthy.store(healthy, Ordering::Relaxed);
        }
    }
}
