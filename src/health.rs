use crate::{backend::Backend, pool::ConnectionPools};

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::TcpStream;

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
                let healthy = matches!(
                    tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(backend_addr))
                        .await,
                    Ok(Ok(_))
                );

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
