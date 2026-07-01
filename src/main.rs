use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use tokio::net::TcpStream;

static INDEX: AtomicUsize = AtomicUsize::new(0);

struct Backend {
    addr: String,
    healthy: AtomicBool,
}

impl Backend {
    fn new<T: Into<String>>(addr: T) -> Backend {
        Backend {
            addr: addr.into(),
            healthy: AtomicBool::new(true),
        }
    }
}

async fn handle_connection(
    req: Request<hyper::body::Incoming>,
    backends: Arc<Vec<Backend>>,
) -> Result<Response<hyper::body::Incoming>, Box<dyn std::error::Error + Send + Sync>> {
    println!("{:?}", req);

    let healthy_backends = backends
        .iter()
        .filter(|b| b.healthy.load(Ordering::Relaxed))
        .collect::<Vec<_>>();

    if healthy_backends.is_empty() {
        return Err("no healthy backend available".into());
    }

    let backend_addr =
        &healthy_backends[INDEX.fetch_add(1, Ordering::Relaxed) % healthy_backends.len()].addr;
    println!("routing to: {}", backend_addr);

    let stream =
        tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(backend_addr)).await??;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {:?}", err);
        }
    });

    let res = sender.send_request(req).await?;

    println!("response status: {}", res.status());

    Ok(res)
}

async fn start_health_checker(backends: Arc<Vec<Backend>>, interval: Duration) {
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
                println!(
                    "backend {} {}",
                    backend.addr,
                    if healthy { "is back up" } else { "went down" }
                );
            }
            backend.healthy.store(healthy, Ordering::Relaxed);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;

    let backends = Arc::new(vec![
        Backend::new("localhost:8000"),
        Backend::new("localhost:8001"),
        Backend::new("localhost:8002"),
    ]);

    tokio::spawn(start_health_checker(
        Arc::clone(&backends),
        Duration::from_secs(5),
    ));

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let backends = Arc::clone(&backends);

        tokio::task::spawn(async move {
            let service = service_fn(move |req| handle_connection(req, Arc::clone(&backends)));

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
