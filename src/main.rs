use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use tokio::net::TcpStream;

static INDEX: AtomicUsize = AtomicUsize::new(0);

async fn handle_connection(
    req: Request<hyper::body::Incoming>,
    backend_addrs: Arc<Vec<String>>,
) -> Result<Response<hyper::body::Incoming>, Box<dyn std::error::Error + Send + Sync>> {
    println!("{:?}", req);

    let backend_addr = &backend_addrs[INDEX.fetch_add(1, Ordering::Relaxed) % backend_addrs.len()];
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;

    let backend_addrs = Arc::new(
        vec!["localhost:8000", "localhost:8001", "localhost:8002"]
            .into_iter()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>(),
    );

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let backend_addrs = Arc::clone(&backend_addrs);

        tokio::task::spawn(async move {
            let service = service_fn(move |req| handle_connection(req, Arc::clone(&backend_addrs)));

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
