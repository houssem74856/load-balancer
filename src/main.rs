use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use tokio::net::TcpStream;

async fn handle_connection(
    req: Request<hyper::body::Incoming>,
    backend_addr: Arc<String>,
) -> Result<Response<hyper::body::Incoming>, Box<dyn std::error::Error + Send + Sync>> {
    println!("{:?}", req);

    let stream = tokio::time::timeout(
        Duration::from_secs(3),
        TcpStream::connect(backend_addr.as_str()),
    )
    .await??;
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

    let backend_addr = Arc::new("localhost:8000".to_string());

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let backend_addr = Arc::clone(&backend_addr);

        tokio::task::spawn(async move {
            let service = service_fn(move |req| handle_connection(req, backend_addr.clone()));

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
