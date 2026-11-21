use http_body_util::Full;
use hyper::{Request, Response, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::net::TcpListener;

async fn handle_connection(
    req: Request<hyper::body::Incoming>,
    request_count: Arc<AtomicUsize>,
    name: &'static str,
) -> Result<Response<Full<Bytes>>, Infallible> {
    // health checker hits "/", we shouldn't count them
    if req.uri().path() != "/" {
        request_count.fetch_add(1, Ordering::Relaxed);
    }

    Ok(Response::new(Full::new(Bytes::from(name))))
}

pub struct MockBackend {
    pub address: String,
    request_count: Arc<AtomicUsize>,
}

impl MockBackend {
    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
    }

    pub async fn start(name: &'static str) -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_clone = Arc::clone(&request_count);

        tokio::task::spawn(async move {
            loop {
                let stream = match listener.accept().await {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        eprintln!("Accept error: {}", e);
                        continue;
                    }
                };
                let io = TokioIo::new(stream);
                let request_count = Arc::clone(&request_count_clone);

                tokio::task::spawn(async move {
                    let service =
                        service_fn(|req| handle_connection(req, Arc::clone(&request_count), name));

                    if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                        eprintln!("Error serving connection: {:?}", err);
                    }
                });
            }
        });

        Self {
            address,
            request_count,
        }
    }
}
