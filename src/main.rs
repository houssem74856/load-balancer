use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use tokio::net::TcpStream;

fn error_response(status: u16, msg: &str) -> Response<BoxBody<Bytes, hyper::Error>> {
    Response::builder()
        .status(status)
        .body(
            Full::new(Bytes::from(msg.to_string()))
                .map_err(|e| match e {})
                .boxed(),
        )
        .unwrap()
}

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

struct PooledConnection {
    sender: SendRequest<Incoming>,
    last_used: Instant,
}

struct ConnectionPools {
    pools: HashMap<String, Arc<Mutex<Vec<PooledConnection>>>>,
    max_idle_per_host: usize,
    idle_timeout: Duration,
}

impl ConnectionPools {
    fn new(
        max_idle_per_host: usize,
        idle_timeout: Duration,
        backends_addresses: Vec<&str>,
    ) -> Self {
        let mut pools = HashMap::new();

        for addr in backends_addresses {
            pools.insert(addr.to_string(), Arc::new(Mutex::new(Vec::new())));
        }

        ConnectionPools {
            pools,
            max_idle_per_host,
            idle_timeout,
        }
    }

    async fn get_connection(
        &self,
        backend_addr: &String,
    ) -> Result<SendRequest<Incoming>, Box<dyn std::error::Error + Send + Sync>> {
        let connections_mutex = self.pools.get(backend_addr).unwrap();
        /*note:
        problem: was holding mutex accross an async call (sender.ready().await) which would block other requests from accessing the vec.
        solution: drop right after poping a connection then testing if it is ready separately if it isn't then reacuire the lock again,
        */
        let mut option_pooled_connection = {
            let mut connections = connections_mutex.lock().unwrap();
            connections.retain(|c| c.last_used.elapsed() < self.idle_timeout);
            connections.pop()
        };

        while let Some(mut pooled) = option_pooled_connection {
            if pooled.sender.ready().await.is_ok() {
                return Ok(pooled.sender);
            }

            option_pooled_connection = {
                let mut connections = connections_mutex.lock().unwrap();
                connections.pop()
            };
        }

        let stream = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(backend_addr))
            .await??;
        let io = TokioIo::new(stream);
        let (sender, conn) = hyper::client::conn::http1::handshake(io).await?;

        tokio::task::spawn(async move {
            if let Err(err) = conn.await {
                eprintln!("Connection failed: {:?}", err);
            }
        });

        Ok(sender)
    }

    async fn return_connection(&self, backend_addr: &String, sender: SendRequest<Incoming>) {
        let mut connections = self.pools.get(backend_addr).unwrap().lock().unwrap();

        if connections.len() < self.max_idle_per_host {
            connections.push(PooledConnection {
                sender,
                last_used: Instant::now(),
            });
        }
    }

    async fn empty_backend_connections(&self, backend_addr: &String) {
        if let Some(connections_mutex) = self.pools.get(backend_addr) {
            let mut connections = connections_mutex.lock().unwrap();
            connections.clear();
        }
    }
}

async fn handle_connection(
    req: Request<hyper::body::Incoming>,
    backends: Arc<Vec<Backend>>,
    connection_pools: Arc<ConnectionPools>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>> {
    //note: using BoxBody so I can return Incoming (streaming) to the client, while also being able to return Full responses for stuff like no healthy backend and stuff, which can't be done with Incoming only
    println!("{:?}", req);

    let healthy_backends = backends
        .iter()
        .filter(|b| b.healthy.load(Ordering::Relaxed))
        .collect::<Vec<_>>();

    if healthy_backends.is_empty() {
        eprintln!("no healthy backend available");
        return Ok(error_response(503, "no healthy backend available"));
    }

    let backend_addr =
        &healthy_backends[INDEX.fetch_add(1, Ordering::Relaxed) % healthy_backends.len()].addr;
    println!("routing to: {}", backend_addr);

    let mut sender = match connection_pools.get_connection(backend_addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to get connection to {}: {:?}", backend_addr, e);
            return Ok(error_response(502, "could not connect to backend"));
        }
    };

    let res = match tokio::time::timeout(Duration::from_secs(5), sender.send_request(req)).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            eprintln!("sending request to {} failed: {:?}", backend_addr, e);
            return Ok(error_response(
                502,
                "backend closed connection or failed mid-request",
            ));
        }
        Err(_) => {
            eprintln!("request to {} timed out", backend_addr);
            return Ok(error_response(504, "request to backend timed out"));
        }
    };

    connection_pools
        .return_connection(backend_addr, sender)
        .await;

    println!("response status: {}", res.status());

    Ok(res.map(|b| b.boxed()))
}

async fn start_health_checker(
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;

    let backends_addresses = vec!["localhost:8000", "localhost:8001", "localhost:8002"];
    let backends = Arc::new(
        backends_addresses
            .iter()
            .map(|&addr| Backend::new(addr))
            .collect::<Vec<_>>(),
    );

    let max_idle_per_host = 100;
    let idle_timeout = Duration::from_secs(30);
    let connection_pools = Arc::new(ConnectionPools::new(
        max_idle_per_host,
        idle_timeout,
        backends_addresses,
    ));

    tokio::spawn(start_health_checker(
        Arc::clone(&backends),
        Duration::from_secs(5),
        Arc::clone(&connection_pools),
    ));

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let backends = Arc::clone(&backends);
        let connection_pools = Arc::clone(&connection_pools);

        tokio::task::spawn(async move {
            let service = service_fn(move |req| {
                handle_connection(req, Arc::clone(&backends), Arc::clone(&connection_pools))
            });

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
