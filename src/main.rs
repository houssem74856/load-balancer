use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use http_body_util::Empty;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::{Body, Bytes, Frame, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use tokio::net::TcpStream;

struct PrefixBody {
    prefix: Option<Bytes>,
    rest: Incoming,
}

impl Body for PrefixBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        if let Some(bytes) = this.prefix.take() {
            return Poll::Ready(Some(Ok(Frame::data(bytes))));
        }

        Pin::new(&mut this.rest).poll_frame(cx)
    }
}

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

const RETRIABLE_METHODS: &[Method] = &[
    Method::GET,
    Method::HEAD,
    Method::OPTIONS,
    Method::PUT,
    Method::DELETE,
];
const MAX_BUFFER_BYTES: usize = 1024 * 1024; // 1MB
static INDEX: AtomicUsize = AtomicUsize::new(0);
const MAX_RETRY_ATTEMPTS_FOR_GET_CONNECTION: u8 = 3;
const MAX_RETRY_ATTEMPTS_FOR_SEND_RETRIABLE_BODY: u8 = 3;
const MAX_CONSECUTIVE_FAILURES_FOR_A_BACKEND: u8 = 3;

struct Backend {
    addr: String,
    healthy: AtomicBool,
    consecutive_failures: AtomicU8,
}

impl Backend {
    fn new<T: Into<String>>(addr: T) -> Backend {
        Backend {
            addr: addr.into(),
            healthy: AtomicBool::new(true),
            consecutive_failures: AtomicU8::new(0),
        }
    }
}

struct PooledConnection {
    sender: SendRequest<BoxBody<Bytes, hyper::Error>>,
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
    ) -> Result<SendRequest<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>>
    {
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

    async fn return_connection(
        &self,
        backend_addr: &String,
        sender: SendRequest<BoxBody<Bytes, hyper::Error>>,
    ) {
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

enum RequestBody {
    Retriable { bytes: Bytes },
    OneShot(BoxBody<Bytes, hyper::Error>),
}

impl RequestBody {
    fn max_attempts(&self) -> u8 {
        match self {
            RequestBody::Retriable { .. } => MAX_RETRY_ATTEMPTS_FOR_SEND_RETRIABLE_BODY,
            RequestBody::OneShot(_) => 1,
        }
    }
}

async fn handle_body(
    mut body: Incoming,
) -> Result<RequestBody, Box<dyn std::error::Error + Send + Sync>> {
    let mut buffered_bytes = Vec::new();
    let mut current_size = 0;
    let mut exceeded = false;

    while let Some(frame_res) = body.frame().await {
        let frame = frame_res?;

        if let Some(data) = frame.data_ref() {
            current_size += data.len();

            if current_size > MAX_BUFFER_BYTES {
                buffered_bytes.extend_from_slice(data);
                exceeded = true;
                break;
            }

            buffered_bytes.extend_from_slice(data);
        }
    }

    let buffered_bytes = Bytes::from(buffered_bytes);

    if exceeded {
        Ok(RequestBody::OneShot(
            PrefixBody {
                prefix: Some(buffered_bytes),
                rest: body,
            }
            .boxed(),
        ))
    } else {
        Ok(RequestBody::Retriable {
            bytes: buffered_bytes,
        })
    }
}

enum ConnectionError {
    NoHealthyBackend,
    CouldNotConnect,
}

async fn get_connection<'a>(
    backends: &'a Vec<Backend>,
    connection_pools: &ConnectionPools,
) -> Result<(SendRequest<BoxBody<Bytes, hyper::Error>>, &'a Backend), ConnectionError> {
    let mut connection_result = None;
    for _ in 0..MAX_RETRY_ATTEMPTS_FOR_GET_CONNECTION {
        let healthy_backends = backends
            .iter()
            .filter(|b| b.healthy.load(Ordering::Relaxed))
            .collect::<Vec<_>>();

        if healthy_backends.is_empty() {
            eprintln!("no healthy backend available");
            return Err(ConnectionError::NoHealthyBackend);
        }

        let backend =
            healthy_backends[INDEX.fetch_add(1, Ordering::Relaxed) % healthy_backends.len()];
        println!("routing to: {}", backend.addr);

        match connection_pools.get_connection(&backend.addr).await {
            Ok(s) => {
                connection_result = Some((s, backend));
                break;
            }
            Err(e) => {
                eprintln!("failed to get connection to {}: {:?}", backend.addr, e);

                if backend.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1
                    >= MAX_CONSECUTIVE_FAILURES_FOR_A_BACKEND
                {
                    backend.healthy.store(false, Ordering::Relaxed);
                }
            }
        };
    }

    match connection_result {
        Some((s, backend)) => return Ok((s, backend)),
        None => return Err(ConnectionError::CouldNotConnect),
    };
}

async fn handle_connection(
    req: Request<hyper::body::Incoming>,
    backends: Arc<Vec<Backend>>,
    connection_pools: Arc<ConnectionPools>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>> {
    //note: using BoxBody so I can return Incoming (streaming) to the client, while also being able to return Full responses for stuff like no healthy backend and stuff, which can't be done with Incoming only
    println!("{:?}", req);

    let (parts, body) = req.into_parts();
    let retriable = RETRIABLE_METHODS.contains(&parts.method);

    let mut body = if retriable {
        match handle_body(body).await {
            Ok(rb) => rb,
            Err(e) => {
                eprintln!("stream error: {:?}", e);
                return Ok(error_response(400, "recieve request failed"));
            }
        }
    } else {
        RequestBody::OneShot(body.boxed())
    };

    enum RequestError {
        Failed,
        TimedOut,
    }

    let mut req;
    let (mut res, mut sender, mut backend, mut last_error) = (None, None, None, None);
    let attempts_allowed = body.max_attempts();
    for _ in 0..attempts_allowed {
        (sender, backend) = match get_connection(&backends, &connection_pools).await {
            Ok((sender, backend)) => (Some(sender), Some(backend)),
            Err(ConnectionError::NoHealthyBackend) => {
                return Ok(error_response(503, "no healthy backend available"));
            }
            Err(ConnectionError::CouldNotConnect) => {
                return Ok(error_response(502, "could not connect to backend"));
            }
        };

        let attempt_body = match &mut body {
            RequestBody::OneShot(b) => {
                std::mem::replace(b, Empty::new().map_err(|e| match e {}).boxed())
            }
            RequestBody::Retriable { bytes } => {
                Full::new(bytes.clone()).map_err(|e| match e {}).boxed()
            }
        };

        req = Request::from_parts(parts.clone(), attempt_body);
        let backend_addr = &backend.as_ref().unwrap().addr;
        let backend_consecutive_failures = &backend.as_ref().unwrap().consecutive_failures;
        let backend_healthy = &backend.as_ref().unwrap().healthy;

        match tokio::time::timeout(
            Duration::from_secs(5),
            sender.as_mut().unwrap().send_request(req),
        )
        .await
        {
            Ok(Ok(r)) => {
                res = Some(r);
                backend_consecutive_failures.store(0, Ordering::Relaxed);
                break;
            }
            Ok(Err(e)) => {
                eprintln!("sending request to {} failed: {:?}", backend_addr, e);

                if backend_consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1
                    >= MAX_CONSECUTIVE_FAILURES_FOR_A_BACKEND
                {
                    backend_healthy.store(false, Ordering::Relaxed);
                }
                last_error = Some(RequestError::Failed);
            }
            Err(_) => {
                eprintln!("request to {} timed out", backend_addr);

                if backend_consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1
                    >= MAX_CONSECUTIVE_FAILURES_FOR_A_BACKEND
                {
                    backend_healthy.store(false, Ordering::Relaxed);
                }
                last_error = Some(RequestError::TimedOut);
            }
        };
    }

    let (res, sender, backend_addr) = match res {
        Some(r) => (r, sender.unwrap(), &backend.as_ref().unwrap().addr),
        None => match last_error.unwrap() {
            RequestError::Failed => {
                return Ok(error_response(
                    502,
                    "backend closed connection or failed mid-request",
                ));
            }
            RequestError::TimedOut => {
                return Ok(error_response(504, "request to backend timed out"));
            }
        },
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
