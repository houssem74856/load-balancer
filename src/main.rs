mod backend;
mod body;
mod health;
mod pool;

use crate::backend::{Backend, MAX_CONSECUTIVE_FAILURES_FOR_A_BACKEND};
use crate::body::{RequestBody, handle_body};
use crate::health::start_health_checker;
use crate::pool::ConnectionPools;

use std::net::SocketAddr;

use http_body_util::Empty;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::body::Bytes;
use hyper::client::conn::http1::SendRequest;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

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
static INDEX: AtomicUsize = AtomicUsize::new(0);
const MAX_RETRY_ATTEMPTS_FOR_GET_CONNECTION: u8 = 3;

pub struct RetryBudget {
    tokens: AtomicU8,
    max_tokens: u8,
    retry_cost: u8,
}

impl RetryBudget {
    pub fn new(max_tokens: u8, retry_cost: u8) -> Self {
        RetryBudget {
            tokens: AtomicU8::new(max_tokens),
            max_tokens,
            retry_cost,
        }
    }

    pub fn deposit(&self) {
        let _ = self
            .tokens
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |t| {
                Some(t.saturating_add(1).min(self.max_tokens))
            });
    }

    pub fn try_withdraw(&self) -> bool {
        self.tokens
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |t| {
                if t >= self.retry_cost {
                    Some(t - self.retry_cost)
                } else {
                    None
                }
            })
            .is_ok()
    }
}

enum ConnectionError {
    NoHealthyBackend,
    CouldNotConnect,
}

async fn get_connection<'a>(
    backends: &'a Vec<Backend>,
    connection_pools: &ConnectionPools,
    tried_backends: &mut Vec<&'a String>,
    retry_budget: &RetryBudget,
) -> Result<(SendRequest<BoxBody<Bytes, hyper::Error>>, &'a Backend), ConnectionError> {
    let mut connection_result = None;
    let mut attempt_idx = 0;
    while (tried_backends.len() as u8) < MAX_RETRY_ATTEMPTS_FOR_GET_CONNECTION {
        if attempt_idx > 0 && !retry_budget.try_withdraw() {
            break;
        }
        attempt_idx += 1;
        let healthy_backends = backends
            .iter()
            .filter(|b| !tried_backends.contains(&&b.addr) && b.healthy.load(Ordering::Relaxed))
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

                tried_backends.push(&backend.addr);

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
    retry_budget: Arc<RetryBudget>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>> {
    //note: using BoxBody so I can return Incoming (streaming) to the client, while also being able to return Full responses for stuff like no healthy backend and stuff, which can't be done with Incoming only
    println!("{:?}", req);

    if !backends.iter().any(|b| b.healthy.load(Ordering::Relaxed)) {
        eprintln!("no healthy backend available");
        return Ok(error_response(503, "no healthy backend available"));
    }

    retry_budget.deposit();

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
    let mut tried_backends = Vec::new();
    for attempt_idx in 0..attempts_allowed {
        if attempt_idx > 0 && !retry_budget.try_withdraw() {
            break;
        }

        (sender, backend) = match get_connection(
            &backends,
            &connection_pools,
            &mut tried_backends,
            &retry_budget,
        )
        .await
        {
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

                tried_backends.push(backend_addr);

                if backend_consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1
                    >= MAX_CONSECUTIVE_FAILURES_FOR_A_BACKEND
                {
                    backend_healthy.store(false, Ordering::Relaxed);
                }
                last_error = Some(RequestError::Failed);
            }
            Err(_) => {
                eprintln!("request to {} timed out", backend_addr);

                tried_backends.push(backend_addr);

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let retry_budget = Arc::new(RetryBudget::new(100, 10));
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
        let retry_budget = Arc::clone(&retry_budget);

        tokio::task::spawn(async move {
            let service = service_fn(move |req| {
                handle_connection(
                    req,
                    Arc::clone(&backends),
                    Arc::clone(&connection_pools),
                    Arc::clone(&retry_budget),
                )
            });

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
