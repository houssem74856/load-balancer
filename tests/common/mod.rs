#![allow(dead_code)]

use load_balancer::Config;

use http_body_util::{BodyExt, Full};
use hyper::{
    Request, Response, StatusCode,
    body::Bytes,
    header::{CONNECTION, HeaderValue},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::net::TcpListener;

//returns (lb_addr, admin_addr), port 0 so every test gets its own free ports
pub async fn start_lb_with_config(
    backends_addresses: Vec<String>,
    config: Config,
) -> (SocketAddr, SocketAddr) {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let lb_listener = TcpListener::bind(addr).await.unwrap();
    let admin_listener = TcpListener::bind(addr).await.unwrap();

    let lb_addr = lb_listener.local_addr().unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();

    tokio::spawn(load_balancer::run(
        config,
        lb_listener,
        admin_listener,
        backends_addresses,
    ));

    (lb_addr, admin_addr)
}

pub async fn start_lb(backends_addresses: Vec<String>) -> SocketAddr {
    let (lb_addr, _) = start_lb_with_config(backends_addresses, Config::default()).await;
    lb_addr
}

//this fn is used by tests to poll a condition until true instead of sleeping a fixed time(fancy I would say)
//it panics if the condition still isn't true after 5 seconds
//I recommend seeing it in action to understand it quickly
pub async fn wait_until(mut condition: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);

    while !condition().await {
        assert!(
            Instant::now() < deadline,
            "condition not met within 5 seconds"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Clone, Copy)]
enum Behavior {
    Normal,
    Close,
    Hang,
    Echo,
    Status(u16),
    CloseAfterResponse,
}

async fn handle_connection(
    req: Request<hyper::body::Incoming>,
    state: Arc<MockState>,
    connection_counted: Arc<AtomicBool>,
    name: &'static str,
    behavior: Behavior,
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    // health checker hits "/", we shouldn't count them, and the test can make them fail if it wants
    if req.uri().path() == "/" {
        let mut res = Response::new(Full::new(Bytes::from(name)));
        if state.fail_health_checks.load(Ordering::Relaxed) {
            *res.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        }

        return Ok(res);
    }

    state.request_count.fetch_add(1, Ordering::Relaxed);

    //a connection is counted once, on its first real request, so the health checker's connections don't count
    if !connection_counted.swap(true, Ordering::Relaxed) {
        state.connection_count.fetch_add(1, Ordering::Relaxed);
    }

    match behavior {
        Behavior::Normal => Ok(Response::new(Full::new(Bytes::from(name)))),
        Behavior::Close => Err("closing connection on purpose".into()),
        Behavior::Hang => std::future::pending().await,
        Behavior::Echo => {
            //sends back the body, and the x-echo header if there is one
            let echo_header = req.headers().get("x-echo").cloned();
            let body = req.into_body().collect().await?.to_bytes();

            let mut res = Response::new(Full::new(body));
            if let Some(value) = echo_header {
                res.headers_mut().insert("x-echo", value);
            }

            Ok(res)
        }
        Behavior::Status(code) => {
            let mut res = Response::new(Full::new(Bytes::from(name)));
            *res.status_mut() = StatusCode::from_u16(code).unwrap();

            Ok(res)
        }
        Behavior::CloseAfterResponse => {
            //hyper closes the connection after sending a response that has this header
            let mut res = Response::new(Full::new(Bytes::from(name)));
            res.headers_mut()
                .insert(CONNECTION, HeaderValue::from_static("close"));

            Ok(res)
        }
    }
}

#[derive(Default)]
struct MockState {
    request_count: AtomicUsize,
    connection_count: AtomicUsize,
    fail_health_checks: AtomicBool,
}

pub struct MockBackend {
    pub address: String,
    state: Arc<MockState>,
}

impl MockBackend {
    pub fn request_count(&self) -> usize {
        self.state.request_count.load(Ordering::Relaxed)
    }

    pub fn connection_count(&self) -> usize {
        self.state.connection_count.load(Ordering::Relaxed)
    }

    pub fn fail_health_checks(&self) {
        self.state.fail_health_checks.store(true, Ordering::Relaxed);
    }

    pub fn pass_health_checks(&self) {
        self.state
            .fail_health_checks
            .store(false, Ordering::Relaxed);
    }

    pub async fn start_a_normal_backend(name: &'static str) -> Self {
        Self::start(name, Behavior::Normal).await
    }

    pub async fn start_a_closing_backend(name: &'static str) -> Self {
        Self::start(name, Behavior::Close).await
    }

    pub async fn start_a_hanging_backend(name: &'static str) -> Self {
        Self::start(name, Behavior::Hang).await
    }

    pub async fn start_an_echo_backend(name: &'static str) -> Self {
        Self::start(name, Behavior::Echo).await
    }

    pub async fn start_a_backend_with_status(name: &'static str, status: u16) -> Self {
        Self::start(name, Behavior::Status(status)).await
    }

    pub async fn start_a_close_after_response_backend(name: &'static str) -> Self {
        Self::start(name, Behavior::CloseAfterResponse).await
    }

    async fn start(name: &'static str, behavior: Behavior) -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(addr).await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let state = Arc::new(MockState::default());
        let state_clone = Arc::clone(&state);

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
                let state = Arc::clone(&state_clone);
                //every accepted connection gets its own flag
                let connection_counted = Arc::new(AtomicBool::new(false));

                tokio::task::spawn(async move {
                    let service = service_fn(|req| {
                        handle_connection(
                            req,
                            Arc::clone(&state),
                            Arc::clone(&connection_counted),
                            name,
                            behavior,
                        )
                    });

                    if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                        eprintln!("Error serving connection: {:?}", err);
                    }
                });
            }
        });

        Self { address, state }
    }
}
