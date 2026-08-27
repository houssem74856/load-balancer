use http_body_util::combinators::BoxBody;
use hyper::body::Bytes;
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

struct PooledConnection {
    sender: SendRequest<BoxBody<Bytes, hyper::Error>>,
    last_used: Instant,
}

pub struct ConnectionPools {
    pools: HashMap<String, Arc<Mutex<Vec<PooledConnection>>>>,
    max_idle_per_host: usize,
    idle_timeout: Duration,
}

impl ConnectionPools {
    pub fn new(
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

    pub async fn get_connection(
        &self,
        backend_addr: &str,
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
            match pooled.sender.ready().await {
                Ok(_) => {
                    return Ok(pooled.sender);
                }
                Err(e) => {
                    eprintln!("sender not ready: {}", e);
                }
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

    pub async fn return_connection(
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

    pub async fn empty_backend_connections(&self, backend_addr: &String) {
        if let Some(connections_mutex) = self.pools.get(backend_addr) {
            let mut connections = connections_mutex.lock().unwrap();
            connections.clear();
        }
    }
}
