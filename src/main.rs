use load_balancer::Config;

use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;

    let admin_addr = SocketAddr::from(([127, 0, 0, 1], 3001)); //note: must be localhost, so it can only be reachable from within
    let admin_listener = TcpListener::bind(admin_addr).await?;

    let backends_addresses = vec![
        "localhost:8000".to_string(),
        "localhost:8001".to_string(),
        "localhost:8002".to_string(),
    ];

    load_balancer::run(
        Config::default(),
        listener,
        admin_listener,
        backends_addresses,
    )
    .await
}
