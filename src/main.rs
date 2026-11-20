use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;

    let admin_addr = SocketAddr::from(([127, 0, 0, 1], 3001)); //note: must be localhost, so it can only be reachable from within
    let admin_listener = TcpListener::bind(admin_addr).await?;

    load_balancer::run(listener, admin_listener).await
}
