// src/main.rs
use kvdb::server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 启动 TCP 服务，默认 0.0.0.0:6379
    server::start().await?;
    Ok(())
}