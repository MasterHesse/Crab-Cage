// src/main.rs
mod config;
mod persistence;
mod server;
mod engine;

use anyhow::Result;
use config::load;
use persistence::Persistence;
use sled::Db;
use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 读配置
    let cfg = load("config.json")?;
    println!("Loaded config: {:?}", cfg);

    // 2. 打开 sled 数据库
    let db: Db = sled::open("kv.db")?;

    // 3. 构造持久化器
    let pers = Persistence::new(cfg.clone(), db.clone())?;

    // 4. 启动前先重放 AOF
    pers.load_aof(&db)?;

    // 5. 启动网络服务
    let serve_handle = tokio::spawn(async move {
        server::start_with_db_and_pers(db, pers).await.unwrap();
    });

    // 6. 等待 Ctrl-C ，然后退出
    signal::ctrl_c().await?;
    println!("Shutting down...");

    // AOF 强制 fsync
    serve_handle.abort();
    Ok(())
}