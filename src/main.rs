use clap::Parser;
use anyhow::Result;
use tokio::signal;
use std::sync::Arc;

mod config;
mod persistence;
mod server;
mod engine;
mod types;
mod expire;
mod txn;
mod monitor;

use config::load;
use persistence::Persistence;
use sled::Db;
use std::path::PathBuf;
use monitor::Monitor;

/// crab-cage 启动参数
#[derive(Parser, Debug)]
#[command(author, version, about="Rudis server with AOF+RDB", long_about = None)]
struct Args {
    /// 监听地址 (host:port)
    #[arg(short, long, default_value = "127.0.0.1:6380")]
    listen: String,

    /// JSON 配置文件路径
    #[arg(short, long, default_value = "config.json")]
    config: PathBuf,

    /// sled 数据库目录
    #[arg(short = 'd', long, default_value = "kv.db")]
    db_path: PathBuf,

    /// AOF 日志文件路径
    #[arg(long, default_value = "appendonly.aof")]
    aof_path: PathBuf,

    /// RDB 快照文件路径
    #[arg(long, default_value = "dump.rdb")]
    rdb_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 解析命令行参数
    let args = Args::parse();
    println!("Starting Crab-Cage with args: {:?}", args);

    // 2. 读取 JSON 配置
    let cfg = load(&args.config)?;
    println!("Loaded config: {:?}", cfg);

    // 3. 打开 sled
    let sled_db: Db = sled::open(&args.db_path)?;

    // 4. 创建监视管理器
    let watch_manager = Arc::new(engine::watch::WatchManager::new());
    
    // 5. 创建数据库实例
    let db = engine::kv::DbInstance{
        db: sled_db.clone(),
        watch_manager: watch_manager.clone(),
    };

    // 6. 构造持久化器 (支持自定义路径)
    let pers = Persistence::new_with_paths(
        cfg.clone(),
        sled_db.clone(),
        args.aof_path.clone(),
        args.rdb_path.clone(),
    )?;

    // 7. 创建监控系统
    let monitor = Arc::new(Monitor::new());

    // 8. 启动前重放 AOF
    pers.load_aof()?;

    // 9. 启动网络服务
    let serve_handle = {
        let db = db.clone();
        let pers = pers.clone();
        let addr = args.listen.clone();
        let monitor = monitor.clone();
        tokio::spawn(async move {
            server::start_with_addr_db_and_pers(&addr, db, pers, monitor)
                .await
                .unwrap();
        })
    };

    // 10. 启动HTTP指标服务
    if cfg.metrics_enabled {
        let metrics_port = cfg.metrics_port;
        let metrics = monitor.metrics.clone();
        tokio::spawn(async move {
            start_metrics_server(metrics, metrics_port).await;
        });
    }

    // 11. 等 CTRL-C 优雅退出
    signal::ctrl_c().await?;
    println!("Shutting down…");
    serve_handle.abort();
    pers.fsync_and_close();
    Ok(())
}

async fn start_metrics_server(metrics: Arc<monitor::Metrics>, port: u16) {
    use warp::Filter;

    let route = warp::path("metrics")
        .map(move || warp::reply::html(metrics.to_prometheus()));

    println!("Metrics server listening on 0.0.0.0:{}", port);
    warp::serve(route).run(([0, 0, 0, 0], port)).await;
}