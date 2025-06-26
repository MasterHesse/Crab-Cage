use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::Path
};
use anyhow::{Context, Result};
use serde_json;


/// 进程启动后，从 config.rs 中读到的全局配置
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    /// 是否打开 AOF 日志
    pub aof: bool,
    /// 是否开启 RDB 快照
    pub rdb: bool,
    /// RDB快照周期 （秒）
    pub snapshot_interval_secs: u64,
    /// 每固定 N 次写操作强制快照
    pub snapshot_threshold: u64,
    // 监控配置
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    pub slowlog_threshold_ms: u64,
}

/// 从指定路径读取并反序列化 JSON 配置
pub fn load<P: AsRef<Path>>(path: P) -> Result<Config> {
    let path_ref = path.as_ref();
    
    // 如果配置文件不存在，创建默认配置
    if !path_ref.exists() {
        println!("Config file not found, creating default configuration...");
        
        let default_cfg = Config {
            aof: true,
            rdb: true,
            snapshot_interval_secs: 60,
            snapshot_threshold: 20,
            metrics_enabled: true,
            metrics_port: 9090,
            slowlog_threshold_ms: 10,
        };
        
        let default_json = serde_json::to_string_pretty(&default_cfg)?;
        fs::write(path_ref, default_json)?;
        println!("Default config created at {:?}", path_ref);
        
        return Ok(default_cfg);
    }

    let data = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file {:?}", path.as_ref()))?;
    let cfg: Config = serde_json::from_str(&data)
        .context("Failed to parse config.json")?;
    Ok(cfg)
}