use serde::Deserialize;
use std::{
    fs,
    path::Path
};
use anyhow::{Context, Result};

/// 进程启动后，从 config.rs 中读到的全局配置
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// 是否打开 AOF 日志
    pub aof: bool,
    /// 是否开启 RDB 快照
    pub rdb: bool,
    /// RDB快照周期 （秒）
    pub snapshot_interval_secs: u64,
    /// 每固定 N 次写操作强制快照
    pub snapshot_threshold: u64,
}

/// 从指定路径读取并反序列化 JSON 配置
pub fn load<P: AsRef<Path>>(path: P) -> Result<Config> {
    let data = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read config file {:?}", path.as_ref()))?;
    let cfg: Config = serde_json::from_str(&data)
        .context("Failed to parse config.json")?;
    Ok(cfg)
}