// src/monitor/mod.rs
//! 监控与诊断模块
mod client;
pub mod info;
mod slowlog;
mod metrics;

use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::{Instant, Duration};
use std::collections::VecDeque;
use std::net::SocketAddr;
use dashmap::DashMap;

pub use client::ClientTracker;
pub use slowlog::SlowLog;
pub use metrics::Metrics;

/// 监控系统主结构
#[derive(Clone)]
pub struct Monitor {
    pub client_tracker: Arc<ClientTracker>,
    pub slow_log: Arc<SlowLog>,
    pub metrics: Arc<Metrics>,
}

impl Monitor {
    pub fn new() -> Self {
        Monitor {
            client_tracker: Arc::new(ClientTracker::new()),
            slow_log: Arc::new(SlowLog::new(128)),
            metrics: Arc::new(Metrics::new()),
        }
    }
}

/// 客户端信息
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub addr: SocketAddr,
    pub connect_time: Instant,
    pub last_command: String,
    pub last_command_time: Instant,
}

/// 慢日志条目
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    pub timestamp: Instant,
    pub duration: Duration,
    pub command: String,
    pub client_addr: String,
}