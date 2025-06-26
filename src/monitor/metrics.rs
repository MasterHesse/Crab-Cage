// src/monitor/metrics.rs

use super::*;
use crate::engine::KvEngine;

#[derive(Default)]
pub struct Metrics {
    pub connected_clients: Arc<AtomicU64>,
    pub total_connections: Arc<AtomicU64>,
    pub command_count: Arc<AtomicU64>,
    pub command_stats: Arc<DashMap<String, u64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics::default()
    }

    pub fn record_command(&self, command: &str) {
        self.command_count.fetch_add(1, Ordering::Relaxed);
        self.command_stats.entry(command.to_string()).and_modify(|c| *c += 1).or_insert(1);
    }

    pub fn memory_usage(&self) -> u64 {
        // 简化实现 - 实际中应计算实际内存使用量
        1024 * 1024 // 1MB
    }

    pub fn key_count(&self, db: &impl KvEngine) -> u64 {
        // 统计键数量
        if let Some(sled_db) = db.as_db() {
            sled_db.open_tree("").unwrap().len() as u64
        } else {
            0
        }
    }

    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        
        output.push_str("# HELP Crab-Cage_connected_clients Current number of client connections\n");
        output.push_str("# TYPE Crab-Cage_connected_clients gauge\n");
        output.push_str(&format!(
            "Crab-Cage_connected_clients {}\n",
            self.connected_clients.load(Ordering::Relaxed)
        ));
        
        output.push_str("# HELP Crab-Cage_total_connections Total connections since startup\n");
        output.push_str("# TYPE Crab-Cage_total_connections counter\n");
        output.push_str(&format!(
            "Crab-Cage_total_connections {}\n",
            self.total_connections.load(Ordering::Relaxed)
        ));
        
        output.push_str("# HELP Crab-Cage_command_count Total commands processed\n");
        output.push_str("# TYPE Crab-Cage_command_count counter\n");
        output.push_str(&format!(
            "Crab-Cage_command_count {}\n",
            self.command_count.load(Ordering::Relaxed)
        ));
        
        output.push_str("# HELP Crab-Cage_command_stats Command statistics\n");
        output.push_str("# TYPE Crab-Cage_command_stats counter\n");
        for entry in self.command_stats.iter() {
            output.push_str(&format!(
                "Crab-Cage_command_stats{{command=\"{}\"}} {}\n",
                entry.key(),
                entry.value()
            ));
        }
        
        output     
    }
}