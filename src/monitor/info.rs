use super::*;
use crate::monitor::metrics::Metrics;
use crate::persistence::Persistence;
use crate::engine::KvEngine;

pub fn build_info_response(
    section: Option<&str>,
    db: &impl KvEngine,
    pers: &Persistence,
    metrics: &Metrics,
) -> String {
    let sections = section.map(|s| vec![s]).unwrap_or_else(|| {
        vec![
            "server",
            "clients",
            "memory",
            "persistence",
            "stats",
            "commandstats",
        ]
    });

    let mut response = String::new();

    for sec in sections {
        match sec.to_lowercase().as_str() {
            "server" => {
                response.push_str("#Server\n");
                response.push_str(&format!("Crab-Cage_version:{}\n", env!("CARGO_PKG_VERSION")));
                response.push_str(&format!("OS:{}\n", std::env::consts::OS));
                // 此处为硬编码端口，后续需要修改
                // response.push_str(&format!("tcp_port:{}\n", 6380));
            }
            "clients" => {
                response.push_str("# Clients\n");
                response.push_str(&format!(
                    "connected_clients:{}\n",
                    metrics.connected_clients.load(Ordering::Relaxed)
                ));
                response.push_str(&format!(
                    "total_connections:{}\n",
                    metrics.total_connections.load(Ordering::Relaxed)
                ));
            }
            "memory" => {
                response.push_str(&format!("# Memory\n"));
                response.push_str(&format!(
                    "used_memory:{} bytes\n",
                    metrics.memory_usage()
                ));
            }
            "persistence" => {
                response.push_str("# Persistence\n");
                response.push_str(&format!(
                    "aof_enabled:{}\n",
                    pers.cfg.aof as u8
                ));
                response.push_str(&format!(
                    "aof_size:{} bytes\n",
                    pers.aof_size()
                ));
                response.push_str(&format!(
                    "rdb_last_save:{}\n",
                    pers.last_save_time()
                ));               
            }
            "stats" => {
                response.push_str("# Stats\n");
                response.push_str(&format!(
                    "total_commands_processed:{}\n",
                    metrics.command_count.load(Ordering::Relaxed)
                ));
                response.push_str(&format!(
                    "total_keys:{}\n",
                    metrics.key_count(db)
                ));
            }
            "commandstats" => {
                response.push_str("# Command Stats\n");
                for entry in metrics.command_stats.iter() {
                    response.push_str(&format!(
                        "cmd_{}:{}\n",
                        entry.key(),
                        entry.value()
                    ));
                }
            }
            _ => {}
        }
    }

    response
}