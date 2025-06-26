// src/monitor/slowlog.rs

use super::*;
use std::sync::Mutex;

pub struct SlowLog {
    logs: Arc<Mutex<VecDeque<SlowLogEntry>>>,
    max_entries: usize,
    slow_threshold: Duration,
}

impl SlowLog {
    pub fn new(max_entries: usize) -> Self {
        SlowLog {
            logs: Arc::new(Mutex::new(VecDeque::with_capacity(max_entries))),
            max_entries,
            slow_threshold: Duration::from_millis(10),
        }
    }

    pub fn set_threshold(&mut self, threshold_ms: u64) {
        self.slow_threshold = Duration::from_millis(threshold_ms);
    }

    pub fn add_entry(&self, command: &str, duration: Duration, client_addr: &str) {
        if duration >= self.slow_threshold {
            let mut logs = self.logs.lock().unwrap();
            if logs.len() >= self.max_entries {
                logs.pop_back();
            }
            logs.push_front(SlowLogEntry {
                timestamp: Instant::now(),
                duration,
                command: command.to_string(),
                client_addr: client_addr.to_string(),
            });
        }
    }

    pub fn get_logs(&self) -> String {
        let logs = self.logs.lock().unwrap();
        let mut response = String::new();
        
        for (i, entry) in logs.iter().enumerate() {
            response.push_str(&format!(
                "{}. timestamp: {:?}, duration: {:?}ms, command: {}, client: {}\n",
                i + 1,
                entry.timestamp,
                entry.duration.as_millis(),
                entry.command,
                entry.client_addr
            ));
        }
        
        response
    }
}