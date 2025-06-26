// src/engine/watch.rs
use dashmap::{DashMap, DashSet};
use std::{sync::Arc, vec};

#[derive(Debug, Clone)]
pub struct WatchManager {
    // key -> 监视该 key 的会话 ID 集合
    watched_keys: Arc<DashMap<String, DashSet<u64>>>,
    // Session ID -> 该会话监视的 key 集合
    session_watches: Arc<DashMap<u64, DashSet<String>>>,
}

impl WatchManager {
    pub fn new() -> Self {
        Self { 
            watched_keys: Arc::new(DashMap::new()),
            session_watches: Arc::new(DashMap::new()), 
        }
    }

    // 添加监视
    pub fn watch(&self, session_id: u64, keys:&[String]) {
        for key in keys {
            // 添加 key 到 session 的映射
            self.watched_keys
                .entry(key.clone())
                .or_insert_with(|| DashSet::new())
                .insert(session_id);
                
            // 添加 session 到 key 的映射
            self.session_watches
                .entry(session_id)
                .or_insert_with(|| DashSet::new())
                .insert(key.clone());
        }
    }

    // 移除 session 的所有监视
    pub fn unwatch(&self, session_id: u64) {
        if let Some(keys) = self.session_watches.remove(&session_id) {
            for key in keys.1.iter() {
                let key_str = key.as_str();
                if let Some(entry) = self.watched_keys.get_mut(key_str) {
                    entry.remove(&session_id);
                }
            }
        }
    }

    // 通知 key 被修改
    pub fn notify_key_change(&self, key: &str) -> Vec<u64> {
        let mut affected_sessions = vec![];

        let normalized_key = key.to_lowercase();

        if let Some(sessions) = self.watched_keys.get(&normalized_key) {
            affected_sessions = sessions.iter().map(|id| *id).collect();

            // 移除该 key 的所有监视
            if let Some(entry) = self.watched_keys.get_mut(&normalized_key) {
                entry.clear();
            }
        }

        affected_sessions
    }

    // 检查对话是否标记为脏
    pub fn is_dirty(&self, session_id: u64) -> bool {
        if let Some(keys) = self.session_watches.get(&session_id) {
            for key in keys.iter() {
                let normalized_key = key.to_lowercase();
                let key_str = normalized_key.as_str();
                if !self.watched_keys.contains_key(key_str) {
                    return true;
                }
            }
        }
        false
    }

    // 清除会话的所有监视
    pub fn clear_session(&self, session_id: u64) {
        self.unwatch(session_id);
    }
}

// src/engine/watch.rs 底部添加
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watch_and_notify() {
        let manager = WatchManager::new();
        let session_id = 16;
        let keys = vec!["key1".to_string(), "key2".to_string()];
        
        // 添加监视
        manager.watch(session_id, &keys);
        
        // 验证键被监视
        assert!(manager.watched_keys.contains_key("key1"));
        assert!(manager.watched_keys.contains_key("key2"));
        assert_eq!(manager.watched_keys.get("key1").unwrap().len(), 1);
        
        // 通知键被修改
        let affected = manager.notify_key_change("key1");
        assert_eq!(affected, vec![session_id]);
        
        // 验证会话被标记为脏
        assert!(manager.is_dirty(session_id));
        
        // 清除监视
        manager.clear_session(session_id);
        assert!(!manager.session_watches.contains_key(&session_id));
    }
}