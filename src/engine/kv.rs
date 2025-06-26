// src/engine/kv.rs

use std::sync::Arc;

use anyhow::Error;
use sled::{Db, IVec};
use sled::transaction::TransactionalTree;

use crate::engine::watch::WatchManager;

/// 统一普通 Db 与事务上下文的最小 KV 抽象
pub trait KvEngine {
    /// GET key
    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Error>;
    /// SET key -> value
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<IVec>, Error>;
    /// DEL key
    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Error>;

    fn scan_prefix(&self, prefix: &[u8]) -> Box<dyn Iterator<Item = Result<(IVec, IVec), Error>>>;

    /// 如果底层是一个 sled::Db，就返回 Some(&Db)；否则（事务上下文）返回 None
    fn as_db(&self) -> Option<&Db> {
        None
    }

    // 获取底层数据库引用 （用于 WATCH/UNWATCH 机制）
    fn watch_manager(&self) -> Option<Arc<WatchManager>> {
        None
    }

}

impl KvEngine for Db {
    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Error> {
        self.open_tree("")?.get(key).map_err(Into::into)
    }
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<IVec>, Error> {
        self.open_tree("")?.insert(key, value).map_err(Into::into)
    }
    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Error> {
        self.open_tree("")?.remove(key).map_err(Into::into)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Box<dyn Iterator<Item = Result<(IVec, IVec), Error>>> {
        Box::new(self.open_tree("").unwrap().scan_prefix(prefix).map(|res| res.map_err(Into::into)))
    }

    fn as_db(&self) -> Option<&Db> {
        Some(self)
    }

    fn watch_manager(&self) -> Option<Arc<WatchManager>> {
        None
    }
}

impl KvEngine for TransactionalTree {
    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Error> {
        TransactionalTree::get(self, key).map_err(Error::from)
    }
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<IVec>, Error> {
        TransactionalTree::insert(self, key, value).map_err(Error::from)
    }
    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Error> {
        TransactionalTree::remove(self, key).map_err(Error::from)
    }

    // 事务操作暂不支持扫描，返回空迭代器
    fn scan_prefix(&self, _prefix: &[u8]) -> Box<dyn Iterator<Item = Result<(IVec, IVec), Error>>> {
        Box::new(std::iter::empty()) // 或返回错误
    }

    fn as_db(&self) -> Option<&Db> {
        None
    }

    fn watch_manager(&self) -> Option<Arc<WatchManager>> {
        None
    }
}

/// 数据库实例，包含 sled 数据库和监视管理器
#[derive(Clone)]
pub struct DbInstance {
    pub db: sled::Db,
    pub watch_manager: Arc<WatchManager>,
}

impl KvEngine for DbInstance {
    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Error> {
        self.db.get(key).map_err(Into::into)
    }
    
    fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<IVec>, Error> {
        let res = self.db.insert(key, value)?;
        let key_str = String::from_utf8_lossy(key);
        self.watch_manager.notify_key_change(&key_str);
        Ok(res)
    }
    
    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Error> {
        let res = self.db.remove(key)?;
        let key_str = String::from_utf8_lossy(key);
        self.watch_manager.notify_key_change(&key_str);
        Ok(res)
    }
    
    fn scan_prefix(&self, prefix: &[u8]) -> Box<dyn Iterator<Item = Result<(IVec, IVec), Error>>> {
        Box::new(self.db.scan_prefix(prefix).map(|res| res.map_err(Into::into)))
    }
    
    fn as_db(&self) -> Option<&Db> {
        Some(&self.db)
    }
    
    fn watch_manager(&self) -> Option<Arc<WatchManager>> {
        Some(self.watch_manager.clone())
    }
}