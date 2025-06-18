// src/engine/kv.rs

use sled::{Db, IVec};
use sled::transaction::TransactionalTree;
use sled::CompareAndSwapError;

/// 把 sled::Db 和 sled::TransactionalTree 的基本 KV 操作统一化
pub trait KVEngine {
    type Err: std::fmt::Debug + std::fmt::Display;
    fn get(&self, key: &[u8]) -> Result<Option<IVec>, Self::Err>;
    fn insert(&self, key: &[u8], val: &[u8]) -> Result<Option<IVec>, Self::Err>;
    fn remove(&self, key: &[u8]) -> Result<Option<IVec>, Self::Err>;

    /// 原子 CAS：比对 old, 若相符则写入 new
    fn cas(
        &self,
        key: &[u8],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<Option<IVec>, Self::Err>;
}

impl KVEngine for Db {
    type Err = String;

    fn get(&self, k: &[u8]) -> Result<Option<IVec>, Self::Err> {
        self.get(k).map_err(|e| e.to_string())
    }

    fn insert(&self, k: &[u8], v: &[u8]) -> Result<Option<IVec>, Self::Err> {
        self.insert(k, v).map_err(|e| e.to_string())
    }

    fn remove(&self, k: &[u8]) -> Result<Option<IVec>, Self::Err> {
        self.remove(k).map_err(|e| e.to_string())
    }

    fn cas(
        &self,
        k: &[u8],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<Option<IVec>, Self::Err> {
        // 先拿到当前值，便于成功时返回
        let prev = self.get(k).map_err(|e| e.to_string())?;

        // 做 CAS
        let inner = self
            .compare_and_swap(k, old, new)
            .map_err(|e| e.to_string())?;           // 把 sled::Error -> String

        match inner {
            Ok(()) => Ok(prev),                     // 成功：返回旧值
            Err(_cas_err) => Err("cas mismatch".into()), // 期望值不匹配
        }
    }
}

impl KVEngine for TransactionalTree {
    type Err = String;

    fn get(&self, k: &[u8]) -> Result<Option<IVec>, Self::Err> {
        self.get(k).map_err(|e| e.to_string())
    }
    fn insert(&self, k: &[u8], v: &[u8]) -> Result<Option<IVec>, Self::Err> {
        self.insert(k, v).map_err(|e| e.to_string())
    }
    fn remove(&self, k: &[u8]) -> Result<Option<IVec>, Self::Err> {
        self.remove(k).map_err(|e| e.to_string())
    }
    fn cas(
        &self,
        k: &[u8],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<Option<IVec>, Self::Err> {
        // 事务内没有内置 CAS，自己实现：
        // 1. 读取当前值
        let prev = self.get(k).map_err(|e| e.to_string())?;
        // 2. 检查是否与 old 匹配
        let prev_bytes = prev.as_ref().map(|iv| iv.as_ref());
        if prev_bytes != old {
            return Err("cas mismatch".into());
        }
        // 3. 执行写或删
        let old_return = if let Some(nv) = new {
            self.insert(k, nv).map_err(|e| e.to_string())?
        } else {
            self.remove(k).map_err(|e| e.to_string())?
        };
        Ok(old_return)
    }
}