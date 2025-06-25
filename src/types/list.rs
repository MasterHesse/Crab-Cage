// src/types/list.rs

use anyhow::{Context, Result};
use sled::transaction::ConflictableTransactionError;
use std::str;
use crate::engine::kv::KvEngine;

const DATA_PREFIX: &str = "list:data:";
const META_PREFIX: &str = "list:meta:";

/// 将序列号转换为排序友好的 u64 表示
fn seq_to_u64(seq: i64) -> u64 {
    (seq as u64) ^ (1 << 63)
}

/// 获取/设置 i64 元数据
fn get_i64<E: KvEngine>(db: &E, key: &str) -> Result<Option<i64>> {
    if let Some(bs) = db.get(key.as_bytes())? {
        let arr: [u8; 8] = bs.as_ref().try_into()?;
        Ok(Some(i64::from_be_bytes(arr)))
    } else {
        Ok(None)
    }
}

fn put_i64<E: KvEngine>(db: &E, key: &str, value: i64) -> Result<()> {
    db.insert(key.as_bytes(), &value.to_be_bytes())?;
    Ok(())
}

/// 获取列表的 head 和 tail
fn get_bounds<E: KvEngine>(db: &E, key: &str) -> Result<Option<(i64, i64)>> {
    let head_key = format!("{}{}:head", META_PREFIX, key);
    let tail_key = format!("{}{}:tail", META_PREFIX, key);
    
    let head = match get_i64(db, &head_key)? {
        Some(h) => h,
        None => return Ok(None),
    };
    
    let tail = get_i64(db, &tail_key)?
        .with_context(|| format!("Missing tail metadata for list '{}'", key))?;
    
    Ok(Some((head, tail)))
}

/// LPUSH 实现
pub fn lpush<E: KvEngine>(db: &E, key: &str, value: &str) -> Result<String> {
    let (head, tail) = match get_bounds(db, key)? {
        Some((h, t)) => (h, t),
        None => (0, -1),  // 空列表
    };
    
    let new_head = head - 1;
    let data_key = format!("{}{}:{}", DATA_PREFIX, key, seq_to_u64(new_head));
    
    // 在事务中执行所有操作
    if let Some(plain_db) = db.as_db() {
        let tree = plain_db.open_tree("")?;
        tree.transaction(|tx| {
            tx.insert(data_key.as_bytes(), value.as_bytes())?;
            
            // 更新 head
            let head_key = format!("{}{}:head", META_PREFIX, key);
            tx.insert(head_key.as_bytes(), &new_head.to_be_bytes())?;
            
            // 如果是第一个元素，更新 tail
            if tail < head {
                let tail_key = format!("{}{}:tail", META_PREFIX, key);
                tx.insert(tail_key.as_bytes(), &new_head.to_be_bytes())?;
            }
            
            Ok::<(), ConflictableTransactionError>(())
        })?;
    } else {
        // 在事务上下文中
        db.insert(data_key.as_bytes(), value.as_bytes())?;
        
        let head_key = format!("{}{}:head", META_PREFIX, key);
        db.insert(head_key.as_bytes(), &new_head.to_be_bytes())?;
        
        if tail < head {
            let tail_key = format!("{}{}:tail", META_PREFIX, key);
            db.insert(tail_key.as_bytes(), &new_head.to_be_bytes())?;
        }
    }
    
    // 计算新长度
    let new_tail = if tail < head { new_head } else { tail };
    let len = (new_tail - new_head + 1) as usize;
    Ok(len.to_string())
}

/// RPUSH 实现
pub fn rpush<E: KvEngine>(db: &E, key: &str, value: &str) -> Result<String> {
    let (head, tail) = match get_bounds(db, key)? {
        Some((h, t)) => (h, t),
        None => (0, -1),
    };
    
    let new_tail = tail + 1;
    let data_key = format!("{}{}:{}", DATA_PREFIX, key, seq_to_u64(new_tail));
    
    // 在事务中执行所有操作
    if let Some(plain_db) = db.as_db() {
        let tree = plain_db.open_tree("")?;
        tree.transaction(|tx| {
            tx.insert(data_key.as_bytes(), value.as_bytes())?;
            
            // 更新 tail
            let tail_key = format!("{}{}:tail", META_PREFIX, key);
            tx.insert(tail_key.as_bytes(), &new_tail.to_be_bytes())?;
            
            // 如果是第一个元素，更新 head
            if tail < head {
                let head_key = format!("{}{}:head", META_PREFIX, key);
                tx.insert(head_key.as_bytes(), &new_tail.to_be_bytes())?;
            }
            
            Ok::<(), ConflictableTransactionError>(())
        })?;
    } else {
        db.insert(data_key.as_bytes(), value.as_bytes())?;
        
        let tail_key = format!("{}{}:tail", META_PREFIX, key);
        db.insert(tail_key.as_bytes(), &new_tail.to_be_bytes())?;
        
        if tail < head {
            let head_key = format!("{}{}:head", META_PREFIX, key);
            db.insert(head_key.as_bytes(), &new_tail.to_be_bytes())?;
        }
    }
    
    // 计算新长度
    let new_head = if tail < head { new_tail } else { head };
    let len = (new_tail - new_head + 1) as usize;
    Ok(len.to_string())
}

/// LPOP 实现
pub fn lpop<E: KvEngine>(db: &E, key: &str) -> Result<String> {
    let (head, tail) = match get_bounds(db, key)? {
        Some(ht) => ht,
        None => return Ok("nil".into()),
    };
    
    let data_key = format!("{}{}:{}", DATA_PREFIX, key, seq_to_u64(head));
    let result = if let Some(bs) = db.remove(data_key.as_bytes())? {
        // 更新元数据
        if head + 1 > tail {
            // 列表为空，删除元数据
            let head_key = format!("{}{}:head", META_PREFIX, key);
            let tail_key = format!("{}{}:tail", META_PREFIX, key);
            
            if let Some(plain_db) = db.as_db() {
                let tree = plain_db.open_tree("")?;
                tree.transaction(|tx| {
                    tx.remove(head_key.as_bytes())?;
                    tx.remove(tail_key.as_bytes())?;
                    Ok::<(), ConflictableTransactionError>(())
                })?;
            } else {
                db.remove(head_key.as_bytes())?;
                db.remove(tail_key.as_bytes())?;
            }
        } else {
            // 更新 head
            let head_key = format!("{}{}:head", META_PREFIX, key);
            put_i64(db, &head_key, head + 1)?;
        }
        
        String::from_utf8(bs.to_vec())?
    } else {
        "nil".into()
    };
    
    Ok(result)
}

/// RPOP 实现
pub fn rpop<E: KvEngine>(db: &E, key: &str) -> Result<String> {
    let (head, tail) = match get_bounds(db, key)? {
        Some(ht) => ht,
        None => return Ok("nil".into()),
    };
    
    let data_key = format!("{}{}:{}", DATA_PREFIX, key, seq_to_u64(tail));
    let result = if let Some(bs) = db.remove(data_key.as_bytes())? {
        // 更新元数据
        if head > tail - 1 {
            // 列表为空，删除元数据
            let head_key = format!("{}{}:head", META_PREFIX, key);
            let tail_key = format!("{}{}:tail", META_PREFIX, key);
            
            if let Some(plain_db) = db.as_db() {
                let tree = plain_db.open_tree("")?;
                tree.transaction(|tx| {
                    tx.remove(head_key.as_bytes())?;
                    tx.remove(tail_key.as_bytes())?;
                    Ok::<(), ConflictableTransactionError>(())
                })?;
            } else {
                db.remove(head_key.as_bytes())?;
                db.remove(tail_key.as_bytes())?;
            }
        } else {
            // 更新 tail
            let tail_key = format!("{}{}:tail", META_PREFIX, key);
            put_i64(db, &tail_key, tail - 1)?;
        }
        
        String::from_utf8(bs.to_vec())?
    } else {
        "nil".into()
    };
    
    Ok(result)
}

/// LRANGE 实现
pub fn lrange<E: KvEngine>(
    db: &E, 
    key: &str, 
    start: isize, 
    stop: isize
) -> Result<String> {
    let (head, tail) = match get_bounds(db, key)? {
        Some((h, t)) => (h, t),
        None => return Ok(String::new()), // 空列表
    };
    
    let total = (tail - head + 1) as isize;
    if total <= 0 {
        return Ok(String::new());
    }
    
    // 处理负索引
    let s = if start < 0 { total + start } else { start };
    let e = if stop < 0 { total + stop } else { stop };
    
    // 边界检查
    let s = s.max(0).min(total - 1) as i64;
    let e = e.max(0).min(total - 1) as i64;
    
    if s > e {
        return Ok(String::new());
    }
    
    let mut results = Vec::new();
    for idx in s..=e {
        let seq = head + idx;
        let data_key = format!("{}{}:{}", DATA_PREFIX, key, seq_to_u64(seq));
        
        if let Some(bs) = db.get(data_key.as_bytes())? {
            let value = String::from_utf8(bs.to_vec())?;
            results.push(value);
        }
    }
    
    Ok(results.join(","))
}


#[cfg(test)]
mod tests {
    use super::*;
    use sled::Config;

    /// 创建一个临时的 sled::Db，用于测试
    fn make_db() -> sled::Db {
        Config::new()
            .temporary(true)
            .open()
            .expect("打开临时 sled db 失败")
    } 

    #[test]
    fn test_list_operations() {
    let db = make_db();
    
    // 基本操作
    assert_eq!(lpush(&db, "mylist", "world").unwrap(), "1");
    assert_eq!(lpush(&db, "mylist", "hello").unwrap(), "2");
    assert_eq!(rpush(&db, "mylist", "!").unwrap(), "3");
    
    assert_eq!(lpop(&db, "mylist").unwrap(), "hello");
    assert_eq!(rpop(&db, "mylist").unwrap(), "!");
    
    // 范围查询
    assert_eq!(lrange(&db, "mylist", 0, -1).unwrap(), "world");
    
}
}