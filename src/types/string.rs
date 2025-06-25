// src/types/string.rs

//! String 类型的简单存取操作
//!
//! 本模块直接基于 sled 提供 SET/GET/DEL 三种语义：
//! - SET key value → "OK" 表示写入成功
//! - GET key → 返回 value 或者 "ERR key not found"
//! - DEL key → "OK"（删除成功）或 "ERR key not found"

use sled::transaction::ConflictableTransactionError;
use anyhow::{Result, Context, anyhow};
use std::str;
use crate::engine::kv::KvEngine;

const PREFIX: &str = "string:";

/// 将一个字符串写入指定的键，已有值会被覆盖。
///
/// # 示例
///
/// ```ignore
/// let res = set(&db, "foo", "bar")?;
/// assert_eq!(res, "OK");
/// ```
///
/// # 错误
/// - 底层 sled 插入失败时，返回带上下文的错误
pub fn set<E>(db: &E, key: &str, value: &str) -> Result<String> 
where 
    E:KvEngine,
{
    let namespaced = format!("{}{}", PREFIX, key);
    db.insert(namespaced.as_bytes(), value.as_bytes())
        .with_context(|| format!("ERR failed to SET key '{}'", key))?;
    Ok("OK".to_string())
}

/// 从指定键读取一个字符串。
///
/// # 返回
/// - Ok(value)           – 键存在且值为合法 UTF-8 字符串  
/// - Ok("ERR key not found") – 键不存在  
///
/// # 错误
/// - sled 读取失败  
/// - 存储的字节不是合法 UTF-8 时，带上下文的错误
pub fn get<E>(db: &E, key: &str) -> Result<String> 
where 
    E:KvEngine,
{
    let namespaced = format!("{}{}", PREFIX, key);
    let maybe = db
        .get(namespaced.as_bytes())
        .with_context(|| format!("ERR failed to GET key '{}'", key))?;
    if let Some(ivec) = maybe {
        let s = str::from_utf8(&ivec)
            .with_context(|| format!("ERR non-utf8 data for key '{}'", key))?;
        Ok(s.to_string())
    } else {
        Ok("ERR key not found".to_string())
    }
}

/// 删除指定键。
///
/// # 返回
/// - Ok("OK")               – 键存在且删除成功  
/// - Ok("ERR key not found") – 键不存在  
///
/// # 错误
/// - sled 删除操作失败时，带上下文的错误
pub fn del<E>(db: &E, key: &str) -> Result<String> 
where 
    E:KvEngine,
{
    let namespaced = format!("{}{}", PREFIX, key);
    let existed = db
        .remove(namespaced.as_bytes())
        .with_context(|| format!("ERR failed to DEL key '{}'", key))?
        .is_some();
    if existed {
        Ok("OK".to_string())
    } else {
        Ok("ERR key not found".to_string())
    }
}

/// 原子地 +1：
/// - 如果底层是 &Db，就用 sled::transaction 保证本条命令的原子性  
/// - 如果是事务上下文 &TransactionalTree，就直接用 `db.get` / `db.insert`，
///   由外层事务一并保证原子
pub fn incr<E>(db: &E, key: &str) -> Result<String>
where
    E: KvEngine,
{
    let full_key = format!("{}{}", PREFIX, key);
    // 1) 如果能拆出 &Db，那就在这个 &Db 上开事务
    if let Some(plain) = db.as_db() {
        let tree = plain.open_tree("")?;
        let new = tree.transaction(|tx| {
            // 获取原始字节值
            let bytes = tx.get(full_key.as_bytes())?;

            // 转换并解析为 i64
            let old = if let Some(iv) = bytes {
                // 1. 转换为字符串
                let s = String::from_utf8(iv.to_vec())
                    .map_err(|_| ConflictableTransactionError::Abort("ERR value is not a valid UTF-8 string"))?;
                
                // 2. 解析为整数
                s.parse::<i64>()
                    .map_err(|_| ConflictableTransactionError::Abort("ERR value is not an integer"))?
            } else {
                0 // 键不存在时默认为 0
            };
            
            // 检查溢出
            let new = old.checked_add(1)
                .ok_or(ConflictableTransactionError::Abort("ERR increment would overflow"))?;
            
            // 写入新值
            tx.insert(full_key.as_bytes(), new.to_string().as_bytes())?;
            Ok(new)
        }).map_err(|e| anyhow!("{}", e))?;
        
        return Ok(new.to_string());
    }

    // 2) 否则我们在事务上下文里：直接用 KvEngine 的 get/insert，外层事务保证原子
    let old = db.get(full_key.as_bytes())?
        .map(|iv| String::from_utf8(iv.to_vec()).ok())
        .flatten()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let new = old.checked_add(1)
        .ok_or_else(|| anyhow!("overflow"))?;
    db.insert(full_key.as_bytes(), new.to_string().as_bytes())
        .context("ERR failed to INCR")?;
    Ok(new.to_string())
}

/// 同理实现 DECR
pub fn decr<E>(db: &E, key: &str) -> Result<String>
where
    E: KvEngine,
{
    let full_key = format!("{}{}", PREFIX, key);
    if let Some(plain) = db.as_db() {
        let tree = plain.open_tree("")?;
        let new = tree.transaction(|tx| {
            let bytes = tx.get(full_key.as_bytes())?;
            
            let old = if let Some(iv) = bytes {
                let s = String::from_utf8(iv.to_vec())
                    .map_err(|_| ConflictableTransactionError::Abort("ERR value is not a valid UTF-8 string"))?;
                
                s.parse::<i64>()
                    .map_err(|_| ConflictableTransactionError::Abort("ERR value is not an integer"))?
            } else {
                0
            };
            
            let new = old.checked_sub(1)
                .ok_or(ConflictableTransactionError::Abort("ERR decrement would underflow"))?;
            
            tx.insert(full_key.as_bytes(), new.to_string().as_bytes())?;
            Ok(new)
        }).map_err(|e| anyhow!("{}", e))?;
        
        return Ok(new.to_string());
    }
    let old = db.get(full_key.as_bytes())?
        .map(|iv| String::from_utf8(iv.to_vec()).ok())
        .flatten()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    let new = old.checked_sub(1)
        .ok_or_else(|| anyhow!("underflow"))?;
    db.insert(full_key.as_bytes(), new.to_string().as_bytes())
        .context("ERR failed to DECR")?;
    Ok(new.to_string())
}


#[cfg(test)]
mod tests {
    use super::*;
    use sled::Config;
    use anyhow::Result;

    /// 创建一个临时的 sled::Db，用于测试
    fn make_db() -> sled::Db {
        Config::new()
            .temporary(true)
            .open()
            .expect("打开临时 sled db 失败")
    }    

    #[test]
    fn test_set_get_del() -> Result<()> {
        let db = make_db();

        // 1) set & get
        assert_eq!(set(&db, "foo", "bar")?, "OK");
        assert_eq!(get(&db, "foo")?, "bar");

        // 2) overwrite
        assert_eq!(set(&db, "foo", "baz")?, "OK");
        assert_eq!(get(&db, "foo")?, "baz");

        // 3) del existing
        assert_eq!(del(&db, "foo")?, "OK");
        assert_eq!(get(&db, "foo")?, "ERR key not found");

        // 4) del again → not found
        assert_eq!(del(&db, "foo")?, "ERR key not found");

        Ok(())
    }

        #[test]
    fn test_get_nonexistent() -> Result<()> {
        let db = make_db();
        assert_eq!(get(&db, "does_not_exist")?, "ERR key not found");
        Ok(())
    }

    #[test]
    fn test_del_nonexistent() -> Result<()> {
        let db = make_db();
        assert_eq!(del(&db, "does_not_exist")?, "ERR key not found");
        Ok(())
    }

    #[test]
    fn test_incr_and_decr_basic() -> Result<()> {
        let db = make_db();

        // incr 1 → 1
        assert_eq!(incr(&db, "counter")?, "1");
        // incr again → 2
        assert_eq!(incr(&db, "counter")?, "2");
        // decr → 1
        assert_eq!(decr(&db, "counter")?, "1");
        // decr → 0
        assert_eq!(decr(&db, "counter")?, "0");
        // decr → -1
        assert_eq!(decr(&db, "counter")?, "-1");

        // 再 incr 回到 0
        assert_eq!(incr(&db, "counter")?, "0");

        Ok(())
    }

#[test]
fn test_incr_overflow() {
    let db = make_db();
    let key = "overflow";
    let full_key = format!("{}{}",PREFIX,key);
    let max = i64::MAX.to_string();
    
    // 写入后立即读取验证
    set(&db, &key, &max).unwrap();
    let value = db.get(full_key.as_bytes()).unwrap();
    if let Some(iv) = value {
        let s = String::from_utf8_lossy(&iv);
        println!("Value after set: {}", s);
    } else {
        println!("Value not found after set!");
    }
    
    let result = incr(&db, &key);
    let value = db.get(full_key.as_bytes()).unwrap();
    if let Some(iv) = value {
        let s = String::from_utf8_lossy(&iv);
        println!("Value after incr: {}", s);
    } else {
        println!("Value not found after incr!");
    }
    
    match result {
        Ok(val) => panic!("Expected error but got Ok({})", val),
        Err(e) => {
            println!("Received error: {}", e);
            assert!(e.to_string().contains("overflow"));
        }
    }
}

    #[test]
    fn test_decr_underflow() {
        let db = make_db();
        let key = "underflow";
        let full_key = format!("{}{}",PREFIX,key);
        let min = i64::MIN.to_string();

        set(&db, &key, &min).unwrap();

        let result = decr(&db, &key);
        let value = db.get(full_key.as_bytes()).unwrap();
        if let Some(iv) = value {
            let s = String::from_utf8_lossy(&iv);
            print!("Value after decr:{}", s)
        } else {
            println!("Value not found after decr!");
        }

        match result {
            Ok(val) => panic!("Expected error but got Ok({})", val),
            Err(e) => {
                println!("Received error: {}", e);
                assert!(e.to_string().contains("underflow"));
        }
    }
    }
}