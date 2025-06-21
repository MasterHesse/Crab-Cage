// src/types/string.rs

//! String 类型的简单存取操作
//!
//! 本模块直接基于 sled 提供 SET/GET/DEL 三种语义：
//! - SET key value → "OK" 表示写入成功
//! - GET key → 返回 value 或者 "ERR key not found"
//! - DEL key → "OK"（删除成功）或 "ERR key not found"

use sled::Db;
use anyhow::{Result, Context, anyhow};
use std::str;

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
pub fn set(db: &Db, key: &str, value: &str) -> Result<String> {
    db.insert(key, value.as_bytes())
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
pub fn get(db: &Db, key: &str) -> Result<String> {
    let maybe = db
        .get(key)
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
pub fn del(db: &Db, key: &str) -> Result<String> {
    let existed = db
        .remove(key)
        .with_context(|| format!("ERR failed to DEL key '{}'", key))?
        .is_some();
    if existed {
        Ok("OK".to_string())
    } else {
        Ok("ERR key not found".to_string())
    }
}

/// 将 key 的整数值 +1，原子操作。不存在当 0 开始。
pub fn incr(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?; // 或者直接用 db
    let new = tree.transaction(|tx| {
        let old = tx.get(key.as_bytes())?
            .map(|ivec| String::from_utf8(ivec.to_vec()).ok())
            .flatten()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let new = old.checked_add(1)
            .ok_or(sled::transaction::ConflictableTransactionError::Abort("overflow"))?;
        tx.insert(key.as_bytes(), new.to_string().as_bytes())?;
        Ok(new)
    }).map_err(|e| anyhow!(e))?;
    Ok(new.to_string())
}

/// 将 key 的整数值 -1，原子操作。不存在当 0 开始。
pub fn decr(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let new = tree.transaction(|tx| {
        let old = tx.get(key.as_bytes())?
            .map(|ivec| String::from_utf8(ivec.to_vec()).ok())
            .flatten()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let new = old.checked_sub(1)
            .ok_or(sled::transaction::ConflictableTransactionError::Abort("overflow"))?;
        tx.insert(key.as_bytes(), new.to_string().as_bytes())?;
        Ok(new)
    }).map_err(|e| anyhow!(e))?;
    Ok(new.to_string())
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_string_basic() -> Result<()> {
        let dir = tempdir()?;
        let db = sled::open(dir.path().join("sdb"))?;

        // SET 新 key
        assert_eq!(set(&db, "k1", "v1")?, "OK");

        // GET 存在
        assert_eq!(get(&db, "k1")?, "v1");
        // GET 不存在
        assert_eq!(get(&db, "nonexistent")?, "ERR key not found");

        // DEL 存在
        assert_eq!(del(&db, "k1")?, "OK");
        // DEL 不存在
        assert_eq!(del(&db, "nonexistent")?, "ERR key not found");

        // 删除后确认不存在
        assert_eq!(get(&db, "k1")?, "ERR key not found");

        Ok(())
    }

    #[test]
    fn test_non_utf8() -> Result<()> {
        let dir = tempdir()?;
        let db = sled::open(dir.path().join("sdb"))?;

        // 直接插入非法 UTF-8 数据
        db.insert("binary", &[0xff, 0xfe, 0xfd])?;

        // 读取时应当报错，并包含 non-utf8 上下文
        let err = get(&db, "binary").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("non-utf8 data for key 'binary'"));

        Ok(())
    }

        #[test]
    fn test_incr_decr() -> Result<()> {
        let dir = tempdir()?;
        let db = sled::open(dir.path().join("sdb"))?;

        assert_eq!(incr(&db, "c")?, "1");
        assert_eq!(incr(&db, "c")?, "2");
        assert_eq!(decr(&db, "c")?, "1");
        Ok(())
    }
}