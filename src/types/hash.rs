// src/types/hash.rs

//! 支持 Hash 类型：每个 hash 对应一个 sled::Tree
//! Tree 名称: "hash:<hashname>"

use sled::{Db, IVec};
use anyhow::Result;

/// HSET key field value
/// 返回 "1" 表示新建了 field，"0" 表示覆盖了旧值
pub fn hset(db: &Db, key: &str, field: &str, value: &str) -> Result<String> {
    let tree = db.open_tree(format!("hash:{}", key))?;
    let prev = tree.insert(field, value.as_bytes())?;
    tree.flush()?;
    Ok(if prev.is_none() { "1".into() } else { "0".into() })
}

/// HGET key field
/// 返回值，field 不存在时返回 "nil"
pub fn hget(db: &Db, key: &str, field: &str) -> Result<String> {
    let tree = db.open_tree(format!("hash:{}", key))?;
    match tree.get(field)? {
        Some(ivec) => {
            let s = std::str::from_utf8(&ivec)?;
            Ok(s.to_string())
        }
        None => Ok("nil".into()),
    }
}

/// HDEL key field
/// 返回 "1" 如果删除了一个存在的 field，"0" 否则
pub fn hdel(db: &Db, key: &str, field: &str) -> Result<String> {
    let tree = db.open_tree(format!("hash:{}", key))?;
    let prev = tree.remove(field)?;
    tree.flush()?;
    Ok(if prev.is_some() { "1".into() } else { "0".into() })
}

/// HKEYS key
/// 返回所有 field，用逗号分隔。不存在时返回空字符串。
pub fn hkeys(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("hash:{}", key))?;
    let mut v = Vec::new();
    for pair in tree.iter() {
        let (k, _v) = pair?;
        v.push(String::from_utf8(k.to_vec())?);
    }
    Ok(v.join(","))
}

/// HVALS key
/// 返回所有 value，用逗号分隔。不存在时返回空字符串。
pub fn hvals(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("hash:{}", key))?;
    let mut v = Vec::new();
    for pair in tree.iter() {
        let (_k, iv) = pair?;
        v.push(std::str::from_utf8(&iv)?.to_string());
    }
    Ok(v.join(","))
}

/// HGETALL key
/// 返回 field1,value1,field2,value2… 用逗号分隔。不存在时返回空字符串。
pub fn hgetall(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("hash:{}", key))?;
    let mut v = Vec::new();
    for pair in tree.iter() {
        let (k, iv) = pair?;
        v.push(String::from_utf8(k.to_vec())?);
        v.push(std::str::from_utf8(&iv)?.to_string());
    }
    Ok(v.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::env;

    #[test]
    fn test_hash_basic() -> Result<()> {
        let tmp = tempdir()?;
        env::set_current_dir(&tmp)?;
        let db = sled::open("hdb")?;

        // HSET 新 field
        assert_eq!(hset(&db, "myhash", "f1", "v1")?, "1");
        // HSET 覆盖 field
        assert_eq!(hset(&db, "myhash", "f1", "v1a")?, "0");
        // HGET 存在
        assert_eq!(hget(&db, "myhash", "f1")?, "v1a");
        // HGET 不存在
        assert_eq!(hget(&db, "myhash", "f2")?, "nil");

        // HKEYS / HVALS / HGETALL
        hset(&db, "myhash", "f2", "v2")?;
        let keys = hkeys(&db, "myhash")?;
        let mut ks: Vec<&str> = keys.split(',').collect();
        ks.sort();
        assert_eq!(ks, vec!["f1", "f2"]);

        let vals = hvals(&db, "myhash")?;
        let mut vs: Vec<&str> = vals.split(',').collect();
        vs.sort();
        assert_eq!(vs, vec!["v1a", "v2"]);

        let all = hgetall(&db, "myhash")?;
        let mut elems: Vec<&str> = all.split(',').collect();
        elems.sort();
        assert_eq!(elems, vec!["f1", "f2", "v1a", "v2"]);

        // HDEL 存在
        assert_eq!(hdel(&db, "myhash", "f1")?, "1");
        assert_eq!(hget(&db, "myhash", "f1")?, "nil");
        // HDEL 不存在
        assert_eq!(hdel(&db, "myhash", "no")?, "0");

        Ok(())
    }
}