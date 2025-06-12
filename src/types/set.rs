// src/types/set.rs

//! Set 类型：每个 set 用一个 sled::Tree "set:<key>"，
//! key=member,value=()

use sled::Db;
use anyhow::Result;

const PREFIX: &str = "set:";

/// SADD key member
/// 返回 "1" 新增一个，"0" 已存在
pub fn sadd(db: &Db, key: &str, member: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let prev = tree.insert(member, &[])?;
    tree.flush()?;
    Ok(if prev.is_none() { "1".into() } else { "0".into() })
}

/// SREM key member
/// 返回 "1" 删除成功，"0" 不存在
pub fn srem(db: &Db, key: &str, member: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let prev = tree.remove(member)?;
    tree.flush()?;
    Ok(if prev.is_some() { "1".into() } else { "0".into() })
}

/// SMEMBERS key
/// 返回所有 member，用逗号分隔；空集合返回空字符串
pub fn smembers(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let mut v = Vec::new();
    for item in tree.iter() {
        let (k, _) = item?;
        v.push(String::from_utf8(k.to_vec())?);
    }
    Ok(v.join(","))
}

/// SISMEMBER key member
/// 返回 "1" 存在，"0" 不存在
pub fn sismember(db: &Db, key: &str, member: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    Ok(if tree.contains_key(member)? { "1".into() } else { "0".into() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::env;

    #[test]
    fn test_set_basic() -> Result<()> {
        let tmp = tempdir()?;
        env::set_current_dir(&tmp)?;
        let db = sled::open("sdb")?;

        // SADD
        assert_eq!(sadd(&db, "S", "a")?, "1");
        assert_eq!(sadd(&db, "S", "a")?, "0");
        assert_eq!(sadd(&db, "S", "b")?, "1");

        // SISMEMBER
        assert_eq!(sismember(&db, "S", "a")?, "1");
        assert_eq!(sismember(&db, "S", "x")?, "0");

        // SMEMBERS
        let members = smembers(&db, "S")?;
        let mut ms: Vec<&str> = members.split(',').collect();
        ms.sort();
        assert_eq!(ms, vec!["a", "b"]);

        // SREM
        assert_eq!(srem(&db, "S", "a")?, "1");
        assert_eq!(srem(&db, "S", "a")?, "0");
        let members = smembers(&db, "S")?;
        assert_eq!(members, "b");

        Ok(())
    }
}