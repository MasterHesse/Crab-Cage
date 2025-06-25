// src/types/set.rs

//! # Set Type Support
//!
//! This module implements Redis-like Set data structures on top of `sled`.
//! Each set is stored as a separate `sled::Tree` named `"set:<key>"`.
//! Members are stored as the tree’s keys with empty values `()`,
//! providing O(log n) insertion, removal, and membership checks.
//!
//! Supported commands:
//! - `SADD`
//! - `SREM`
//! - `SMEMBERS`
//! - `SISMEMBER`

use anyhow::{Result,Context};
use crate::engine::kv::KvEngine;

const PREFIX: &str = "set:";

/// Execute the SADD command:
/// Add the specified `member` to the set stored at `key`.
///
/// # Arguments
///
/// * `db`     – Reference to the opened `sled::Db`.
/// * `key`    – Name of the set.
/// * `member` – Member to add to the set.
///
/// # Returns
///
/// * `"1"` if the member was newly added (did not exist before).
/// * `"0"` if the member was already present in the set.
///
/// # Errors
///
/// Returns an error if opening the tree, inserting the member,
/// or flushing the tree fails.
pub fn sadd<E>(db: &E, key: &str, member: &str) -> Result<String> 
where 
    E: KvEngine,
{
    let namespaced = format!("{}{}:{}", PREFIX, key, member);
    let prev = db
        .insert(namespaced.as_bytes(), &[])
        .with_context(|| format!("ERR failed to SADD {}/{}", key, member))?;
    Ok(if prev.is_none() { "1".into() } else { "0".into() })
}

/// Execute the SREM command:
/// Remove the specified `member` from the set stored at `key`.
///
/// # Arguments
///
/// * `db`     – Reference to the opened `sled::Db`.
/// * `key`    – Name of the set.
/// * `member` – Member to remove from the set.
///
/// # Returns
///
/// * `"1"` if the member existed and was removed.
/// * `"0"` if the member did not exist.
///
/// # Errors
///
/// Returns an error if opening the tree, removing the member,
/// or flushing the tree fails.
pub fn srem<E>(db: &E, key: &str, member: &str) -> Result<String> 
where 
    E: KvEngine,
{
    let namespaced = format!("{}{}:{}", PREFIX, key, member);
    let prev = db
        .remove(namespaced.as_bytes())
        .with_context(|| format!("ERR failed to SREM {}/{}", key, member))?;
    Ok(if prev.is_some() { "1".into() } else { "0".into() })
}


/// Execute the SISMEMBER command:
/// Check if the specified `member` exists in the set stored at `key`.
///
/// # Arguments
///
/// * `db`     – Reference to the opened `sled::Db`.
/// * `key`    – Name of the set.
/// * `member` – Member to check for existence.
///
/// # Returns
///
/// * `"1"` if the member exists in the set.
/// * `"0"` if the member does not exist.
///
/// # Errors
///
/// Returns an error if opening the tree or checking for the key fails.
pub fn sismember<E>(db: &E, key: &str, member: &str) -> Result<String> 
where 
    E:KvEngine
{
    let namespaced = format!("{}{}:{}", PREFIX,key,member);
    let exist = db
        .get(namespaced.as_bytes())
        .with_context(|| format!("ERR failed to SISMEMBER {}/{}", key, member))?
        .is_some();
    Ok(if exist { "1".into() } else { "0".into() })
}

/// Execute the SMEMBERS command:
/// Retrieve all members of the set stored at `key`.
///
/// # Arguments
///
/// * `db`  – Reference to the opened `sled::Db`.
/// * `key` – Name of the set.
///
/// # Returns
///
/// A comma-separated `String` of all members in the set.
/// Returns an empty string if the set does not exist or has no members.
///
/// # Errors
///
/// Returns an error if opening the tree, iterating entries,
/// or converting bytes to UTF-8 strings fails.
pub fn smembers<E>(db: &E, key: &str) -> Result<String> 
where 
    E:KvEngine
{
    let prefix = format!("{}{}:",PREFIX,key);
    let mut members = Vec::new();
    for item in db.scan_prefix(prefix.as_bytes()) {
        let (k, _) = item?;
        members.push(std::str::from_utf8(&k[prefix.len()..])?.to_string());
    }
    Ok(members.join(","))
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

    /// Basic tests for Set commands: SADD, SREM, SMEMBERS, SISMEMBER
    #[test]
    fn test_set_basic() -> Result<()> {
        // Create a temporary directory and open a sled database.
        let db = make_db();

        // SADD: add members "a" and "b"
        assert_eq!(sadd(&db, "S", "a")?, "1");
        assert_eq!(sadd(&db, "S", "a")?, "0"); // already exists
        assert_eq!(sadd(&db, "S", "b")?, "1");

        // SISMEMBER: check membership
        assert_eq!(sismember(&db, "S", "a")?, "1");
        assert_eq!(sismember(&db, "S", "x")?, "0");

        // SMEMBERS: list all members
        let members = smembers(&db, "S")?;
        let mut ms: Vec<&str> = members.split(',').collect();
        ms.sort();
        assert_eq!(ms, vec!["a", "b"]);

        // SREM: remove member "a"
        assert_eq!(srem(&db, "S", "a")?, "1");
        assert_eq!(srem(&db, "S", "a")?, "0"); // already removed

        // After removal, only "b" remains
        let remaining = smembers(&db, "S")?;
        assert_eq!(remaining, "b");

        Ok(())
    }
}