// src/types/hash.rs

//! # Hash Type Support
//! 
//! This module implements Redis-like Hash data structures on top of `sled`.
//! Each hash key is stored as a separate `sled::Tree` named `"hash:<key>"`.
//! 
//! Supported commands:
//! - `HSET`
//! - `HGET`
//! - `HDEL`
//! - `HKEYS`
//! - `HVALS`
//! - `HGETALL`

use anyhow::{Context, Ok, Result};
use crate::engine::kv::KvEngine;

const PREFIX: &str = "hash:";

/// Execute the HSET command:
/// Set the string value of a hash field.
///
/// # Arguments
///
/// * `db`    – Reference to the opened `sled::Db`.
/// * `key`   – Name of the hash.
/// * `field` – Field within the hash to set.
/// * `value` – Value to associate with `field`.
///
/// # Returns
///
/// * `"1"` if a new field was created.
/// * `"0"` if an existing field’s value was overwritten.
///
/// # Errors
///
/// Returns an error if opening the tree, inserting the value, or flushing the tree fails.
pub fn hset<E>(db: &E, key: &str, field: &str, value: &str) -> Result<String> 
where 
    E: KvEngine,
{
    let namespaced = format!("{}{}:{}", PREFIX, key, field);
    let prev = db
        .insert(namespaced.as_bytes(), value.as_bytes())
        .with_context(|| format!("ERR failed to HSET {}/{}", key, field))?;

    Ok(if prev.is_none() { "1".into() } else { "0".into() })
}

/// Execute the HGET command:
/// Get the value of a hash field.
///
/// # Arguments
///
/// * `db`    – Reference to the opened `sled::Db`.
/// * `key`   – Name of the hash.
/// * `field` – Field whose value to retrieve.
///
/// # Returns
///
/// * The field’s value as `String` if it exists.
/// * `"nil"` if the field does not exist.
///
/// # Errors
///
/// Returns an error if opening the tree, reading the value, or UTF-8 conversion fails.
pub fn hget<E>(db: &E, key: &str, field: &str) -> Result<String> 
where 
    E:KvEngine,
{
    let namespaced = format!("{}{}:{}", PREFIX, key, field);
    if let Some(bytes) = db.get(namespaced.as_bytes())? {
        let s = std::str::from_utf8(&bytes)
            .context("ERR non-utf8 in HGET")?;
        Ok(s.to_string())
    } else {
        Ok("nil".into())
    }
}

/// Execute the HDEL command:
/// Delete one or more hash fields.
///
/// # Arguments
///
/// * `db`    – Reference to the opened `sled::Db`.
/// * `key`   – Name of the hash.
/// * `field` – Field to delete.
///
/// # Returns
///
/// * `"1"` if the field existed and was removed.
/// * `"0"` if the field did not exist.
///
/// # Errors
///
/// Returns an error if opening the tree, removing the value, or flushing the tree fails.
pub fn hdel<E>(db: &E, key: &str, field: &str) -> Result<String> 
where 
    E:KvEngine
{
    let namespaced = format!("{}{}:{}", PREFIX, key, field);
    let removed = db.remove(namespaced.as_bytes())?;
    Ok(if removed.is_some() { "1".into() } else { "0".into() })
}

/// Execute the HKEYS command:
/// Get all field names in a hash.
///
/// # Arguments
///
/// * `db`  – Reference to the opened `sled::Db`.
/// * `key` – Name of the hash.
///
/// # Returns
///
/// A comma-separated `String` of all field names. Returns an empty string if the hash does not exist or has no fields.
///
/// # Errors
///
/// Returns an error if opening the tree, iterating, or UTF-8 conversion fails.
pub fn hkeys<E>(db: &E, key: &str) -> Result<String> 
where 
    E:KvEngine,
{
    let prefix = format!("{}{}:", PREFIX, key);
    let mut fields = Vec::new();
    
    for entry in db.scan_prefix(prefix.as_bytes()) {
        let (k, _) = entry?;
        let field = std::str::from_utf8(&k[prefix.len()..])?;
        fields.push(field.to_string());
    }
    
    Ok(fields.join(","))
}

/// Execute the HVALS command:
/// Get all values in a hash.
///
/// # Arguments
///
/// * `db`  – Reference to the opened `sled::Db`.
/// * `key` – Name of the hash.
///
/// # Returns
///
/// A comma-separated `String` of all values. Returns an empty string if the hash does not exist or has no fields.
///
/// # Errors
///
/// Returns an error if opening the tree, iterating, or UTF-8 conversion fails.
pub fn hvals<E>(db: &E, key: &str) -> Result<String> 
where 
    E: KvEngine,
{
    let prefix = format!("{}{}:", PREFIX, key);
    let mut values = Vec::new();
    
    for entry in db.scan_prefix(prefix.as_bytes()) {
        let (_, v) = entry?;
        let value = std::str::from_utf8(&v)?;
        values.push(value.to_string());
    }
    
    Ok(values.join(","))
}

/// Execute the HGETALL command:
/// Get all fields and values in a hash.
///
/// # Arguments
///
/// * `db`  – Reference to the opened `sled::Db`.
/// * `key` – Name of the hash.
///
/// # Returns
///
/// A comma-separated `String` in the form `field1,value1,field2,value2,…`.
/// Returns an empty string if the hash does not exist or has no fields.
///
/// # Errors
///
/// Returns an error if opening the tree, iterating, or UTF-8 conversion fails.
pub fn hgetall<E>(db: &E, key: &str) -> Result<String> 
where 
    E: KvEngine
{
    let prefix = format!("{}{}:", PREFIX, key);
    let mut entries = Vec::new();
    for entry in db.scan_prefix(prefix.as_bytes()) {
        let (k, v) = entry?;
        entries.push(std::str::from_utf8(&k[prefix.len()..])?.to_string());
        entries.push(std::str::from_utf8(&v)?.to_string());
    }
    Ok(entries.join(","))
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

    /// Basic tests for Hash commands: HSET, HGET, HDEL, HKEYS, HVALS, HGETALL
    #[test]
    fn test_hash_basic() -> Result<()> {
        // Create a temporary directory and open a sled database inside it.
        let db = make_db();

        // HSET on a new field should return "1"
        assert_eq!(hset(&db, "myhash", "f1", "v1")?, "1");
        // HSET on an existing field should return "0"
        assert_eq!(hset(&db, "myhash", "f1", "v1a")?, "0");
        // HGET existing field
        assert_eq!(hget(&db, "myhash", "f1")?, "v1a");
        // HGET non-existent field returns "nil"
        assert_eq!(hget(&db, "myhash", "f2")?, "nil");

        // Add another field for key/value listings
        hset(&db, "myhash", "f2", "v2")?;

        // HKEYS should list fields sorted lexicographically after split+sort
        let hk = hkeys(&db, "myhash")?;
        let mut ks: Vec<&str> = hk.split(',').collect();
        ks.sort();
        assert_eq!(ks, vec!["f1", "f2"]);

        // HVALS should list values
        let hv = hvals(&db, "myhash")?;
        let mut vs: Vec<&str> = hv.split(',').collect();
        vs.sort();
        assert_eq!(vs, vec!["v1a", "v2"]);

        // HGETALL should list interleaved field,value pairs
        let hga = hgetall(&db, "myhash")?;
        let mut elems: Vec<&str> = hga.split(',').collect();
        elems.sort();
        assert_eq!(elems, vec!["f1", "f2", "v1a", "v2"]);

        // HDEL existing field returns "1" and subsequent HGET returns "nil"
        assert_eq!(hdel(&db, "myhash", "f1")?, "1");
        assert_eq!(hget(&db, "myhash", "f1")?, "nil");
        // HDEL non-existent field returns "0"
        assert_eq!(hdel(&db, "myhash", "no")?, "0");

        Ok(())
    }
}