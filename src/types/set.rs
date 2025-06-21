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

use sled::Db;
use anyhow::Result;

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
pub fn sadd(db: &Db, key: &str, member: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let prev = tree.insert(member, &[])?;
    tree.flush()?;
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
pub fn srem(db: &Db, key: &str, member: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let prev = tree.remove(member)?;
    tree.flush()?;
    Ok(if prev.is_some() { "1".into() } else { "0".into() })
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
pub fn smembers(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let mut members = Vec::new();
    for item in tree.iter() {
        let (k, _) = item?;
        members.push(String::from_utf8(k.to_vec())?);
    }
    Ok(members.join(","))
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
pub fn sismember(db: &Db, key: &str, member: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    Ok(if tree.contains_key(member)? { "1".into() } else { "0".into() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::env;

    /// Basic tests for Set commands: SADD, SREM, SMEMBERS, SISMEMBER
    #[test]
    fn test_set_basic() -> Result<()> {
        // Create a temporary directory and open a sled database.
        let tmp = tempdir()?;
        env::set_current_dir(&tmp)?;
        let db = sled::open("sdb")?;

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