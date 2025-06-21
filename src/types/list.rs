// src/types/list.rs

//! # List Type Support
//!
//! This module implements Redis-like List (deque) data structures on top of `sled`.
//! Each list is stored as a separate `sled::Tree` named `"list:<key>"`.
//! Elements are indexed by big-endian bytes of an `i64` sequence number with a bitwise
//! offset flip, allowing efficient double-ended push and pop.
//!
//! Supported commands:
//! - `LPUSH`
//! - `RPUSH`
//! - `LPOP`
//! - `RPOP`
//! - `LRANGE`

use sled::{Db, Tree};
use anyhow::Result;
use std::str;

const PREFIX: &str = "list:";

/// Map an `i64` sequence number to a `u64` by flipping the sign bit,
/// so that ordering by `u64` big-endian bytes matches the signed ordering of `i64`.
fn seq_to_u64(seq: i64) -> u64 {
    (seq as u64) ^ (1 << 63)
}

/// Reverse the bitwise flip and convert back to `i64`.
fn u64_to_seq(u: u64) -> i64 {
    (u ^ (1 << 63)) as i64
}

/// Convert an `i64` sequence number into an 8-byte big-endian array key.
fn seq_to_key(seq: i64) -> [u8; 8] {
    seq_to_u64(seq).to_be_bytes()
}

/// Parse an 8-byte big-endian array key back into an `i64` sequence number.
fn key_to_seq(k: &[u8]) -> i64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&k[0..8]);
    u64_to_seq(u64::from_be_bytes(b))
}

/// Retrieve the current head and tail sequence numbers (min and max) from the tree.
/// Returns `Ok(Some((head, tail)))` if the list is non-empty,
/// or `Ok(None)` if the list has no elements.
///
/// # Errors
/// Returns an error if iteration over the tree fails.
fn get_bounds(tree: &Tree) -> Result<Option<(i64, i64)>> {
    let mut iter = tree.iter();
    if let Some(Ok((first_k, _))) = iter.next() {
        let head = key_to_seq(&first_k);
        // Find tail by reversing the iteration
        let mut last = head;
        for item in tree.iter().rev() {
            let (k, _) = item?;
            last = key_to_seq(&k);
            break;
        }
        Ok(Some((head, last)))
    } else {
        Ok(None)
    }
}

/// Execute LPUSH:
/// Push `value` to the head (left) of the list stored at `key`.
///
/// # Arguments
///
/// * `db`    – Reference to the opened `sled::Db`.
/// * `key`   – Name of the list.
/// * `value` – Value to push.
///
/// # Returns
///
/// The new length of the list as a string.
///
/// # Errors
///
/// Returns an error if opening the tree, inserting the element,
/// flushing, or retrieving bounds fails.
pub fn lpush(db: &Db, key: &str, value: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let len = if let Some((head, _)) = get_bounds(&tree)? {
        let new_head = head - 1;
        tree.insert(seq_to_key(new_head), value.as_bytes())?;
        tree.flush()?;
        tree.len()
    } else {
        // First element uses sequence 0
        tree.insert(seq_to_key(0), value.as_bytes())?;
        tree.flush()?;
        1
    };
    Ok(len.to_string())
}

/// Execute RPUSH:
/// Push `value` to the tail (right) of the list stored at `key`.
///
/// # Arguments
///
/// * `db`    – Reference to the opened `sled::Db`.
/// * `key`   – Name of the list.
/// * `value` – Value to push.
///
/// # Returns
///
/// The new length of the list as a string.
///
/// # Errors
///
/// Returns an error if opening the tree, inserting the element,
/// flushing, or retrieving bounds fails.
pub fn rpush(db: &Db, key: &str, value: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let len = if let Some((_, tail)) = get_bounds(&tree)? {
        let new_tail = tail + 1;
        tree.insert(seq_to_key(new_tail), value.as_bytes())?;
        tree.flush()?;
        tree.len()
    } else {
        // First element uses sequence 0
        tree.insert(seq_to_key(0), value.as_bytes())?;
        tree.flush()?;
        1
    };
    Ok(len.to_string())
}

/// Execute LPOP:
/// Pop and return the head (leftmost) element of the list at `key`,
/// or `"nil"` if the list is empty or does not exist.
///
/// # Arguments
///
/// * `db`  – Reference to the opened `sled::Db`.
/// * `key` – Name of the list.
///
/// # Returns
///
/// The popped element as a string, or `"nil"`.
///
/// # Errors
///
/// Returns an error if opening the tree, removing the element,
/// flushing, or UTF-8 conversion fails.
pub fn lpop(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    if let Some((head, _)) = get_bounds(&tree)? {
        let k = seq_to_key(head);
        if let Some(iv) = tree.remove(&k)? {
            tree.flush()?;
            return Ok(str::from_utf8(&iv)?.to_string());
        }
    }
    Ok("nil".into())
}

/// Execute RPOP:
/// Pop and return the tail (rightmost) element of the list at `key`,
/// or `"nil"` if the list is empty or does not exist.
///
/// # Arguments
///
/// * `db`  – Reference to the opened `sled::Db`.
/// * `key` – Name of the list.
///
/// # Returns
///
/// The popped element as a string, or `"nil"`.
///
/// # Errors
///
/// Returns an error if opening the tree, removing the element,
/// flushing, or UTF-8 conversion fails.
pub fn rpop(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    if let Some((_, tail)) = get_bounds(&tree)? {
        let k = seq_to_key(tail);
        if let Some(iv) = tree.remove(&k)? {
            tree.flush()?;
            return Ok(str::from_utf8(&iv)?.to_string());
        }
    }
    Ok("nil".into())
}

/// Execute LRANGE:
/// Return a comma-separated list of elements in the range `[start, stop]`,
/// supporting negative indices (counting from the end).
///
/// # Arguments
///
/// * `db`    – Reference to the opened `sled::Db`.
/// * `key`   – Name of the list.
/// * `start` – Zero-based start index or negative for offset from end.
/// * `stop`  – Zero-based stop index or negative for offset from end.
///
/// # Returns
///
/// A comma-separated string of elements in the specified range.
/// Returns an empty string if there are no elements in range or the list is empty.
///
/// # Errors
///
/// Returns an error if opening the tree, retrieving bounds,
/// iterating elements, or UTF-8 conversion fails.
pub fn lrange(db: &Db, key: &str, start: isize, stop: isize) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let (head, tail) = match get_bounds(&tree)? {
        Some(b) => b,
        None => return Ok(String::new()),
    };
    // Total number of elements
    let total = (tail - head + 1) as isize;
    // Normalize negative indices
    let s = if start < 0 { total + start } else { start };
    let e = if stop  < 0 { total + stop  } else { stop };
    // Clamp to valid range
    let s = s.max(0).min(total - 1) as i64;
    let e = e.max(0).min(total - 1) as i64;
    if s > e {
        return Ok(String::new());
    }
    let mut out = Vec::new();
    for idx in s..=e {
        let seq = head + idx;
        if let Some(iv) = tree.get(seq_to_key(seq))? {
            out.push(str::from_utf8(&iv)?.to_string());
        }
    }
    Ok(out.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::env;

    /// Basic tests for List commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE
    #[test]
    fn test_list_basic() -> Result<()> {
        let tmp = tempdir()?;
        env::set_current_dir(&tmp)?;
        let db = sled::open("ldb")?;

        // LPUSH / RPUSH
        assert_eq!(lpush(&db, "L", "a")?, "1");
        assert_eq!(lpush(&db, "L", "b")?, "2"); // b, a
        assert_eq!(rpush(&db, "L", "c")?, "3"); // b, a, c

        // LRANGE full and single-element
        assert_eq!(lrange(&db, "L", 0, 2)?, "b,a,c");
        assert_eq!(lrange(&db, "L", 1, 1)?, "a");

        // LPOP, RPOP
        assert_eq!(lpop(&db, "L")?, "b");
        assert_eq!(rpop(&db, "L")?, "c");
        assert_eq!(lrange(&db, "L", 0, -1)?, "a");

        // Exhaust and empty pops
        assert_eq!(lpop(&db, "L")?, "a");
        assert_eq!(lpop(&db, "L")?, "nil");
        assert_eq!(rpop(&db, "L")?, "nil");
        Ok(())
    }
}