// src/types/list.rs

//! List 类型：每个 key 对应一个 sled::Tree "list:<key>"，
//! 用“偏移翻转”后的 i64 BE bytes 做索引，支持双端队列。

use sled::{Db, Tree};
use anyhow::Result;
use std::str;

const PREFIX: &str = "list:";

/// 把 i64 序号映射到顺序一致的 u64
fn seq_to_u64(seq: i64) -> u64 {
    (seq as u64) ^ (1 << 63)
}
fn u64_to_seq(u: u64) -> i64 {
    (u ^ (1 << 63)) as i64
}

fn seq_to_key(seq: i64) -> [u8; 8] {
    seq_to_u64(seq).to_be_bytes()
}
fn key_to_seq(k: &[u8]) -> i64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&k[0..8]);
    u64_to_seq(u64::from_be_bytes(b))
}

/// 获取当前 head/tail（seq 最小和最大），空时返回 None
fn get_bounds(tree: &Tree) -> Result<Option<(i64, i64)>> {
    let mut iter = tree.iter();
    if let Some(Ok((first_k, _))) = iter.next() {
        let head = key_to_seq(&first_k);
        // last: 用反向迭代：sled 支持 .iter().rev()
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

/// LPUSH key value → 返回当前长度
pub fn lpush(db: &Db, key: &str, value: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let len = if let Some((head, _tail)) = get_bounds(&tree)? {
        let new_head = head - 1;
        tree.insert(seq_to_key(new_head), value.as_bytes())?;
        tree.flush()?;
        tree.len()
    } else {
        tree.insert(seq_to_key(0), value.as_bytes())?;
        tree.flush()?;
        1
    };
    Ok(len.to_string())
}

/// RPUSH key value → 返回当前长度
pub fn rpush(db: &Db, key: &str, value: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let len = if let Some((_head, tail)) = get_bounds(&tree)? {
        let new_tail = tail + 1;
        tree.insert(seq_to_key(new_tail), value.as_bytes())?;
        tree.flush()?;
        tree.len()
    } else {
        tree.insert(seq_to_key(0), value.as_bytes())?;
        tree.flush()?;
        1
    };
    Ok(len.to_string())
}

/// LPOP key → 返回弹出元素或 "nil"
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

/// RPOP key → 返回弹出元素或 "nil"
pub fn rpop(db: &Db, key: &str) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    if let Some((_h, tail)) = get_bounds(&tree)? {
        let k = seq_to_key(tail);
        if let Some(iv) = tree.remove(&k)? {
            tree.flush()?;
            return Ok(str::from_utf8(&iv)?.to_string());
        }
    }
    Ok("nil".into())
}

/// LRANGE key start stop → 返回逗号分隔元素列表
pub fn lrange(db: &Db, key: &str, start: isize, stop: isize) -> Result<String> {
    let tree = db.open_tree(format!("{}{}", PREFIX, key))?;
    let (head, tail) = match get_bounds(&tree)? {
        Some(b) => b,
        None => return Ok(String::new()),
    };
    // total 元素数
    let total = (tail - head + 1) as isize;
    // normalize start/stop
    let s = if start < 0 { total + start } else { start };
    let e = if stop  < 0 { total + stop  } else { stop };
    let s = s.max(0).min(total-1) as i64;
    let e = e.max(0).min(total-1) as i64;
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

    #[test]
    fn test_list_basic() -> Result<()> {
        let tmp = tempdir()?;
        env::set_current_dir(&tmp)?;
        let db = sled::open("ldb")?;

        // LPUSH / RPUSH
        assert_eq!(lpush(&db, "L", "a")?, "1");
        assert_eq!(lpush(&db, "L", "b")?, "2"); // b, a
        assert_eq!(rpush(&db, "L", "c")?, "3"); // b,a,c

        // LRANGE
        assert_eq!(lrange(&db, "L", 0, 2)?, "b,a,c");
        assert_eq!(lrange(&db, "L", 1, 1)?, "a");

        // LPOP, RPOP
        assert_eq!(lpop(&db, "L")?, "b");
        assert_eq!(rpop(&db, "L")?, "c");
        assert_eq!(lrange(&db, "L", 0, -1)?, "a");

        // Pop all
        assert_eq!(lpop(&db, "L")?, "a");
        assert_eq!(lpop(&db, "L")?, "nil");
        assert_eq!(rpop(&db, "L")?, "nil");
        Ok(())
    }
}