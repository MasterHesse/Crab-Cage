// src/expire.rs

use anyhow::{Context, Result};
use crate::engine::KvEngine;
use std::time::{SystemTime, UNIX_EPOCH};
use std::result::Result::Ok;
// use tokio::time::{interval, Duration};

/// 所有过期元数据都存到默认 tree 下的 key = "expire:{user_key}"
const EXPIRE_PREFIX: &str = "expire:";

/// 返回当前的 UNIX 毫秒
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 设置 key 的过期时间
pub fn expire<E:KvEngine>(db: &E, key: &str, secs: u64) -> Result<String> {
    let ts = now_ms().saturating_add(secs * 1_000);
    let meta = format!("{}{}", EXPIRE_PREFIX, key);
    let prev = db   
        .insert(meta.as_bytes(), &ts.to_be_bytes())
        .context("ERR write EXPIRE")?;
    Ok(if prev.is_none() {"1".into()} else {"0".into()})
}

/// 查询 key TTL （返回剩余时间，key 不存在 或 无 expire 返回 -1）
pub fn ttl<E: KvEngine>(db: &E, key: &str) -> Result<String> {
    let meta = format!("{}{}", EXPIRE_PREFIX, key);
    if let Some(bs) = db.get(meta.as_bytes()).context("ERR get TTL")? {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bs);
        let exp_ts = u64::from_be_bytes(buf);
        let now = now_ms();
        if exp_ts <= now {
            remove_key(db, key)?;
            return Ok("-2".into());
        }
        let left = ((exp_ts - now) + 999) / 1000;
        Ok(left.to_string())
    } else {
        Ok("-1".into())
    }
}

/// 移除 key 的过期属性
pub fn persist<E:KvEngine>(db: &E, key: &str) -> Result<String> {
    let meta = format!("{}{}", EXPIRE_PREFIX, key);
    let prev = db
        .remove(meta.as_bytes())
        .context("ERR PERSIST")?;
    Ok(if prev.is_some() {"1".into()} else {"0".into()})
}

/// 检查 key 是否过期，是则删除所有相关记录
pub fn remove_if_expired<E: KvEngine>(db: &E, key: &str) -> Result<()> {
    let meta = format!("{}{}", EXPIRE_PREFIX, key);
    if let Some(bs) = db.get(meta.as_bytes()).context("ERR get EXPIRE")? {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bs);
        if u64::from_be_bytes(buf) <= now_ms() {
            remove_key(db.as_db().expect("ERR remove EXPIRE"), key)?;
        }
    }
    Ok(())
}

/// 删除主 data tree 和 各类型 子 Tree 和 expire Tree
pub fn remove_key<E: KvEngine>(db: &E, key: &str) -> Result<()> {
    // 1) 默认 tree 下删主 key
    let main_key = key.as_bytes();
    let _ = db.remove(main_key).context("ERR remove main data")?;

    // 2) 删过期元数据
    let meta = format!("{}{}", EXPIRE_PREFIX, key);
    let _ = db.remove(meta.as_bytes()).context("ERR remove EXPIRE")?;

    // 3) 如果是 &Db，就能 drop_tree
    if let Some(plain) = db.as_db() {
        let _ = plain.drop_tree(format!("hash:{}", key));
        let _ = plain.drop_tree(format!("list:{}", key));
        let _ = plain.drop_tree(format!("set:{}", key));
        let _ = plain.drop_tree(format!("string:{}",key));
    }
    Ok(())
}
/// 后台定时清理任务
// pub async fn start_cleaner(db: sled::Db, interval_secs: u64) {
//     let mut iv = interval(Duration::from_secs(interval_secs));
//     loop {
//         iv.tick().await;
//         let now = now_ms().to_be_bytes();

//         // scan_prefix 只遍历默认 tree 下所有 "expire:" 开头的 entry
//         for entry in db.scan_prefix(EXPIRE_PREFIX.as_bytes()) {
//             if let Ok((k, v)) = entry {
//                 // k = b"expire:thekey"
//                 if v <= (&now).into() {
//                     if let Ok(kstr) = std::str::from_utf8(&k[EXPIRE_PREFIX.len()..]) {
//                         let _ = remove_key(&db, kstr);
//                     }
//                 }
//             }
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_expire_and_ttl() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        std::env::set_current_dir(&tmp)?;
        let db = sled::open("db")?;

        // SET + EXPIRE
        db.insert(b"k", b"v")?;
        assert_eq!(expire(&db, "k", 1)?, "1");
        // 立即 TTL 应接近 1
        let t1 = ttl(&db, "k")?.parse::<i64>()?;
        assert!(t1 <= 1 && t1 >= 0);
        // 睡眠 1.2s
        sleep(std::time::Duration::from_millis(1200));
        // TTL 返回 -2，且 key 被删除
        assert_eq!(ttl(&db, "k")?, "-2");
        assert!(db.get(b"k")?.is_none());

        Ok(())
    }
}