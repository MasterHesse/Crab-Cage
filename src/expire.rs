use sled::{Db, Tree};
use anyhow::{Result};
use std::time::{SystemTime, UNIX_EPOCH};
use std::result::Result::Ok;
use tokio::time::{interval, Duration};

const EXPIRE_TREE: &str = "expire";

/// 返回当前的 UNIX 毫秒
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// 获取 或 创建 expire Tree
fn expire_tree(db: &Db) -> Result<Tree> {
    Ok(db.open_tree(EXPIRE_TREE)?)
}

/// 设置 key 的过期时间
pub fn expire(db: &Db, key: &str, secs: u64) -> Result<String> {
    let tree = expire_tree(db)?;
    let ts = now_ms().saturating_add(secs * 1000);
    tree.insert(key.as_bytes(), &ts.to_be_bytes())?;
    tree.flush()?;
    Ok("1".into())
}

/// 查询 key TTL （返回剩余时间，key 不存在 或 无 expire 返回 -1）
pub fn ttl(db: &Db, key: &str) -> Result<String> {
    let tree = expire_tree(db)?;
    if let Some(ivec) = tree.get(key.as_bytes())? {
        let mut b = [0u8; 8];
        b.copy_from_slice(&ivec);
        let exp_ts = u64::from_be_bytes(b);
        let now = now_ms();
        if exp_ts <= now {
            // 惰性清理
            remove_key(db, key)?;
            return Ok("-2".into());
        }
        let secs_left = ((exp_ts - now) + 999) / 1000;
        Ok(format!("{}", secs_left))
    } else {
        Ok("-1".into())
    }
}

/// 移除 key 的过期属性
pub fn persist(db: &Db, key: &str) -> Result<String> {
    let tree = expire_tree(db)?;
    let prev = tree.remove(key.as_bytes())?;
    if prev.is_some() {
        tree.flush()?;
        Ok("1".into())
    } else {
        Ok("0".into())
    }
}

/// 检查 key 是否过期，是则删除所有相关记录
pub fn remove_if_expired(db: &Db, key: &str) -> Result<()> {
    let tree = expire_tree(db)?;
    if let Some(ivec) = tree.get(key.as_bytes())? {
        let mut b = [0u8;8];
        b.copy_from_slice(&ivec);
        let exp_ts = u64::from_be_bytes(b);
        if exp_ts <= now_ms() {
            remove_key(db, key)?;
        }
    }
    Ok(())
}

/// 删除主 data tree 和 各类型 子 Tree 和 expire Tree
fn remove_key(db: &Db, key: &str) -> Result<()> {
    // 1. 从主 Data tree 删除
    let _ = db.remove(key.as_bytes())?;
    // 2. 从 hash，list，set 等拓展数据类型删除
    let _ = db.drop_tree(format!("hash:{}", key));
    let _ = db.drop_tree(format!("list:{}", key));
    let _ = db.drop_tree(format!("set:{}", key));
    // 3. 删除过期 entry
    let tree = expire_tree(db)?;
    let _ = tree.remove(key.as_bytes())?;
    tree.flush()?;
    db.flush()?;
    
    Ok(())
}
/// 后台定时清理任务
pub async fn start_cleaner(db: Db, interval_secs: u64) {
    let mut iv = interval(Duration::from_secs(interval_secs));
    loop {
        iv.tick().await;
        match db.open_tree(EXPIRE_TREE) {
            Ok(tree) => {
                let now_bytes = now_ms().to_be_bytes();
                // iterate over all keys <= now
                for entry in tree.range(..=now_bytes) {
                    if let Ok((k, _)) = entry {
                        if let Ok(key_str) = std::str::from_utf8(&k) {
                            // delete expired key
                            let _ = remove_key(&db, key_str);
                        }
                    }
                }
            }
            Err(_e) => {
                
            }
        }
    }
}

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
        assert!(db.get("k")?.is_none());

        Ok(())
    }
}