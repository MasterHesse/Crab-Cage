// tests/persistence_integration.rs
#![allow(warnings)]  // 禁用所有警告

use tempfile::tempdir;
use std::env;
use sled::Db;
use anyhow::Result;

// 假设你在 Cargo.toml 里声明的包名是 kvdb
use rudis::{config::Config, persistence::Persistence, engine};


#[tokio::test]
async fn test_aof_persistence_and_replay() -> Result<()> {
    // 1. 临时目录
    let tmp = tempdir()?;
    env::set_current_dir(&tmp)?;

    // 2. 第一次打开 sled
    let db1: Db = sled::open("db1")?;
    let cfg = Config { aof: true, rdb: false, snapshot_interval_secs:0, snapshot_threshold:0 };
    let pers1 = Persistence::new(cfg.clone(), db1.clone())?;

    // 3. 模拟写命令
    let cmds = vec![
        vec!["SET","k1","v1"],
        vec!["SET","k2","v2"],
        vec!["DEL","k1"],
    ];
    for parts in cmds {
        let parts_str: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
        let resp = engine::execute(parts_str.clone(), &db1);
        assert_eq!(resp, "OK");
        pers1.append_aof_and_maybe_snapshot(&parts.join(" "), &db1);
    }

    // 4. fsync + flush
    pers1.fsync_and_close();
    db1.flush()?;

    // 5. “重启”：新开 sled + persistence，重放 AOF
    let db2: Db = sled::open("db2")?;
    let pers2 = Persistence::new(cfg, db2.clone())?;
    pers2.load_aof(&db2)?;

    // 6. 验证
    assert!(db2.get("k1")?.is_none());
    let k2_value = db2.get("k2")?.unwrap();
    let v2 = std::str::from_utf8(&k2_value)?;
    assert_eq!(v2, "v2");
    Ok(())
}