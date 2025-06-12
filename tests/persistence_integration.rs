// tests/persistence_integration.rs

//! 集成测试：验证 AOF 持久化与重放
//! 流程：
//! 1. 在临时目录启动 sled + Persistence（只开 AOF）
//! 2. 执行若干 SET/DEL，追加到 AOF
//! 3. 调 fsync_and_close() 强制写盘，db.flush() 确保 sled 落盘
//! 4. “重启”：新开 sled + Persistence，再 load_aof 重放
//! 5. 验证重放结果

use tempfile::tempdir;
use std::env;
use sled::Db;
use anyhow::Result;

// 注意：这里的 kvdb 要与 Cargo.toml 中的 package.name 一致
use rudis::{config::Config, persistence::Persistence, engine};

#[tokio::test]
async fn test_aof_persistence_and_replay() -> Result<()> {
    // 1) 创建一个自动删除的临时目录，并切换到该目录
    let tmp = tempdir()?;
    env::set_current_dir(&tmp)?;

    // 2) 打开第一个 sled 实例（模拟运行时）
    let db1: Db = sled::open("db1")?;

    // 3) 构造只启用 AOF、不启用 RDB 的配置
    let cfg = Config {
        aof: true,
        rdb: false,
        snapshot_interval_secs: 0,
        snapshot_threshold: 0,
    };

    // 4) 构造持久化器
    let pers1 = Persistence::new(cfg.clone(), db1.clone())?;

    // 5) 模拟写命令：SET k1 v1、SET k2 v2、DEL k1
    let cmds = vec![
        vec!["SET", "k1", "v1"],
        vec!["SET", "k2", "v2"],
        vec!["DEL", "k1"],
    ];

    for parts in cmds.iter() {
        // a) 用 engine 真正写入 sled
        let parts_str: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
        let resp = engine::execute(parts_str.clone(), &db1);
        assert_eq!(resp, "OK");

        // b) 追加到 AOF
        pers1.append_aof_and_maybe_snapshot(&parts.join(" "), &db1);
    }

    // 6) 强制 fsync AOF，并 flush sled
    pers1.fsync_and_close();
    db1.flush()?;

    // 7) “重启”：新开 sled 实例 + Persistence
    let db2: Db = sled::open("db2")?;
    let pers2 = Persistence::new(cfg, db2.clone())?;

    // 8) 重放 AOF
    pers2.load_aof()?;

    // 9) 验证：k1 被删除，k2 的值为 "v2"
    assert!(db2.get("k1")?.is_none(), "k1 应被删除");
    let k2_value = db2.get("k2")?.unwrap(); // Store the Vec<u8> in a variable first
    let v2 = std::str::from_utf8(&k2_value)?; // Then create the string slice
    assert_eq!(v2, "v2");

    Ok(())
}