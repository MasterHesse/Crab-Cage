// tests/integration_persistence.rs

//! 集成测试：验证 AOF 持久化与重放，并验证 EXPIRE 生效
//! 流程：
//! 1. 在临时目录启动 sled + Persistence（只开 AOF）
//! 2. 执行若干 SET/DEL/EXPIRE，追加到 AOF
//! 3. 调 fsync_and_close() 强制写盘，db.flush() 确保 sled 落盘
//! 4. “重启”：新开 sled + Persistence，再 load_aof 重放
//! 5. 验证重放结果 & 过期效果

use std::{env, path::PathBuf, thread, time::Duration};
use tempfile::tempdir;
use sled::Db;
use anyhow::Result;
use std::time::Instant;

use rudis::{config::Config, persistence::Persistence, engine};

#[test]
fn test_aof_persistence_and_replay_with_expire() -> Result<()> {
    // 1) 准备临时目录并切换
    let tmp = tempdir()?;
    env::set_current_dir(tmp.path())?;

    // 2) sled 实例
    let db1: Db = sled::open("db1")?;

    // 3) 只启 AOF，不启 RDB
    let cfg = Config {
        aof: true,
        rdb: false,
        snapshot_interval_secs: 0,
        snapshot_threshold: 0,
    };

    // 4) Persistence：指定 AOF/RDB 路径
    let aof_path = tmp.path().join("appendonly.aof");
    let rdb_path = tmp.path().join("dump.rdb");
    let pers1 = Persistence::new_with_paths(
        cfg.clone(),
        db1.clone(),
        aof_path.clone(),
        rdb_path.clone(),
    )?;


    // 5) 执行命令并附加到 AOF，包括 EXPIRE
    let cmds = vec![
        vec!["SET",    "k1", "v1"],
        vec!["SET",    "k2", "v2"],
        vec!["DEL",    "k1"],
        vec!["SET",    "k3", "v3"],
        vec!["EXPIRE", "k3", "1"], // 1 秒后过期
    ];

    for parts in &cmds {
        let parts_str: Vec<String> = parts.iter().map(|s| s.to_string()).collect();

        // a) 真正执行业务
        let resp = engine::execute(parts_str.clone(), &db1);

        // b) 根据命令类型断言不同的返回值
        match parts[0] {
            "EXPIRE" => assert_eq!(
                resp, "1",
                "命令 {:?} 应返回 \"1\" 表示设置成功",
                parts
            ),
            _ => assert_eq!(
                resp, "OK",
                "命令 {:?} 应返回 \"OK\"",
                parts
            ),
        }

        // c) 再追加到 AOF
        pers1.append_aof_and_maybe_snapshot(&parts.join(" "), &db1);
    }

    // 6) 强制写盘
    pers1.fsync_and_close();
    db1.flush()?;

    // 7) “重启”：新开 sled + Persistence
    let db2: Db = sled::open("db2")?;
    let pers2 = Persistence::new_with_paths(
        cfg,
        db2.clone(),
        aof_path,
        rdb_path,
    )?;

    // 8) 重放 AOF
    pers2.load_aof()?;

    // 9) 验证重放后数据正确性
    // k1 已被删除
    assert!(db2.get("k1")?.is_none(), "k1 应被删除");
    // k2 值 v2
    let k2_val = db2.get("k2")?.unwrap();
    let v2 = std::str::from_utf8(&k2_val)?;
    assert_eq!(v2, "v2");

    // 10) 验证过期策略：重放后 k3 立即存在，随后在 2 秒内一定会过期
    // 10a) 立刻确认 k3 还在
    assert_eq!(
        engine::execute(vec!["GET".into(), "k3".into()], &db2),
        "v3",
        "重放后 k3 应立即可读"
    );

    // 10b) 在一个最大 2 s 的窗口内，轮询检测是否过期
    let timeout = Duration::from_secs(2);
    let start = Instant::now();
    let mut expired = false;
    while start.elapsed() < timeout {
        let got = engine::execute(vec!["GET".into(), "k3".into()], &db2);
        if got == "ERR key not found" {
            expired = true;
            break;
        }
        // 小憩一下再测
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        expired,
        "k3 在重放后应该在 2 s 内过期，但一直没看到过期"
    );

    Ok(())
}