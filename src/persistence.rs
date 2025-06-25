// src/persistence.rs

use anyhow::Result;
use sled::Db;
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread, time::Duration,
};
use crate::{config::Config, engine};

/// 持久化器：AOF 日志 + RDB 快照
pub struct Persistence {
    cfg:     Config,
    db:      Db,
    aof_path: PathBuf,
    rdb_path: PathBuf,
    aof_writer: Option<Arc<Mutex<File>>>,
    write_count: AtomicU64,
}

impl Persistence {
    /// 新 API：指定 AOF/RDB 文件路径
    pub fn new_with_paths(
        cfg: Config,
        db: Db,
        aof_path: PathBuf,
        rdb_path: PathBuf,
    ) -> Result<Arc<Self>> {
        // 打开或创建 AOF
        let aof_writer = if cfg.aof {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&aof_path)?;
            Some(Arc::new(Mutex::new(f)))
        } else {
            None
        };

        let pers = Arc::new(Self {
            cfg: cfg.clone(),
            db: db.clone(),
            aof_path,
            rdb_path: rdb_path.clone(),
            aof_writer,
            write_count: AtomicU64::new(0),
        });

        // RDB 快照线程
        if cfg.rdb {
            let p = pers.clone();
            thread::spawn(move || {
                let interval = Duration::from_secs(cfg.snapshot_interval_secs);
                loop {
                    thread::sleep(interval);
                    if let Err(e) = p.do_snapshot() {
                        eprintln!("RDB snapshot failed: {}", e);
                    }
                }
            });
        }

        Ok(pers)
    }

    /// 启动时重放 AOF
    pub fn load_aof(&self) -> Result<()> {
        if self.aof_writer.is_some() && self.aof_path.exists() {
            let f = File::open(&self.aof_path)?;
            let reader = BufReader::new(f);
            for line in reader.lines() {
                let line = line?;
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.is_empty() { continue; }
                // split_whitespace 并收集为 Vec<String>
                let parts: Vec<String> =
                    line.split_whitespace().map(|s| s.to_string()).collect();
                if parts.is_empty() {
                    continue;
                }
                // 调用 engine 执行业务命令（包括 SET/DEL/EXPIRE/..）
                let _ = engine::execute_non_txn_command(&parts[0].to_uppercase(), &parts, &self.db);
            }
            self.db.flush()?;
        }
        Ok(())
    }

    /// 写命令后追加 AOF 并触发 RDB
    pub fn append_aof_and_maybe_snapshot(&self, raw: &str, _db: &Db) {
        if let Some(w) = &self.aof_writer {
            let mut f = w.lock().unwrap();
            let _ = writeln!(f, "{}", raw);
        }
        if self.cfg.rdb {
            let prev = self.write_count.fetch_add(1, Ordering::SeqCst);
            if prev + 1 >= self.cfg.snapshot_threshold {
                self.write_count.store(0, Ordering::SeqCst);
                if let Err(e) = self.do_snapshot() {
                    eprintln!("RDB snapshot failed: {}", e);
                }
            }
        }
    }

    /// 执行一次全量 RDB 快照
    fn do_snapshot(&self) -> Result<()> {
        // 确保 sled 数据落盘
        self.db.flush()?;

        // 写入临时文件
        let tmp = self.rdb_path.with_extension("tmp");
        let mut f = File::create(&tmp)?;
        for item in self.db.iter() {
            let (k, v) = item?;
            writeln!(
                f,
                "{} {} {} {}",
                k.len(),
                v.len(),
                hex::encode(&k),
                hex::encode(&v)
            )?;
        }
        f.sync_all()?;

        // 原子替换
        std::fs::rename(tmp, &self.rdb_path)?;
        Ok(())
    }

    /// 优雅关闭时调用，强制 fsync AOF
    pub fn fsync_and_close(&self) {
        if let Some(w) = &self.aof_writer {
            if let Ok(f) = w.lock() {
                let _ = f.sync_all();
            }
        }
    }
}