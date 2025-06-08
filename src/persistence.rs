use anyhow::Result;
use sled::Db;
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use crate::config::Config;

/// 持久化模块：负责 AOF 日志 和 RDB 快照
pub struct Persistence {
    cfg: Config,
    // AOF 文件的写入器（带缓冲），进程退出时要 fsync
    aof_writer: Option<Arc<std::sync::Mutex<File>>>, 
    // 已执行的写命令数，用于触发 RDB
    write_count: AtomicU64, 
}

impl Persistence {
    /// 构造：打开 AOF (append) ，并根据 config 启动快照线程
    pub fn new(cfg: Config, db:Db) -> Result<Arc<Self>> {
        // 1) 如果开启 AOF ，就打开或新建 appendonly.aof
        let aof_writer = if cfg.aof {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open("appendonly.aof")?;
            Some(Arc::new(std::sync::Mutex::new(f)))
        } else {
            None
        };

        let pers = Arc::new(Self {
            cfg: cfg.clone(),
            aof_writer,
            write_count: AtomicU64::new(0),
        });

        // 2) 如果开启 RDB ，启动 RDB 快照线程
        if cfg.rdb {
            let pers_cloned = pers.clone();
            // 使用本身并发安全的 sled auto-flush
            thread::spawn(move || {
                let interval = Duration::from_secs(cfg.snapshot_interval_secs);
                loop {
                    thread::sleep(interval);
                    if let Err(e) = pers_cloned.do_snapshot(&db) {
                        eprintln!("RDB snapshot failed: {}", e);
                    }
                }
            });
        }

        Ok(pers)
    }

    /// 启动时：读取并重放 AOF
    pub fn load_aof(&self, db: &Db) -> Result<()> {
        if let Some(_) = &self.aof_writer {
            if Path::new("appendonly.aof").exists() {
                let f = File::open("appendonly.aof")?;
                let reader = BufReader::new(f);
                for line in reader.lines() {
                    let line = line?;
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.is_empty() {continue;}
                    // 简单只支持 SET k v / DEL k
                    match parts[0].to_uppercase().as_str() {
                        "SET" if parts.len() == 3 => {db.insert(parts[1], parts[2].as_bytes())?;}
                        "DEL" if parts.len() == 2 => {db.remove(parts[1])?;}
                        _ => { /* 忽略不支持的语句 */ }
                    }
                }
                // sled写操作完成，显式 flush 持久化到本地
                db.flush()?;
            }
        }
        Ok(())
    }

    /// 每次写命令执行后调用：追加一行到 AOF ，并根据阈值触发一次快照
    pub fn append_aof_and_maybe_snapshot(&self, raw_cmd: &str, db:&Db) {
        // AOF
        if let Some(w) = &self.aof_writer {
            let mut guard = w.lock().unwrap();
            let _ = writeln!(guard, "{}", raw_cmd);
        }

        // RDB by count
        if self.cfg.rdb {
            let prev = self.write_count.fetch_add(1, Ordering::SeqCst);
            if prev + 1 >= self.cfg.snapshot_threshold {
                //  重置计数
                self.write_count.store(0, Ordering::SeqCst);
                // 立即快照
                if let Err(e) = self.do_snapshot(db) {
                    eprintln!("RDB snapshot failed: {}", e);
                }
            }
        }
    }

    /// 真正执行一次全量 RDB 快照，遍历sled，把 KV 序列化到临时文件，再原子命名
    fn do_snapshot(&self, db:&Db) -> Result<()> {
        // 1) 先把 sled 数据写入磁盘
        db.flush()?;

        // 2) 打开临时文件
        let tmp = "dump.rdb.tmp";
        let mut f = File::create(tmp)?;

        // 3) 迭代 sled 所有 KV
        for item in db.iter() {
           let (k,v) = item?;
            // 格式：<key_len> <value_len> <key_bytes> <value_bytes>\n
            writeln!(f, "{} {} {} {}",
                k.len(),
                v.len(),
                hex::encode(&k),
                hex::encode(&v),
            )?;
        }
        f.sync_all()?; // 确保写入磁盘

        // 4) 原子替换
        std::fs::rename(tmp, "dump.rdb")?;
        Ok(())
    }

    /// 退出时 fsync AOF
    pub fn fsync_and_close(&self) {
        if let Some(w) = &self.aof_writer {
            if let Ok(mut guard) = w.lock() {
                let _ = guard.sync_all();
            }
        }
    }  
}