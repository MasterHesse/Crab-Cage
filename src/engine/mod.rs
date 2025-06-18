//! 命令调度与执行层
//! --------------------------------------------------
//! 1. `execute_generic`：对 String / INCR / DECR 等单-Tree
//!    操作做统一实现，既能直接跑在 sled::Db，也能跑在
//!    TransactionalTree（事务闭包里）。
//!
//! 2. `execute`：普通请求入口。先做惰性过期删除，再尝试
//!    execute_generic，失败则回退到各数据结构模块。
//!
//! 3. `execute_txn`：事务闭包里的执行，只走 generic；
//!    Hash / List / Set 跨多个 Tree，在 sled 当前实现下
//!    无法放进事务，这里选择直接报错。
//!
//! 4. `execute_with_ctx`：对外暴露的入口，带有 TxnContext，
//!    负责 MULTI / DISCARD / EXEC 的状态机。

mod kv;
use kv::KVEngine;

use sled::{transaction::TransactionalTree, Db};
use std::str;

use crate::{expire, txn::TxnContext};
use crate::types::{hash, list, set, string};

//
// -------------------------------------------------- 通用实现
//

/// execute_generic 只处理 String、INCR/DECR 等“单 Tree”
/// 原子操作，以便在事务里也能复用。
pub fn execute_generic<E>(parts: &[String], engine: &E) -> Result<String, String>
where
    E: KVEngine,
    String: From<E::Err>,
{
    if parts.is_empty() {
        return Err("empty command".into());
    }
    let cmd = parts[0].to_uppercase();

    match cmd.as_str() {
        // ------------------------------- String 基础
        "SET" if parts.len() == 3 => {
            engine.insert(parts[1].as_bytes(), parts[2].as_bytes())?;
            Ok("OK".into())
        }
        "GET" if parts.len() == 2 => match engine.get(parts[1].as_bytes())? {
            Some(iv) => Ok(String::from_utf8(iv.to_vec()).map_err(|e| e.to_string())?),
            None => Ok("nil".into()),
        },
        "DEL" if parts.len() == 2 => {
            let existed = engine.remove(parts[1].as_bytes())?.is_some();
            Ok(if existed { "1" } else { "0" }.into())
        }

        // ------------------------------- INCR / DECR
        "INCR" | "DECR" if parts.len() == 2 => {
            let key = parts[1].as_str();
            let delta: i64 = if cmd == "INCR" { 1 } else { -1 };

            let new_val = loop {
                // 1) 读旧值
                let old_opt = engine.get(key.as_bytes())?;
                let old_num = if let Some(ref iv) = old_opt {
                    let s = str::from_utf8(iv).map_err(|_| "not integer")?;
                    s.parse::<i64>().map_err(|_| "not integer")?
                } else {
                    0
                };

                // 2) 计算
                let new_num = old_num.checked_add(delta).ok_or("overflow")?;
                let new_bytes = new_num.to_string().into_bytes();

                // 3) CAS
                let expect = old_opt.as_ref().map(|iv| iv.as_ref());
                match engine.cas(key.as_bytes(), expect, Some(&new_bytes)) {
                    Ok(_) => break new_num.to_string(),
                    Err(e) if format!("{:?}", e) == "cas mismatch" => continue,
                    Err(e) => return Err(e.into()),
                }
            };

            Ok(new_val)
        }

        _ => Err(format!("unknown or wrong args for '{}'", cmd)),
    }
}

//
// -------------------------------------------------- 普通执行路径
//

/// 工具宏：检查参数个数
macro_rules! argc {
    ($parts:expr, $n:expr, $name:expr) => {
        if $parts.len() != $n {
            return format!("ERR wrong number of arguments for '{}'", $name);
        }
    };
}

/// 把 anyhow/StringError 等结果收敛成 String
fn wrap<R, E: std::fmt::Debug>(r: Result<R, E>) -> String
where
    R: Into<String>,
{
    match r {
        Ok(v) => v.into(),
        Err(e) => format!("ERR {:?}", e),
    }
}

/// 外部调用的普通入口
pub fn execute(parts: Vec<String>, db: &Db) -> String {
    // 0) 懒删除过期键
    if parts.len() > 1 {
        let upper = parts[0].to_uppercase();
        if !matches!(upper.as_str(), "PING" | "QUIT") {
            let _ = expire::remove_if_expired(db, &parts[1]);
        }
    }

    // 1) 先试走通用逻辑
    if let Ok(r) = execute_generic(&parts, db) {
        return r;
    }

    // 2) 回退到各类型模块
    let cmd = parts[0].to_uppercase();
    match cmd.as_str() {
        // --------------------------- String
        "SET"  => { argc!(parts, 3, "SET");  wrap(string::set (db,&parts[1],&parts[2])) }
        "GET"  => { argc!(parts, 2, "GET");  wrap(string::get (db,&parts[1]).map(|o| o.unwrap_or_else(||"nil".into()))) }
        "DEL"  => { argc!(parts, 2, "DEL");  wrap(string::del (db,&parts[1]).map(|b| if b{"1".to_string()}else{"0".to_string()})) }

        // --------------------------- Hash
        "HSET"    => { argc!(parts, 4,"HSET");    wrap(hash::hset   (db,&parts[1],&parts[2],&parts[3])) }
        "HGET"    => { argc!(parts, 3,"HGET");    wrap(hash::hget   (db,&parts[1],&parts[2])) }
        "HDEL"    => { argc!(parts, 3,"HDEL");    wrap(hash::hdel   (db,&parts[1],&parts[2])) }
        "HKEYS"   => { argc!(parts, 2,"HKEYS");   wrap(hash::hkeys  (db,&parts[1])) }
        "HVALS"   => { argc!(parts, 2,"HVALS");   wrap(hash::hvals  (db,&parts[1])) }
        "HGETALL" => { argc!(parts, 2,"HGETALL"); wrap(hash::hgetall(db,&parts[1])) }

        // --------------------------- List
        "LPUSH" => { argc!(parts, 3,"LPUSH"); wrap(list::lpush (db,&parts[1],&parts[2])) }
        "RPUSH" => { argc!(parts, 3,"RPUSH"); wrap(list::rpush (db,&parts[1],&parts[2])) }
        "LPOP"  => { argc!(parts, 2,"LPOP");  wrap(list::lpop  (db,&parts[1])) }
        "RPOP"  => { argc!(parts, 2,"RPOP");  wrap(list::rpop  (db,&parts[1])) }
        "LRANGE"=> {
            argc!(parts, 4,"LRANGE");
            let s: isize = parts[2].parse().unwrap_or(0);
            let e: isize = parts[3].parse().unwrap_or(0);
            wrap(list::lrange(db,&parts[1],s,e))
        }

        // --------------------------- Set
        "SADD"      => { argc!(parts, 3,"SADD");      wrap(set::sadd     (db,&parts[1],&parts[2])) }
        "SREM"      => { argc!(parts, 3,"SREM");      wrap(set::srem     (db,&parts[1],&parts[2])) }
        "SMEMBERS"  => { argc!(parts, 2,"SMEMBERS");  wrap(set::smembers (db,&parts[1])) }
        "SISMEMBER" => { argc!(parts, 3,"SISMEMBER"); wrap(set::sismember(db,&parts[1],&parts[2])) }

        // --------------------------- 其它
        "PING" => "PONG".into(),
        "QUIT" => "OK".into(),

        other => format!("ERR unknown command '{other}'"),
    }
}

//
// -------------------------------------------------- 事务相关
//

/// 事务内单条命令执行：只能走 execute_generic
pub fn execute_txn(parts: Vec<String>, tree: &TransactionalTree) -> Result<String, String> {
    execute_generic(&parts, tree)
}

/// MULTI / DISCARD / EXEC 处理入口
pub fn execute_with_ctx(parts: Vec<String>, db: &Db, ctx: &mut TxnContext) -> String {
    let cmd = parts.get(0).map(|s| s.to_uppercase()).unwrap_or_default();
    match cmd.as_str() {
        "MULTI"   => ctx.multi(),
        "DISCARD" => ctx.discard(),
        "EXEC"    => ctx.exec(db),
        _ if ctx.in_multi => ctx.queue_cmd(parts),
        _ => execute(parts, db),
    }
}