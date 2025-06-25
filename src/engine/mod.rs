// src/engine/mod.rs

//! # 引擎模块
//!
//! `engine` 模块是 Redis 类服务器的核心。它：
//! - 从网络层接收已解析和分词的命令（`Vec<String>`）。
//! - 与底层的 `sled::Db` 进行数据操作交互。
//! - 将业务逻辑委托给类型特定的子模块（`string`、`hash`、`list`、`set`）和 `expire` 模块执行。
//! - 返回一个响应 `String`，网络层将将其格式化为 RESP 简单字符串或错误。
pub mod kv;
pub use kv::KvEngine;

use crate::txn::session::TxnSession;
use crate::txn::executor::exec_all;
use crate::types::{hash, list, set, string};
use crate::expire;

/// 对指定数据库执行单个客户端命令（新增 txn_session 参数）
///
/// # 参数
///
/// * `parts` - 包含命令名称及其参数的 `Vec<String>`
/// * `db` - 打开的 `sled::Db` 实例的引用
/// * `txn_session` - 事务会话状态
pub fn execute<E>(parts: Vec<String>, db: &E, txn_session: &mut TxnSession) -> String 
where 
    E: KvEngine,
{
    // 1. 空白命令检查
    if parts.is_empty() {
        return "ERR empty command".to_string();
    }

    let cmd = parts[0].to_uppercase();
    let parts = parts.clone();

    // 2. 仅在非事务模式且不是事务命令时执行过期检查
    if !txn_session.in_multi && parts.len() > 1 {
        match cmd.as_str() {
            "PING" | "QUIT" => {}
            _ => {
                if let Some(_db) = db.as_db() {
                    let _ = expire::remove_if_expired(_db, &parts[1]);    
                }
            }
        }
    }

    // 3. 处理事务命令
    match cmd.as_str() {
        // --- 事务命令 ---
        "MULTI" => {
            txn_session.begin().map(|s| s.to_string()).unwrap_or_else(|e| e.to_string())
        }
        "EXEC" => {
            match txn_session.take_queue() {
                Ok(queue) => {
                    if let Some(sled_db) = db.as_db() {
                        let results = exec_all(sled_db, &queue);
                        results.join("\n")
                    } else {
                        "ERR transaction not supported".to_string()
                    }
                }
                Err(e) => e.to_string(),
            }
        }
        "DISCARD" => {
            txn_session.discard().map(|s| s.to_string()).unwrap_or_else(|e| e.to_string())
        }
        
        // --- 其他命令 ---
        _ => {
            if txn_session.in_multi {
                // 事务模式下将命令加入队列
                match txn_session.enqueue(parts) {
                    Ok(resp) => resp.to_string(),
                    Err(_) => "ERR not in transaction".to_string(),
                }
            } else {
                // 非事务模式直接执行命令
                execute_non_txn_command(&cmd, &parts, db)
            }
        }
    }
}

/// 执行非事务命令（原命令分发逻辑）
pub fn execute_non_txn_command<E>(cmd: &str, parts: &[String], db: &E) -> String 
where 
    E: KvEngine,
{
    match cmd {
        // --- String commands ---
        "SET" => {
            if parts.len() != 3 {
                "ERR wrong number of arguments for 'SET'".into()
            } else {
                match string::set(db, &parts[1], &parts[2]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        },
        "GET" => {
            if parts.len() != 2 {
                "ERR wrong number of arguments for 'GET'".into()
            } else {
                match string::get(db, &parts[1]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        },
        "DEL" => {
            if parts.len() != 2 {
                "ERR wrong number of arguments for 'DEL'".into()
            } else {
                match string::del(db, &parts[1]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        },

        // 原子增减操作
        "INCR" => {
            if parts.len() != 2 {
                "ERR wrong number of arguments for 'INCR'".into()
            } else {
                match string::incr(db, &parts[1]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        }
        "DECR" => {
            if parts.len() != 2 {
                "ERR wrong number of arguments for 'DECR'".into()
            } else {
                match string::decr(db, &parts[1]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        }


        // --- Hash commands ---
        "HSET" => {
            if parts.len() != 4 {
                "ERR wrong number of arguments for 'HSET'".into()
            } else {
                match hash::hset(db, &parts[1], &parts[2], &parts[3]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        }
        "HGET" => {
            if parts.len() != 3 {
                "ERR wrong number of arguments for 'HGET'".into()
            } else {
                match hash::hget(db, &parts[1], &parts[2]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        }
        "HDEL" => {
            if parts.len() != 3 {
                "ERR wrong number of arguments for 'HDEL'".into()
            } else {
                match hash::hdel(db, &parts[1], &parts[2]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        }
        "HKEYS" => {
            if parts.len() != 2 {
                "ERR wrong number of arguments for 'HKEYS'".into()
            } else {
                match hash::hkeys(db, &parts[1]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        }
        "HVALS" => {
            if parts.len() != 2 {
                "ERR wrong number of arguments for 'HVALS'".into()
            } else {
                match hash::hvals(db, &parts[1]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        }
        "HGETALL" => {
            if parts.len() != 2 {
                "ERR wrong number of arguments for 'HGETALL'".into()
            } else {
                match hash::hgetall(db, &parts[1]) {
                    Ok(s) => s,
                    Err(e) => format!("ERR {}", e),
                }
            }
        }

        // --- List commands ---
        "LPUSH" => {
            if parts.len() != 3 { "ERR wrong number of arguments for 'LPUSH'".into() }
            else { match list::lpush(db, &parts[1], &parts[2]) { Ok(s)=>s, Err(e)=>format!("ERR {}", e) } }
        }
        "RPUSH" => {
            if parts.len() != 3 { "ERR wrong number of arguments for 'RPUSH'".into() }
            else { match list::rpush(db, &parts[1], &parts[2]) { Ok(s)=>s, Err(e)=>format!("ERR {}", e) } }
        }
        "LPOP" => {
            if parts.len() != 2 { "ERR wrong number of arguments for 'LPOP'".into() }
            else {
                match list::lpop(db, &parts[1]) { Ok(s)=>s, Err(e)=>format!("ERR {}", e) }
            }
        }
        "RPOP" => {
            if parts.len() != 2 { "ERR wrong number of arguments for 'RPOP'".into() }
            else {
                match list::rpop(db, &parts[1]) { Ok(s)=>s, Err(e)=>format!("ERR {}", e) }
            }
        }
        "LRANGE" => {
            if parts.len() != 4 { "ERR wrong number of arguments for 'LRANGE'".into() }
            else {
                // Parse start and stop as signed integers
                let start = parts[2].parse::<isize>();
                let stop  = parts[3].parse::<isize>();
                match (start, stop) {
                    (Ok(s), Ok(e)) => match list::lrange(db, &parts[1], s, e) {
                        Ok(r) => r,
                        Err(er) => format!("ERR {}", er),
                    },
                    _ => "ERR invalid start or stop".into(),
                }
            }
        }

        // --- Set commands ---
        "SADD" => {
            if parts.len() != 3 { "ERR wrong number of arguments for 'SADD'".into() }
            else { match set::sadd(db, &parts[1], &parts[2]) { Ok(s)=>s, Err(e)=>format!("ERR {}", e) } }
        }
        "SREM" => {
            if parts.len() != 3 { "ERR wrong number of arguments for 'SREM'".into() }
            else { match set::srem(db, &parts[1], &parts[2]) { Ok(s)=>s, Err(e)=>format!("ERR {}", e) } }
        }
        "SMEMBERS" => {
            if parts.len() != 2 { "ERR wrong number of arguments for 'SMEMBERS'".into() }
            else { match set::smembers(db, &parts[1]) { Ok(s)=>s, Err(e)=>format!("ERR {}", e) } }
        }
        "SISMEMBER" => {
            if parts.len() != 3 { "ERR wrong number of arguments for 'SISMEMBER'".into() }
            else { match set::sismember(db, &parts[1], &parts[2]) { Ok(s)=>s, Err(e)=>format!("ERR {}", e) } }
        }

        // --- Expiration commands ---
        "EXPIRE" => {
            // EXPIRE <key> <seconds>: set a TTL on key
            if parts.len() != 3 {
                return "ERR wrong number of arguments for 'EXPIRE'".to_string();
            }
            let key = &parts[1];
            match parts[2].parse::<u64>() {
                Ok(secs) => match expire::expire(db, key, secs) {
                    Ok(v) => v,                  // "1" if TTL set, "0" if key does not exist
                    Err(e) => format!("ERR {}", e),
                },
                Err(_) => "ERR value is not an integer or out of range".to_string(),
            }
        }

        "TTL" => {
            // TTL <key>: get remaining TTL in seconds
            if parts.len() != 2 {
                return "ERR wrong number of arguments for 'TTL'".to_string();
            }
            match expire::ttl(db, &parts[1]) {
                Ok(v) => v,   // "-2", "-1", or remaining seconds
                Err(e) => format!("ERR {}", e),
            }
        }

        "PERSIST" => {
            // PERSIST <key>: remove existing TTL
            if parts.len() != 2 {
                return "ERR wrong number of arguments for 'PERSIST'".to_string();
            }
            match expire::persist(db, &parts[1]) {
                Ok(v) => v,   // "1" if TTL removed, "0" if key or TTL did not exist
                Err(e) => format!("ERR {}", e),
            }
        }

        // --- Connection / Control commands ---
        "PING" => {
            // PING: health check, always returns "PONG"
            "PONG".to_string()
        }
        "QUIT" => {
            // QUIT: client indicates intent to close connection.
            // Return "OK"; the server loop will handle terminating the session.
            "OK".to_string()
        }

        // --- Unknown command ---
        other => {
            format!("ERR unknown command '{}'", other)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sled::Config;
    use crate::txn::session::TxnSession;

    /// 创建一个临时的 sled::Db，用于测试
    fn make_db() -> sled::Db {
        Config::new()
            .temporary(true)
            .open()
            .expect("打开临时 sled db 失败")
    } 

    // 创建临时数据库和事务会话
    fn make_db_and_session() -> (sled::Db, TxnSession) {
        (make_db(), TxnSession::new())
    }

    // 新增事务测试
    #[test]
    fn test_transaction_commands() {
        let (db, mut session) = make_db_and_session();
        
        // 测试 MULTI
        assert_eq!(
            execute(vec!["MULTI"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
                ),
            "OK"
        );
        assert!(session.in_multi);
        
        // 测试命令入队
        assert_eq!(
            execute(vec!["SET", "tx_key", "tx_value"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
                ),
            "QUEUED"
        );
        assert_eq!(session.queue.len(), 1);
        
        // 测试 DISCARD
        assert_eq!(
            execute(vec!["DISCARD"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
                ),
            "OK"
        );
        assert!(!session.in_multi);
        assert!(session.queue.is_empty());
        
        // 测试 EXEC
        execute(vec!["MULTI"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
        );
        execute(vec!["SET", "tx_key", "tx_value"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
        );
        execute(vec!["EXEC"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
        );
        assert!(!session.in_multi);
        assert!(session.queue.is_empty());
        
        assert_eq!(
            execute(vec!["GET","tx_key"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
                ),
            "tx_value"
        );
        
        // 测试嵌套 MULTI
        execute(vec!["MULTI"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
        );
        assert_eq!(
            execute(vec!["MULTI"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
            ),
            "ERR MULTI calls can not be nested"
        );
        // 关闭事务
        assert_eq!(
            execute(vec!["DISCARD"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
                ),
            "OK"
        );
        
        // 测试 EXEC 无 MULTI
        assert_eq!(
            execute(vec!["EXEC"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
            ),
            "ERR EXEC without MULTI"
        );
        
        // 测试 DISCARD 无 MULTI
        assert_eq!(
            execute(vec!["DISCARD"].iter().map(|s| s.to_string()).collect(),
                    &db, 
                    &mut session
            ),
            "ERR DISCARD without MULTI"
        );
    }

    // 字符串命令测试
    #[test]
    fn test_string_commands() {
        let (db, mut session) = make_db_and_session();

        // SET 命令
        assert_eq!(
            execute(
                vec!["SET", "key1", "value1"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "OK"
        );
        // GET 命令
        assert_eq!(
            execute(
                vec!["GET", "key1"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "value1"
        );
        // GET 不存在的键
        assert_eq!(
            execute(
                vec!["GET", "nonexistence"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "ERR key not found"
        );
        // INCR 命令
        execute(
                vec!["SET", "counter", "10"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
        );
        assert_eq!(
            execute(
                vec!["INCR", "counter"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "11"
        );
        // DECR 命令
        assert_eq!(
            execute(
                vec!["DECR", "counter"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "10"
        );
        // DEL 命令
        assert_eq!(
            execute(
                vec!["DEL", "key1"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "OK"
        );
    }

    // 哈希命令测试
    #[test]
    fn test_hash_commands() {
        let (db, mut session) = make_db_and_session();

        execute(
                vec!["HSET", "user:1","name","Alice"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
        );
        
        // HGET 命令
        assert_eq!(
            execute(
                vec!["HGET", "user:1","name"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "Alice"
        );
        
        // HDEL 命令
        assert_eq!(
            execute(
                vec!["HDEL", "user:1","name"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "1"
        );
        
        // HKEYS 命令
        execute(
                vec!["HSET", "user:1", "email", "alice@example.com"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
        );
        execute(
                vec!["HSET", "user:1","email","alice@example.com"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
        );
        assert_eq!(
            execute(
                vec!["HKEYS", "user:1"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "email"
        );
    }

    // 列表命令测试
    #[test]
    fn test_list_commands() {
        let (db, mut session) = make_db_and_session();

        execute(
                vec!["LPUSH", "mylist", "item1"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
        );
        execute(
                vec!["RPUSH", "mylist", "item2"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
        );
        
        // LPOP 命令
        assert_eq!(
            execute(
                vec!["LPOP", "mylist"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "item1"
        );
        
        // LRANGE 命令
        assert_eq!(
            execute(
                vec!["LRANGE", "mylist", "0", "-1"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "item2"
        );
    }

    // 集合命令测试
    #[test]
    fn test_set_commands() {
        let (db, mut session) = make_db_and_session();
        execute(
                vec!["SADD", "myset", "member1"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
        );
        
        // SISMEMBER 命令
        assert_eq!(
            execute(
                vec!["SISMEMBER", "myset", "member1"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "1"
        );
        
        // SMEMBERS 命令
        assert_eq!(
            execute(
                vec!["SMEMBERS", "myset"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "member1"
        );
    }

    // 过期命令测试
    #[test]
    fn test_expire_commands() {
        let (db, mut session) = make_db_and_session();

        execute(
                vec!["SET", "temp_key", "value"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
        );
        
        // EXPIRE 命令
        assert_eq!(
            execute(
                vec!["EXPIRE", "temp_key", "60"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "1"
        );
        
        // TTL 命令
        let ttl = execute(
                vec!["TTL", "temp_key"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            );
        assert!(ttl.parse::<i64>().unwrap() > 0);
        
        // PERSIST 命令
        assert_eq!(
            execute(
                vec!["PERSIST", "temp_key"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "1"
        );
    }

    // 控制命令测试
    #[test]
    fn test_control_commands() {
        let (db, mut session) = make_db_and_session();
        assert_eq!(            execute(
                vec!["PING"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ), "PONG");
        assert_eq!(            execute(
                vec!["QUIT"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ), "OK");
    }

    // 错误参数测试
    #[test]
    fn test_argument_errors() {
        let (db, mut session) = make_db_and_session();

        // SET 参数不足
        assert_eq!(
            execute(
                vec!["SET", "Key"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "ERR wrong number of arguments for 'SET'"
        );
        
        // GET 多余参数
        assert_eq!(
            execute(
                vec!["GET", "key", "extra"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "ERR wrong number of arguments for 'GET'"
        );
        
        // INCR 多余参数
        assert_eq!(
            execute(
                vec!["INCR", "counter", "extra"].iter().map(|s| s.to_string()).collect(),
                &db,
                &mut session
            ),
            "ERR wrong number of arguments for 'INCR'"
        );
    }
}