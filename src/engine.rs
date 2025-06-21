// src/engine.rs

//! # 引擎模块
//!
//! `engine` 模块是 Redis 类服务器的核心。它：
//! - 从网络层接收已解析和分词的命令（`Vec<String>`）。
//! - 与底层的 `sled::Db` 进行数据操作交互。
//! - 将业务逻辑委托给类型特定的子模块（`string`、`hash`、`list`、`set`）和 `expire` 模块执行。
//! - 返回一个响应 `String`，网络层将将其格式化为 RESP 简单字符串或错误。


use sled::Db;
use crate::types::{hash, list, set, string};
use crate::expire;


/// 对指定数据库执行单个客户端命令。
///
/// # 参数
///
/// * `parts` – 包含命令名称及其参数的 `Vec<String>`，已由网络层拆分。例如：`[“SET”, ‘key’, “value”]`。
/// * `db`    – 打开的 `sled::Db` 实例的引用。
///
/// # 返回值
///
/// 表示命令结果的 `String`：
/// - 成功操作：如 `“OK”`、`“PONG”` 或数值/字符串负载。
/// - 错误：以 `“ERR ”` 开头的消息，例如 `“ERR 参数数量错误”`。
///
/// # 行为
///
/// 1. **空命令检查**：若未提供令牌则返回错误。
/// 2. **命令名称不区分大小写**：将第一个令牌大写以实现命令的不区分大小写分发。
/// 3. **延迟过期**：对于以键作为第一个参数的命令（除 `PING` 和 `QUIT` 之外的所有命令），在执行前调用 `expire::remove_if_expired`，以透明地清除过期键。
/// 4. **命令分发**：根据大写命令名称匹配，并调用相应的子模块或过期函数。
/// 5. **参数验证**：检查参数的数量和格式，必要时返回 `“ERR 参数数量错误，命令为 ‘<CMD>’”` 或其他解析错误。
/// 6. **未知命令**：对于任何未识别的输入，返回 `“ERR 未知命令 ‘<cmd>’”`。

pub fn execute(parts: Vec<String>, db: &Db) -> String {
    // 1. 空白命令检查
    if parts.is_empty() {
        return "ERR empty command".to_string();
    }

    // 2. 提取命令名称并将其转换为大写，以实现不区分大小写的匹配。
    let cmd = parts[0].to_uppercase();

    // 3. 惰性过期检测，如果 `parts[1]`即`key`已过期，则移除
    //    if it's expired. We skip this for PING and QUIT.
    if parts.len() > 1 {
        match cmd.as_str() {
            "PING" | "QUIT" => {}
            _ => {
                // Ignore any expiration errors
                let _ = expire::remove_if_expired(db, &parts[1]);
            }
        }
    }

    // 4. Dispatch based on command name
    match cmd.as_str() {
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