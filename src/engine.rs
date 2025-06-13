// src/engine.rs

//! engine 模块：
//! - 负责接收已经切分好的命令（Vec<String>）
//! - 与 sled::Db 交互，执行业务（读写操作）
//! - 返回一个 String 作为响应，供上层网络层封装成 RESP Simple String 或 Error

use sled::Db; // sled 的数据库类型
use crate::types::{hash, list, set}; // 支持多种数据类型
use crate::expire;
/// 核心执行函数
///
/// parts: 客户端发送的命令切分后得到的 token 列表  
/// db:    sled 数据库实例  
///
/// 返回值：String，业务逻辑的“响应”，形如 "PONG"、"OK"、"ERR xxx"
pub fn execute(parts: Vec<String>, db: &Db) -> String {
    // 1. 空命令检查
    if parts.is_empty() {
        return "ERR empty command".to_string();
    }

    // 2. 取出命令名并大写，方便大小写不敏感处理
    let cmd = parts[0].to_uppercase();

    // 统一惰性过期：除 PING/QUIT 之外，凡是第一个参数当作 key 的命令，
    // 在真正执行逻辑前先 remove_if_expired
    if parts.len() > 1 {
        match cmd.as_str() {
            "PING" | "QUIT" => {}
            _ => {
                // 忽略错误
                let _ = expire::remove_if_expired(db, &parts[1]);
            }
        }
    }

    match cmd.as_str() {
        // --- String 原有命令 ---
        "PING" => {
            // PING: 最简单的心跳，返回 PONG
            "PONG".to_string()
        }

        "SET" => {
            // SET <key> <value>
            if parts.len() != 3 {
                return "ERR wrong number of arguments for 'SET' command".to_string();
            }
            let key = parts[1].as_bytes();
            let val = parts[2].as_bytes();
            // sled 的 insert 方法：插入或更新一个 key
            match db.insert(key, val) {
                Ok(_) => {
                    // 我们这里先不立刻 db.flush()，让持久化模块统一管理
                    "OK".to_string()
                }
                Err(e) => {
                    // 任何底层错误都封装成 ERR 开头
                    format!("ERR failed to SET: {}", e)
                }
            }
        }

        "GET" => {
            // GET <key>
            if parts.len() != 2 {
                return "ERR wrong number of arguments for 'GET' command".to_string();
            }
            let key = parts[1].as_bytes();
            // sled.get 返回 Result<Option<IVec>>
            match db.get(key) {
                Ok(Some(ivec)) => {
                    // 我们假设存入的是 UTF-8 文本，尝试转换
                    match std::str::from_utf8(&ivec) {
                        Ok(s) => s.to_string(),
                        Err(_) => {
                            // 如果不是合法 UTF-8，就当成错误
                            format!("ERR non-utf8 data for key '{}'", parts[1])
                        }
                    }
                }
                Ok(None) => {
                    // key 不存在
                    "ERR key not found".to_string()
                }
                Err(e) => {
                    // sled 访问错误
                    format!("ERR failed to GET: {}", e)
                }
            }
        }

        "DEL" => {
            // DEL <key>
            if parts.len() != 2 {
                return "ERR wrong number of arguments for 'DEL' command".to_string();
            }
            let key = parts[1].as_bytes();
            // sled.remove 返回 Result<Option<IVec>>
            match db.remove(key) {
                Ok(Some(_)) => {
                    // 如果返回了 Some，说明确实删除了一个存在的 key
                    "OK".to_string()
                }
                Ok(None) => {
                    // key 本来就不存在
                    "ERR key not found".to_string()
                }
                Err(e) => {
                    format!("ERR failed to DEL: {}", e)
                }
            }
        }

        // --- Hash 命令 ---
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

        // --- List 命令 ---
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

        // --- Set 命令 ---
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

        // --- 过期策略 ---
        "EXPIRE" => {
        // EXPIRE <key> <seconds>，增加过期时间
            if parts.len() != 3 {
                return "ERR wrong number of arguments for 'EXPIRE'".to_string();
            }
            let key = &parts[1];
            // 秒数必须能 parse
            match parts[2].parse::<u64>() {
                Ok(secs) => {
                    // 调用 expire 模块
                    match expire::expire(db, key, secs) {
                        Ok(v) => v,                   // "1" 或 "0"
                        Err(e) => format!("ERR {}", e),
                    }
                }
                Err(_) => "ERR value is not an integer or out of range".to_string(),
            }
        }

        // TTL <key> ，查询过期时间
        "TTL" => {
            if parts.len() != 2 {
                return "ERR wrong number of arguments for 'TTL'".to_string();
            }
            match expire::ttl(db, &parts[1]) {
                Ok(v) => v,  // "-2", "-1" 或 剩余秒数
                Err(e) => format!("ERR {}", e),
            }
        }

        // PERSIST <key> ，删除过期时间
        "PERSIST" => { 
            if parts.len() != 2 {
                return "ERR wrong number of arguments for 'PERSIST'".to_string();
            }
            match expire::persist(db, &parts[1]) {
                Ok(v) => v,  // "1" 或 "0"
                Err(e) => format!("ERR {}", e),
            }
        }

        // 其他命令
        "QUIT" => {
            // 客户端主动断开可能会发 QUIT
            // 返回 OK，由 server 层决定断开循环
            "OK".to_string()
        }

        other => {
            // 不认识的命令
            format!("ERR unknown command '{}'", other)
        }
    }
}