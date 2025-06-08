// src/engine.rs

//! engine 模块：
//! - 负责接收已经切分好的命令（Vec<String>）
//! - 与 sled::Db 交互，执行业务（读写操作）
//! - 返回一个 String 作为响应，供上层网络层封装成 RESP Simple String 或 Error

use sled::Db; // sled 的数据库类型

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

    match cmd.as_str() {
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