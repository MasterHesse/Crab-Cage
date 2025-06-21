// src/engine.rs

//! # Engine Module
//!
//! The `engine` module is the core of the Redis-like server. It:
//! - Receives parsed and tokenized commands (`Vec<String>`) from the network layer.
//! - Interacts with the underlying `sled::Db` for data operations.
//! - Delegates to type-specific submodules (`string`, `hash`, `list`, `set`) and the `expire`
//!   module to execute business logic.
//! - Returns a response `String`, which the network layer will format as RESP Simple Strings
//!   or Errors.
//!
//! This design cleanly separates parsing, command dispatch, data storage, and expiration logic.

use sled::Db;
use crate::types::{hash, list, set, string};
use crate::expire;

/// Execute a single client command against the given database.
///
/// # Arguments
///
/// * `parts` – A `Vec<String>` containing the command name and its arguments,
///   already split by the network layer. For example: `["SET", "key", "value"]`.
/// * `db`    – A reference to the opened `sled::Db` instance.
///
/// # Returns
///
/// A `String` representing the result of the command:
/// - For successful operations: things like `"OK"`, `"PONG"`, or numeric/string payloads.
/// - For errors: messages beginning with `"ERR "`, e.g. `"ERR wrong number of arguments"`.
///
/// # Behavior
///
/// 1. **Empty Command Check**: Returns an error if no tokens are provided.
/// 2. **Case-Insensitive Command Name**: Uppercases the first token to dispatch commands
///    in a case-insensitive manner.
/// 3. **Lazy Expiration**: For commands that take a key as their first argument (all except
///    `PING` and `QUIT`), it invokes `expire::remove_if_expired` before execution,
///    to purge expired keys transparently.
/// 4. **Command Dispatch**: Matches on the uppercased command name and calls into
///    the appropriate submodule or expiration functions.
/// 5. **Argument Validation**: Checks the number and format of arguments, returning
///    `"ERR wrong number of arguments for '<CMD>'"` or other parse errors as needed.
/// 6. **Unknown Commands**: Returns `"ERR unknown command '<cmd>'"` for any unrecognized input.
pub fn execute(parts: Vec<String>, db: &Db) -> String {
    // 1. Empty command check
    if parts.is_empty() {
        return "ERR empty command".to_string();
    }

    // 2. Extract command name and uppercase it for case-insensitive matching
    let cmd = parts[0].to_uppercase();

    // 3. Lazy expiration: for commands that operate on a key (parts[1]), remove it
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