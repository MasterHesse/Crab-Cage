// tests/integration_types.rs

use anyhow::Result;
use sled::Db;
use std::{env, str};
use tempfile::tempdir;

// 从库里导出 engine::execute
use rudis::engine::execute;

/// 辅助：把 &str 切片数组转成 Vec<String>
fn cmd(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_types_integration() -> Result<()> {
    // 1) 临时目录，隔离文件
    let tmp = tempdir()?;
    env::set_current_dir(tmp.path())?;

    // 2) 打开 sled 实例
    let db: Db = sled::open("db")?;

    //
    // -------- String --------
    //
    let r = execute(cmd(&["SET", "key", "val"]), &db);
    assert_eq!(r, "OK", "SET 应返回 OK");

    let r = execute(cmd(&["GET", "key"]), &db);
    assert_eq!(r, "val", "GET 应返回刚设的值");

    let r = execute(cmd(&["DEL", "key"]), &db);
    assert_eq!(r, "OK", "DEL 应返回 OK");

    let r = execute(cmd(&["GET", "key"]), &db);
    assert_eq!(r, "ERR key not found", "GET 不存在的 key 应返回 ERR key not found");

    //
    // -------- Hash --------
    //
    let r = execute(cmd(&["HSET", "h", "f1", "v1"]), &db);
    assert_eq!(r, "1", "HSET 新 field 应返回 1");

    let r = execute(cmd(&["HSET", "h", "f1", "v2"]), &db);
    assert_eq!(r, "0", "HSET 覆盖已有 field 应返回 0");

    let r = execute(cmd(&["HGET", "h", "f1"]), &db);
    assert_eq!(r, "v2", "HGET 应返回覆盖后的值");

    let r = execute(cmd(&["HKEYS", "h"]), &db);
    assert_eq!(r, "f1", "HKEYS 单个 field");

    let r = execute(cmd(&["HVALS", "h"]), &db);
    assert_eq!(r, "v2", "HVALS 单个 value");

    let all_res = execute(cmd(&["HGETALL", "h"]), &db);
    let mut all: Vec<&str> = all_res.split(',').collect();
    all.sort();
    assert_eq!(all, vec!["f1", "v2"], "HGETALL 返回 field,value 列表");

    let r = execute(cmd(&["HDEL", "h", "f1"]), &db);
    assert_eq!(r, "1", "HDEL 删除存在的 field 应返回 1");

    let r = execute(cmd(&["HGET", "h", "f1"]), &db);
    assert_eq!(r, "nil", "HGET 删除后应返回 nil");

    //
    // -------- List --------
    //
    let r = execute(cmd(&["LPUSH", "L", "a"]), &db);
    assert_eq!(r, "1", "LPUSH 第一个元素，长度应为 1");

    let r = execute(cmd(&["LPUSH", "L", "b"]), &db);
    assert_eq!(r, "2", "LPUSH 再 push，长度应为 2 (b,a)");

    let r = execute(cmd(&["RPUSH", "L", "c"]), &db);
    assert_eq!(r, "3", "RPUSH，在末尾追加，长度应为 3 (b,a,c)");

    let r = execute(cmd(&["LRANGE", "L", "0", "2"]), &db);
    assert_eq!(r, "b,a,c", "LRANGE 0 2 应返回 b,a,c");

    let r = execute(cmd(&["LPOP", "L"]), &db);
    assert_eq!(r, "b", "LPOP 应弹出 b");

    let r = execute(cmd(&["RPOP", "L"]), &db);
    assert_eq!(r, "c", "RPOP 应弹出 c");

    // 剩下 [a]
    let r = execute(cmd(&["LRANGE", "L", "0", "-1"]), &db);
    assert_eq!(r, "a", "LRANGE 剩下的元素 a");

    //
    // -------- Set --------
    //
    let r = execute(cmd(&["SADD", "S", "x"]), &db);
    assert_eq!(r, "1", "SADD 新 member 应返回 1");

    let r = execute(cmd(&["SADD", "S", "x"]), &db);
    assert_eq!(r, "0", "SADD 重复 member 应返回 0");

    let r = execute(cmd(&["SADD", "S", "y"]), &db);
    assert_eq!(r, "1", "SADD 新 member y 应返回 1");

    let members_res = execute(cmd(&["SMEMBERS", "S"]), &db);
    let mut members: Vec<&str> = if members_res.is_empty() {
        Vec::new()
    } else {
        members_res.split(',').collect()
    };
    members.sort();
    assert_eq!(members, vec!["x", "y"], "SMEMBERS 应返回所有 member");

    let r = execute(cmd(&["SISMEMBER", "S", "x"]), &db);
    assert_eq!(r, "1", "SISMEMBER 存在时返回 1");

    let r = execute(cmd(&["SREM", "S", "x"]), &db);
    assert_eq!(r, "1", "SREM 删除存在的 member 返回 1");

    let r = execute(cmd(&["SISMEMBER", "S", "x"]), &db);
    assert_eq!(r, "0", "删除后 SISMEMBER 应返回 0");

    Ok(())
}