use std::env;
use std::error::Error;
use sled;

fn run() -> Result<(), Box<dyn Error>> {
    // 1. 打开 / 创建 数据库
    let db = sled::open("my_db")?;

    // 2. 解析命令行参数
    // 跳过第一个（二进制名）
    let mut args = env::args().skip(1);

    // 3. 提取命令
    let cmd = args.next()
        .ok_or("Usage: kvdb <set|get|remove> [KEY] [VALUE]")?;

    // 4. 根据命令，分支执行任务
    match cmd.as_str() {
        "set" => {
            // 检查参数 key, value
            let key = args.next().ok_or("Missing KEY for set")?;
            let value = args.next().ok_or("Missing VALUE for set")?;

            // 插入或者更新
            db.insert(key.as_bytes(), value.as_bytes())?;
            // 磁盘持久化
            db.flush()?;
            println!("Set key=\"{}\" to value\"{}\"", key, value);
        }

        "get" => {
            // 检查参数 key
            let key = args.next().ok_or("Missing KEY for get")?;

            // 查询
            if let Some(value) = db.get(key.as_bytes())? {
                // 转换Vec<u8>成String，再输出
                let s = String::from_utf8_lossy(&value);
                println!("{}", s);
            } else {
                println!("Key \"{}\" not found", key);
            }
        }

        "remove" => {
            // 检查参数 key
            let key = args.next().ok_or("Missing KEY for remove")?;

            // 删除
            let removed = db.remove(key.as_bytes())?;
            db.flush()?;
            if removed.is_some() {
                println!("Remove key=\"{}\"", key);
            } else {
                println!("Key \"{}\" no found", key);
            }
        }

        _ => {
            eprintln!("Unknown command: {}\nUsage: kvdb <set|get|remove> [KEY] [VALUE]", cmd);
            std::process::exit(1);
        }
    }
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {}", err);
        std::process::exit(1);
    } 
}