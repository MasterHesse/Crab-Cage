// src/server.rs
//! 这是 rudis 服务的网络层：
//! - 监听 TCP 连接  
//! - 解码请求（文本 / RESP）  
//! - 调度到 engine 执行  
//! - 写命令时同步到持久化器  
//! - 以 RESP Simple String/Error 形式回复
use anyhow::Result;
use std::sync::Arc;
use std::io::ErrorKind;

use tokio::{
    net::{TcpListener, TcpStream},
    io::{AsyncReadExt, AsyncBufReadExt, AsyncWriteExt, BufReader},
};

use sled::Db;
use crate::{engine, persistence::Persistence};

/// 启动 KV 服务（stub）
/// 默认入口: 监听 127.0.0.1:6380
pub async fn start_with_db_and_pers(db: sled::Db, pers: Arc<Persistence>) -> Result<()> {
    // 绑定地址
    let addr = "127.0.0.1:6380";
    let listener = TcpListener::bind(addr).await?;
    println!("rudis server listening on {}", addr);
    
    // 进入接受循环
    serve_with_db(listener, db, pers).await
}

/// 核心循环: 接受循环 不断接受 accpet 新连接 并 spawn 出去一个异步任务
async fn serve_with_db(listener: TcpListener, db: Db, pers: Arc<Persistence>) -> Result<()> {
    loop {
        // accept() 返回一个 TcpStream 和客户端地址
        let (stream, peer) = listener.accept().await?;
        println!("Accepted connection from {}", peer);

        // 克隆 Arc，给新的任务一份引用
        let db_clone = db.clone();
        let pers_clone = pers.clone();

        // 为每个连接启动异步任务
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, db_clone, pers_clone).await {
                eprintln!("Connection error: {}", err);
            }
        });
    }
}

/// 单个连接的处理逻辑
/// - 先读第一个字节，区分「RESP Array」或「简单文本」协议
/// - 解析成 Vec<String> parts
/// - 调 engine 执行业务
/// - 如果是写命令，追加 AOF & 触发 RDB
/// - 统一以 RESP Simple String/Error 回复
async fn handle_connection(
    stream: TcpStream,
    db: Db,
    pers: Arc<Persistence>,
) -> Result<()> {
    // 记录对端地址，用于日志
    let peer = stream.peer_addr()?;
    // 把流拆成 reader / writer
    let (reader, mut writer) = stream.into_split();
    // 用 BufReader 包装，以便使用 read_line()
    let mut reader = BufReader::new(reader);

    loop {
        // ----- 1) 先读一个字节，决定协议类型 -----
        let mut first = [0u8; 1];
        match reader.read_exact(&mut first).await {
            // 正常读取到一个字节
            Ok(_) => {}
            // 客户端断开（EOF） or Windows 下的 RST(10054)
            Err(e) if e.kind() == ErrorKind::UnexpectedEof
                     || e.kind() == ErrorKind::ConnectionReset => {
                println!("{} disconnected", peer);
                break;
            }
            // 其它 I/O 错误
            Err(e) => return Err(e.into()),
        }

        // ----- 2) 解析命令到 Vec<String> -----
        let parts: Vec<String> = if first[0] == b'*' {
            // --- RESP Array 分支 ---
            // 典型请求示例：
            //   *2\r\n$3\r\nSET\r\n$5\r\nmykey\r\n
            // 我们只简单支持 Array + Bulk String

            // 2.1 读取 "*<N>\r\n" 剩余部分，拿到 N
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            // 去掉 '*'，再解析数字
            let count: usize = line.trim().parse()?;

            let mut cmd = Vec::with_capacity(count);
            for _ in 0..count {
                // 2.2 读 "$<len>\r\n"
                line.clear();
                reader.read_line(&mut line).await?;
                let len: usize = line
                    .trim_start_matches('$')
                    .trim()
                    .parse()?;

                // 2.3 读实际 payload
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf).await?;
                // 丢弃结尾的 "\r\n"
                let mut crlf = [0u8; 2];
                reader.read_exact(&mut crlf).await?;

                // 转成 UTF-8 字符串
                let s = String::from_utf8(buf)?;
                cmd.push(s);
            }
            cmd
        } else {
            // --- 简单文本协议分支 ---
            // 我们已经读了第一个 byte，把它当成 ASCII 字符
            // 例如 'P'，后面还要读到 '\n'
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            // 把 first + line 拼成完整一行
            let mut full = String::new();
            full.push(first[0] as char);
            full.push_str(&line);
            // 按空格拆分
            full
                .trim_end()
                .split_whitespace()
                .map(str::to_string)
                .collect()
        };

        // 如果 client 发了空行，就重新循环
        if parts.is_empty() {
            continue;
        }

        // ----- 3) 调度到 engine 执行业务 -----
        // 我们假设 engine::execute(parts, &db) -> String
        let cmd_name = parts[0].to_uppercase();
        let is_write = matches!(cmd_name.as_str(), "SET" | "DEL" /*| …*/);

        // 以空格 join 回去，用于记录到 AOF
        let raw_cmd = parts.join(" ");
        let resp = engine::execute(parts, &db);

        // ----- 4) 如果是写命令，再追加 AOF 并触发 RDB -----
        if is_write {
            pers.append_aof_and_maybe_snapshot(&raw_cmd, &db);
        }

        // ----- 5) 统一用 RESP Simple String / Error 格式回复 -----
        // +OK\r\n   或  -ERR something\r\n
        let out = if resp.starts_with("ERR") {
            format!("-{}\r\n", resp)
        } else {
            format!("+{}\r\n", resp)
        };
        writer.write_all(out.as_bytes()).await?;
    }

    Ok(())
}

