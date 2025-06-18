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
use crate::txn::TxnContext;

/// 按指定地址启动服务
pub async fn start_with_addr_db_and_pers(
    addr: &str,
    db: Db,
    pers: Arc<Persistence>,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    println!("Rudis server listening on {}", addr);
    serve_with_db(listener, db, pers).await
}

async fn serve_with_db(listener: TcpListener, db: Db, pers: Arc<Persistence>) -> Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        println!("Accepted connection from {}", peer);

        let db = db.clone();
        let pers = pers.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, db, pers).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    db: Db,
    pers: Arc<Persistence>,
) -> Result<()> {
    let peer = stream.peer_addr()?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        // 1) 读第一个字节以区分 RESP vs 文本
        let mut first = [0u8; 1];
        match reader.read_exact(&mut first).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof
                     || e.kind() == ErrorKind::ConnectionReset => {
                println!("{} disconnected", peer);
                break;
            }
            Err(e) => return Err(e.into()),
        }

        // 2) 解析成 Vec<String>
        let parts: Vec<String> = if first[0] == b'*' {
            // RESP Array + Bulk String
            // 读 "*N\r\n"
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            let count: usize = line.trim().parse()?;

            let mut cmd = Vec::with_capacity(count);
            for _ in 0..count {
                // 读 "$len\r\n"
                line.clear();
                reader.read_line(&mut line).await?;
                let len: usize = line.trim_start_matches('$').trim().parse()?;

                // 读 payload + "\r\n"
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf).await?;
                let mut crlf = [0u8; 2];
                reader.read_exact(&mut crlf).await?;

                cmd.push(String::from_utf8(buf)?);
            }
            cmd
        } else {
            // 简单文本协议
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            let mut full = String::new();
            full.push(first[0] as char);
            full.push_str(&line);
            full
                .trim_end()
                .split_whitespace()
                .map(str::to_string)
                .collect()
        };

        if parts.is_empty() {
            continue;
        }

        // 3) 调度到 engine
        let cmd_name = parts[0].to_uppercase();
        let is_write = matches!(
            cmd_name.as_str(),
            /* String  */ "SET" | "DEL" | "INCR" | "DECR" |
            /* Hash    */ "HSET" | "HDEL" |
            /* List    */ "LPUSH" | "RPUSH" | "LPOP" | "RPOP" |
            /* Set     */ "SADD" | "SREM"
        );
        let raw = parts.join(" ");
        let resp = engine::execute(parts, &db);

        // 4) 写命令时追加 AOF & 触发快照
        if is_write {
            pers.append_aof_and_maybe_snapshot(&raw, &db);
        }

        // 5) 用 RESP SimpleString / Error 回复
        let out = if resp.starts_with("ERR") {
            format!("-{}\r\n", resp)
        } else {
            format!("+{}\r\n", resp)
        };
        writer.write_all(out.as_bytes()).await?;
    }

    Ok(())
}