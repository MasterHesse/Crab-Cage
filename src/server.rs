// src/server.rs
//! 这是 rudis 服务的网络层：
//! - 监听 TCP 连接  
//! - 解码请求（文本 / RESP）  
//! - 调度到 engine 执行  
//! - 写命令时同步到持久化器  
//! - 以 RESP Simple String/Error 形式回复
use anyhow::Result;
use std::{sync::{
    atomic::{AtomicU64, Ordering}, Arc
}, time::Instant};
use std::io::ErrorKind;
use tokio::{
    net::{TcpListener, TcpStream},
    io::{AsyncReadExt, AsyncBufReadExt, AsyncWriteExt, BufReader}
};
use crate::{engine, persistence::Persistence, txn::session::TxnSession};
use crate::engine::KvEngine;
use crate::monitor::{Monitor, info};

/// 按指定地址启动服务
pub async fn start_with_addr_db_and_pers<E>(
    addr: &str,
    db: E,
    pers: Arc<Persistence>,
    monitor: Arc<Monitor>,
) -> Result<()> 
where 
    E: KvEngine + Send + Sync + 'static + Clone,
{
    let listener = TcpListener::bind(addr).await?;
    println!("Carb-Cage server listening on {}", addr);
    serve_with_db(listener, db, pers, monitor).await
}

async fn serve_with_db<E>(
    listener: TcpListener, 
    db: E, 
    pers: Arc<Persistence>,
    monitor: Arc<Monitor>,
) -> Result<()> 
where 
    E: KvEngine + Send + Sync +'static + Clone,
{
    // Sesson ID 计数器
    static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

    loop {
        let (stream, peer) = listener.accept().await?;
        println!("Accepted connection from {}", peer);

        let db = db.clone();
        let pers = pers.clone();
        let monitor = monitor.clone();

        // 注册客户端
        let client_id = monitor.client_tracker.add_client(peer);
        monitor.metrics.connected_clients.fetch_add(1, Ordering::Relaxed);
        monitor.metrics.total_connections.fetch_add(1, Ordering::Relaxed);
        
        tokio::spawn(async move {
            if let Err(e) = 
                handle_connection(
                    stream, 
                    db, 
                    pers,
                    monitor.clone(),
                    client_id,
                    SESSION_COUNTER
                        .fetch_add(1, Ordering::SeqCst))
                        .await
                    
            {
                eprintln!("Connection error: {}", e);
            }

            // 断开连接时清理
            monitor.client_tracker.remove_client(client_id);
            monitor.metrics.connected_clients.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

async fn handle_connection<E>(
    stream: TcpStream,
    db: E,
    pers: Arc<Persistence>,
    monitor: Arc<Monitor>,
    client_id: u64,
    session_id: u64,
) -> Result<()> 
where 
    E: KvEngine + Send + Sync + 'static,
{
    let peer = stream.peer_addr()?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // 每个连接创建一个单独的事务会话
    let mut txn_session = TxnSession::new(session_id);

    loop {
        // 1) 读第一个字节以区分 RESP vs 文本
        let mut first = [0u8; 1];
        match reader.read_exact(&mut first).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof
                     || e.kind() == ErrorKind::ConnectionReset => {
                println!("{} disconnected", peer);

                // 断开前，清理监视
                if let Some(watch_manager) = db.watch_manager() {
                    watch_manager.clear_session(session_id);
                }

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

        // 3) 处理监控命令
        let cmd_name = parts[0].to_uppercase();
        match cmd_name.as_str() {
            "INFO" => {
                let section = parts.get(1).map(|s| s.as_str());
                let response = info::build_info_response(section, &db, &pers, &monitor.metrics);
                writer.write_all(format!("${}\r\n{}\r\n", response.len(), response).as_bytes()).await?;
                continue;
            }
            "CLIENT" if parts.len() > 1 && parts[1].to_uppercase() == "LIST" => {
                let response = monitor.client_tracker.list_clients();
                writer.write_all(format!("${}\r\n{}\r\n", response.len(), response).as_bytes()).await?;
                continue;
            }
            "SLOWLOG" => {
                let response = monitor.slow_log.get_logs();
                writer.write_all(format!("${}\r\n{}\r\n", response.len(), response).as_bytes()).await?;
                continue;
            }
            _=>{}
        }

        // 4) 调度到 engine
        let is_write = matches!(cmd_name.as_str(), 
            // string
            "SET" | "DEL" | "GET" | "INCR" | "DECR" |
            "HSET" | "HGET" | "HDEL" | "HKEYS" | "HVALS" | "HGETALL" |
            "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LRANGE" |
            "SADD" | "SREM" | "SMEMBERS" | "SISMEMBER" |
            "EXPIRE" | "TTL" | "PERSIST" |
            "MULTI" | "EXEC" | "DISCARD" |
            "WATCH" | "UNWATCH" |
            "PING" | "QUIT"
         );
        let raw = parts.join(" ");

        let start_time = Instant::now();
        let resp = engine::execute(parts.clone(), &db, &mut txn_session);
        let duration = start_time.elapsed();

        // 更新监控数据
        monitor.client_tracker.update_command(client_id, &cmd_name);
        monitor.metrics.record_command(&cmd_name);
        monitor.slow_log.add_entry(&raw, duration, &peer.to_string());

        // 4) 写命令时追加 AOF & 触发快照
        // 注意：事务中的命令只在 EXEC 时持久化
        if is_write {
            if cmd_name == "EXEC" {
                // 对于 EXEC 命令，持久化整个事务队列
                if let Some(cmds) = txn_session.get_queued_commands() {
                    for cmd in cmds {
                        pers.append_aof_and_maybe_snapshot(&cmd, &db.as_db().unwrap());
                    }
                }
            } else if !txn_session.in_multi {
                // 非事务模式下的写命令直接持久化
                pers.append_aof_and_maybe_snapshot(&raw, &db.as_db().unwrap());
            }
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