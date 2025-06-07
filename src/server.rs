// src/server.rs
use anyhow::Result;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream}
};
use crate::engine;

/// 启动 KV 服务（stub）
/// 默认入口: 监听 127.0.0.1:6380
pub async fn start() -> Result<()> {
    let addr = "127.0.0.1:6380";
    let listener = TcpListener::bind(addr).await?;
    println!("kvdb server listening on {}", addr);
    serve(listener).await
}

/// 核心循环: 接受循环 并 spawn 出去
pub async fn serve(listener: TcpListener) -> Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        println!("Accepted connection from {}", peer);
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream).await {
                eprintln!("Connection error: {}", err);
            }
        });
    }
}

/// 单个连接的处理逻辑：按行执行命令、split 空格、调度 engine、回写结果
async fn handle_connection(stream: TcpStream) -> Result<()> {
    let peer = stream.peer_addr()?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        // 1) 先读一个字节，看看是普通文本，还是 RESP Array（'*' 开头）
        let mut first = [0u8; 1];
        match reader.read_exact(&mut first).await {
            Ok(_) => {}
            // 如果客户端断开，就跳出循环
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        // 2) 根据首字节分两条逻辑
        let parts: Vec<String> = if first[0] == b'*' {
            // RESP Array 格式：*<N>\r\n then N * ( $<len>\r\n<payload>\r\n )
            // 2.1 读 "*<N>\r\n" 剩余部分
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            let count: usize = line.trim().parse()?;

            // 2.2 循环读 N 个 bulk strings
            let mut cmd = Vec::with_capacity(count);
            for _ in 0..count {
                // 读 "$<len>\r\n"
                line.clear();
                reader.read_line(&mut line).await?;
                let len: usize = line.trim_start_matches('$').trim().parse()?;

                // 读 payload
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf).await?;
                // 丢掉结尾的 "\r\n"
                let mut crlf = [0u8; 2];
                reader.read_exact(&mut crlf).await?;

                cmd.push(String::from_utf8(buf)?);
            }
            cmd
        } else {
            // 文本协议：先把第一个字节拼回去，再 read_line
            let mut line = String::new();
            reader.read_line(&mut line).await?;
            // 包括 first[0] + line
            let mut full = String::new();
            full.push(first[0] as char);
            full.push_str(&line);
            // 空格分词
            full
                .trim_end()
                .split_whitespace()
                .map(str::to_string)
                .collect()
        };

        // 3) 调度到底层 engine
        let mut resp = engine::excute(parts);

        // 4) 包装成 RESP 返回给客户端
        let out = if resp.starts_with("ERR") {
            // Error: -ERR xxx\r\n
            format!("-{}\r\n", resp)
        } else {
            // Simple String: +xxx\r\n
            format!("+{}\r\n", resp)
        };
        writer.write_all(out.as_bytes()).await?;
    }

    println!("{} disconnected", peer);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
        time::{sleep, Duration},
    };

    /// 解析一个 RESP Simple String，去掉前缀 '+' 和尾部 "\r\n"
    fn parse_simple_string(line: &str) -> &str {
        let s = line.trim_end();  // 去掉 "\r\n" 及其它尾部空白
        assert!(s.starts_with('+'), "expected '+' prefix but got {:?}", s);
        &s[1..]
    }

    #[tokio::test]
    async fn test_ping_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener).await.unwrap() });
        sleep(Duration::from_millis(100)).await;

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (r, mut w) = stream.split();
        let mut r = BufReader::new(r);

        w.write_all(b"PING\r\n").await.unwrap();
        let mut line = String::new();
        r.read_line(&mut line).await.unwrap();

        // 解析 RESP Simple String
        let content = parse_simple_string(&line);
        assert_eq!(content, "PONG");
    }

    #[tokio::test]
    async fn test_multiple_clients() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener).await.unwrap() });
        sleep(Duration::from_millis(100)).await;

        let mut handles = Vec::new();
        for _ in 0..5 {
            let addr = addr.clone();
            handles.push(tokio::spawn(async move {
                let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
                let (r, mut w) = stream.split();
                let mut r = BufReader::new(r);

                w.write_all(b"PING\r\n").await.unwrap();
                let mut line = String::new();
                r.read_line(&mut line).await.unwrap();

                let content = parse_simple_string(&line);
                assert_eq!(content, "PONG");
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
}