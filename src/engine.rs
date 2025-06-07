

/// 简单命令执行器 Stub
pub fn excute(cmd: Vec<String>) -> String {
    if cmd.is_empty() {
        return "Error empty command".to_string();
    }
    match cmd[0].to_lowercase().as_str() {
        "ping" => "PONG".to_string(),
        _ => format!("ERR unknown command {}", cmd[0]),
    }
}