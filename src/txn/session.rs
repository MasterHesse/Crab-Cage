// src/txn/session.rs

/// 保存单个连接的 MULTI 队列状态
#[derive(Debug)]
pub struct TxnSession {
    pub in_multi: bool,
    pub queue: Vec<Vec<String>>,
}

impl TxnSession {
    pub fn new() -> Self {
        TxnSession { in_multi: false, queue: Vec::new() }
    }

    pub fn begin(&mut self) -> Result<&'static str, &'static str> {
        if self.in_multi {
            Err("ERR MULTI calls can not be nested")
        } else {
            self.in_multi = true;
            self.queue.clear();
            Ok("OK")
        }
    }

    pub fn enqueue(&mut self, cmd: Vec<String>) -> Result<&'static str, ()> {
        if !self.in_multi {
            Err(())
        } else {
            self.queue.push(cmd);
            Ok("QUEUED")
        }
    }

    pub fn discard(&mut self) -> Result<&'static str, &'static str> {
        if !self.in_multi {
            Err("ERR DISCARD without MULTI")
        } else {
            self.in_multi = false;
            self.queue.clear();
            Ok("OK")
        }
    }

    pub fn take_queue(&mut self) -> Result<Vec<Vec<String>>, &'static str> {
        if !self.in_multi {
            Err("ERR EXEC without MULTI")
        } else {
            self.in_multi = false;
            Ok(std::mem::take(&mut self.queue))
        }
    }

    /// 获取当前队列中的命令（不改变状态）
    pub fn get_queued_commands(&self) -> Option<Vec<String>> {
        if !self.in_multi || self.queue.is_empty() {
            return None;
        }
        
        Some(self.queue.iter().map(|parts| parts.join(" ")).collect())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    // 测试初始化
    // 是否初始为 事务状态
    // 是否初始为 空执行队列
    #[test]
    fn test_new() {
        let session = TxnSession::new();
        assert!(!session.in_multi);
        assert!(session.queue.is_empty());
    }

    // 测试 begin()
    // 调用 begin ，返回 OK
    // 进入事务状态
    // 执行队列为空
    #[test]
    fn test_begin_success() {
        let mut session = TxnSession::new();
        assert_eq!(session.begin(), Ok("OK"));
        assert!(session.in_multi);
        assert!(session.queue.is_empty());
    }

    // 测试 begin
    // 调用 begin ，返回 OK
    // 再调用 begin ，返回错误（已经在事务中）
    #[test]
    fn test_begin_nested_failure() {
        let mut session = TxnSession::new();
        assert_eq!(session.begin(), Ok("OK"));
        assert_eq!(
            session.begin(),
            Err("ERR MULTI calls can not be nested")
        );
        assert!(session.in_multi); // 状态应保持不变
    }

    // 测试执行队列
    // 调用 begin 后
    // cmd 命令 是否正常入队
    #[test]
    fn test_enqueue_success() {
        let mut session = TxnSession::new();
        session.begin().unwrap();
        let cmd = vec!["SET".to_string(), "key".to_string(), "value".to_string()];
        assert_eq!(session.enqueue(cmd.clone()), Ok("QUEUED"));
        assert_eq!(session.queue, vec![cmd]);
    }

    // 测试执行队列
    // 未调用 begin
    // 命令无法入队
    #[test]
    fn test_enqueue_failure_not_in_multi() {
        let mut session = TxnSession::new();
        let cmd = vec!["SET".to_string(), "key".to_string(), "value".to_string()];
        assert_eq!(session.enqueue(cmd), Err(()));
        assert!(session.queue.is_empty());
    }

    // 测试 DISCARD 策略
    // 调用 DISCARD 后，事务状态变成 false，执行队列为空
    #[test]
    fn test_discard_success() {
        let mut session = TxnSession::new();
        session.begin().unwrap();
        session.enqueue(vec!["CMD".to_string()]).unwrap();
        assert_eq!(session.discard(), Ok("OK"));
        assert!(!session.in_multi);
        assert!(session.queue.is_empty());
    }

    // 测试 DISCARD 策略
    // 未开启事务，无法调用 DISCARD
    #[test]
    fn test_discard_failure_not_in_multi() {
        let mut session = TxnSession::new();
        assert_eq!(session.discard(), Err("ERR DISCARD without MULTI"));
        assert!(!session.in_multi);
    }

    // 输出执行队列，关闭事务状态
    #[test]
    fn test_take_queue_success() {
        let mut session = TxnSession::new();
        session.begin().unwrap();
        let cmd1 = vec!["CMD1".to_string()];
        let cmd2 = vec!["CMD2".to_string()];
        session.enqueue(cmd1.clone()).unwrap();
        session.enqueue(cmd2.clone()).unwrap();

        let queue = session.take_queue();
        assert_eq!(queue, Ok(vec![cmd1, cmd2]));
        assert!(!session.in_multi);
        assert!(session.queue.is_empty());
    }

    // 未开启事务，无法输出执行队列
    #[test]
    fn test_take_queue_failure_not_in_multi() {
        let mut session = TxnSession::new();
        assert_eq!(
            session.take_queue(),
            Err("ERR EXEC without MULTI")
        );
        assert!(!session.in_multi);
    }

    // 输出执行队列后，可重新开启事务
    #[test]
    fn test_sequence_operations() {
        let mut session = TxnSession::new();
        
        // 开始事务
        assert_eq!(session.begin(), Ok("OK"));
        
        // 添加命令
        let cmd1 = vec!["GET".to_string(), "key1".to_string()];
        let cmd2 = vec!["SET".to_string(), "key2".to_string(), "value".to_string()];
        assert_eq!(session.enqueue(cmd1.clone()), Ok("QUEUED"));
        assert_eq!(session.enqueue(cmd2.clone()), Ok("QUEUED"));
        
        // 执行事务
        let queue = session.take_queue();
        assert_eq!(queue, Ok(vec![cmd1, cmd2]));
        
        // 检查状态已重置
        assert!(!session.in_multi);
        assert!(session.queue.is_empty());
        
        // 可以重新开始新事务
        assert_eq!(session.begin(), Ok("OK"));
    }
}