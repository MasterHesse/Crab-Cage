// src/txn/mod.rs

use std::collections::HashSet;
use sled::{
    transaction::{ConflictableTransactionError, TransactionError, Transactional, TransactionalTree}, Db
};
use crate::engine;

/// 每个连接独立的事务上下文
pub struct TxnContext {
    pub in_multi: bool,
    queue: Vec<Vec<String>>,
    watched: HashSet<String>, // WATCH/UNWATCH 后续扩展
}

impl TxnContext {
    pub fn new() -> Self {
        TxnContext { in_multi: false, queue: Vec::new(), watched: HashSet::new() }
    }

    pub fn multi(&mut self) -> String {
        if self.in_multi {
            "ERR MULTI calls cannot be nested".into()
        } else {
            self.in_multi = true;
            "OK".into()
        }
    }

    pub fn discard(&mut self) -> String {
        if !self.in_multi {
            "ERR DISCARD without MULTI".into()
        } else {
            self.in_multi = false;
            self.queue.clear();
            "OK".into()
        }
    }

    pub fn queue_cmd(&mut self, parts: Vec<String>) -> String {
        if !self.in_multi {
            "ERR QUEUED commands not allowed outside MULTI".into()
        } else {
            self.queue.push(parts);
            "QUEUED".into()
        }
    }

    pub fn exec(&mut self, db: &Db) -> String {
        if !self.in_multi {
            return "ERR EXEC without MULTI".into();
        }
        let cmds = std::mem::take(&mut self.queue);
        self.in_multi = false;

        let tx_res = db.transaction::<_, _, String>(|tx_db| {
            let mut replies = Vec::with_capacity(cmds.len());
            for parts in &cmds {
                match crate::engine::execute_txn(parts.clone(), tx_db) {
                    Ok(resp) => {
                        if resp.starts_with("ERR") {
                            return Err(ConflictableTransactionError::Abort(resp));
                        }
                        replies.push(resp);
                    }
                    Err(e) => return Err(ConflictableTransactionError::Abort(e)),
                }
            }
            Ok(replies)
        });

        match tx_res {
            Ok(rps) => {
                let mut s = String::from("[");
                s += &rps.join(", ");
                s.push(']');
                s
            }
            Err(TransactionError::Abort(reason)) => {
                format!("ERR Transaction aborted: {}", reason)
            }
            Err(e) => format!("ERR Transaction failed: {:?}", e),
        }
    }
}