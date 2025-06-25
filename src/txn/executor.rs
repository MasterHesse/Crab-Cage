// src/txn/executor.rs

use anyhow::{Result, Error};
use sled::{transaction::{ConflictableTransactionError, TransactionError}, Tree};
use crate::engine;
use sled::{Db};

// 事务的执行命令
// 逐一执行事务队列中的每条命令
// 任一命令若返回 ERR ， 则 Abort
pub fn exec_all(db: &Db, cmds: &[Vec<String>]) -> Vec<String> {
    let tree: Tree = db.open_tree("").expect("Failed to open transaction tree");
    
    let res: Result<Vec<String>, TransactionError<Error>> = tree.transaction(|tx| {
        let mut out = Vec::with_capacity(cmds.len());
        for parts in cmds {
            let r = engine::execute_non_txn_command(&parts[0].to_uppercase(), parts, tx);
            if r.starts_with("ERR") {
                return Err(ConflictableTransactionError::Abort(Error::msg(r)));
            }
            out.push(r);
        }
        Ok(out)
    });

    match res {
        Ok(v) => v,
        Err(e) => vec![format!("ERR {}", e)],
    }
}

