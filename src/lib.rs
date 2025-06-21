// src/lib.rs
//! rudis 库：protocol / server / engine / expire / txn / monitor / types

pub mod config;
pub mod server;    // 网络层 & 命令分发
pub mod engine;    // 存储引擎（sled + 持久化）
pub mod expire;    // 过期策略
pub mod types;     // String / Hash / List / Set / ... 数据结构
pub mod persistence;
