# Crab-Cage 蟹笼

[![Crates.io](https://img.shields.io/crates/v/rudis)](https://crates.io/crates/rudis) [![Rust](https://img.shields.io/badge/rust-1.70+-orange)](https://www.rust-lang.org/) [![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE)

一个用 Rust 实现的，个人学习版的 Redis-like 内存数据缓存。

---

## 目录

- [Crab-Cage 蟹笼](#crab-cage-蟹笼)
  - [目录](#目录)
  - [什么是 Crab-Cage?](#什么是-crab-cage)
  - [为什么开发 Crab-Cage?](#为什么开发-crab-cage)
  - [文件架构](#文件架构)
  - [特性](#特性)
  - [快速开始](#快速开始)
    - [前提](#前提)
    - [构建与运行](#构建与运行)
    - [使用示例](#使用示例)
      - [连接到 rudis 服务](#连接到-rudis-服务)
      - [String 数据类型操作](#string-数据类型操作)
      - [Hash 数据类型操作](#hash-数据类型操作)
      - [List 数据类型操作](#list-数据类型操作)
      - [Set 数据类型操作](#set-数据类型操作)
      - [过期策略](#过期策略)
      - [事务支持](#事务支持)
      - [乐观锁](#乐观锁)
  - [命令支持一览](#命令支持一览)
  - [贡献](#贡献)
  - [许可证](#许可证)

---

## 什么是 Crab-Cage?

`Crab-Cage` 是一个用 Rust 从零实现的轻量级, 个人学习性质的内存数据缓存，兼容 Redis 协议（RESP），支持持久化（AOF/RDB）, 多种数据类型与事务支持。

仅供学习与实验，不适合生产环境。

---

## 为什么开发 Crab-Cage?

- 个人基于学习的深入实践 Rust 核心特性：所有权, 借用, 异步, 零拷贝等  
- 理解 Redis 内部实现：RESP 解析, 命令分发, 数据结构, 持久化与恢复  
- 探索分布式缓存基础：事务与原子操作, 多实例部署  

---

## 文件架构
```
D:.
|   .gitignore
|   appendonly.aof
|   Cargo.lock
|   Cargo.toml
|   config.json
|   dump.rdb
|   README.md
|   rustfmt.toml
\---src
    |   config.rs # 配置模块
    |   expire.rs # 过期策略
    |   lib.rs # 库
    |   main.rs # 主程序
    |   persistence.rs # 持久化模块
    |   server.rs # 服务模块
    |
    +---engine
    |       kv.rs # 统一普通 Db 与事务上下文的最小 KV 抽象
    |       mod.rs # 引擎模块，接受命令并且调用子模块
    |       watch.rs # WATCH 机制
    |
    +---txn
    |       executor.rs # 事务执行器
    |       mod.rs
    |       session.rs # 事务会话模块
    |
    \---types
            hash.rs # 哈希类型支持
            list.rs # 列表类型支持
            mod.rs 
            set.rs # 集合类型支持
            string.rs # 基础字符类型支持
```

---

## 特性

- RESP 协议 TCP 服务器（默认端口 **6380**，避免与 Redis 冲突）  
- 多种数据类型：  
  - String: `GET`, `SET`, `DEL`, `INCR`, `DECR`
  - Hash:  `HSET`, `HGET`, `HDEL`, `HKEYS`, `HVALS`, `HGETALL`  
  - List:  `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`  
  - Set:   `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`  
  - Expire: `EXPIRE`, `TTL`, `PERSIST`
  - Others: `PING`, `QUIT`  
- 持久化：AOF（Append-Only File）与 RDB（快照）  
- 事务支持：
  - 基础事务操作：`MULTI`, `DISCARD`, `EXEC`
  - 乐观锁操作：`WATCH`,`UNWATCH`
  - 支持失败回滚 

---

## 快速开始

### 前提

- Rust 1.70+  
- Cargo 工具链  
- （可选）`redis-cli` 用于测试  

### 构建与运行

```bash
git clone https://github.com/MasterHesse/rudis.git
cd rudis

# Release 模式构建并启动
cargo build --release
cargo run --release
```

默认监听 `127.0.0.1:6380`，持久化文件 `appendonly.aof`/`dump.rdb`。

### 使用示例
#### 连接到 rudis 服务
```bash
# 建议另开终端，用 redis-cli 测试
redis-cli -h 127.0.0.1 -p 6380
```

#### String 数据类型操作
```bash
# --- String ---
127.0.0.1:6380> SET user:1 Alice
OK
127.0.0.1:6380> GET user:1
Alice
127.0.0.1:6380> DEL user:1
OK
127.0.0.1:6380> GET user:1
(error) ERR key not found
127.0.0.1:6380> SET TEN 10
OK
127.0.0.1:6380> INCR TEN
11
127.0.0.1:6380> DECR TEN
10
```

---

#### Hash 数据类型操作
```bash
# --- Hash ---
127.0.0.1:6380> HSET profile name Alice
1
127.0.0.1:6380> HSET profile age 30
1
127.0.0.1:6380> HGET profile name
Alice
127.0.0.1:6380> HKEYS profile
age,name
127.0.0.1:6380> HVALS profile
30,Alice
127.0.0.1:6380> HGETALL profile
age,30,name,Alice
```

---

#### List 数据类型操作
```bash
# --- List ---
127.0.0.1:6380> LPUSH mylist a
1
127.0.0.1:6380> LPUSH mylist b
2
127.0.0.1:6380> RPUSH mylist c
3
127.0.0.1:6380> LRANGE mylist 0 -1
b,a,c
127.0.0.1:6380> LPOP mylist
b
127.0.0.1:6380> RPOP mylist
c
```

---

#### Set 数据类型操作
```bash
# --- Set ---
127.0.0.1:6380> SADD myset x
1
127.0.0.1:6380> SADD myset y
1
127.0.0.1:6380> SADD myset x
0
127.0.0.1:6380> SMEMBERS myset
x,y
127.0.0.1:6380> SISMEMBER myset y
1
127.0.0.1:6380> SREM myset x
1
```

---

#### 过期策略
```bash
# --- 过期策略 ---
127.0.0.1:6380> SET temp hello
OK
127.0.0.1:6380> TTL temp
-1
127.0.0.1:6380> EXPIRE temp 500
1
127.0.0.1:6380> TTL temp
494
127.0.0.1:6380> PERSIST temp
1
127.0.0.1:6380> TTL temp
-1
```

---

#### 事务支持
```bash
127.0.0.1:6380> DISCARD
(error) ERR DISCARD without MULTI
127.0.0.1:6380> EXEC
(error) ERR EXEC without MULTI
127.0.0.1:6380> MULTI
OK
127.0.0.1:6380> SET LOVE YOU
QUEUED
127.0.0.1:6380> DISCARD
OK
127.0.0.1:6380> GET LOVE
(error) ERR key not found
127.0.0.1:6380> MULTI
OK
127.0.0.1:6380> SET LOVE YOU
QUEUED
127.0.0.1:6380> EXEC
OK
127.0.0.1:6380> GET LOVE
YOU
```
---

#### 乐观锁
客户端 1
```bash
redis-cli -p 6380

127.0.0.1:6380> SET balance 100
OK
127.0.0.1:6380> WATCH balance
OK
127.0.0.1:6380> MULTI
OK
127.0.0.1:6380> INCR balance
QUEUED
```

客户端 2
```bash
redis-cli -p 6380

127.0.0.1:6380> INCR balance
101
```

客户端 1
```bash
127.0.0.1:6380> EXEC
nil  # 监视的key改变，事务执行失败
```

---

## 命令支持一览

| 类型   | 命令                                      |
|------|-----------------------------------------   |
| String | GET, SET, DEL, INCR, DECR                |
| Hash   | HSET, HGET, HDEL, HKEYS, HVALS, HGETALL  |
| List   | LPUSH, RPUSH, LPOP, RPOP, LRANGE         |
| Set    | SADD, SREM, SMEMBERS, SISMEMBER          |
| Expire | EXPIRE, TTL, PERSIST                     |
| Transaction | MULTI, DISCARD, EXEC                |
|WATCH   | WATCH, UNWATCH                           |
|Others   | PING, QUIT                           |

---

## 贡献

非常欢迎 Issue, PR 与讨论。  
本项目以学习为主，优先「简单, 可读」的实现。

---

## 许可证

双重许可证：MIT 或 Apache-2.0 