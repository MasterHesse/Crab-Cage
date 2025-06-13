# rudis

[![Crates.io](https://img.shields.io/crates/v/rudis)](https://crates.io/crates/rudis) [![Rust](https://img.shields.io/badge/rust-1.70+-orange)](https://www.rust-lang.org/) [![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE)

一个用 Rust 实现的，个人学习版的 Redis-like 内存数据缓存。

---

## 目录

- [rudis](#rudis)
  - [目录](#目录)
  - [什么是 rudis?](#什么是-rudis)
  - [为什么开发 rudis?](#为什么开发-rudis)
  - [特性](#特性)
  - [快速开始](#快速开始)
    - [前提](#前提)
    - [构建与运行](#构建与运行)
    - [使用示例](#使用示例)
  - [命令支持一览](#命令支持一览)
  - [贡献](#贡献)
  - [许可证](#许可证)

---

## 什么是 rudis?

`rudis` 是一个用 Rust 从零实现的轻量级、个人学习性质的内存数据缓存，兼容 Redis 协议（RESP），支持持久化（AOF/RDB）、多种数据类型与简易分片。仅供学习与实验，不适合生产环境。

---

## 为什么开发 rudis?

- 深入实践 Rust 核心特性：所有权、借用、异步（Tokio/async-await）、零拷贝等  
- 理解 Redis 内部实现：RESP 解析、命令分发、数据结构、持久化与恢复  
- 探索分布式缓存基础：哈希槽分片、多实例部署  

---

## 特性

- RESP 协议 TCP 服务器（默认端口 **6380**，避免与 Redis 冲突）  
- 多种数据类型：  
  - String: `GET`、`SET`、`DEL`  
  - Hash:  `HSET`、`HGET`、`HDEL`、`HKEYS`、`HVALS`、`HGETALL`  
  - List:  `LPUSH`、`RPUSH`、`LPOP`、`RPOP`、`LRANGE`  
  - Set:   `SADD`、`SREM`、`SMEMBERS`、`SISMEMBER`  
- 过期策略：  
  - `EXPIRE key seconds`：为 key 设置过期时间（单位：秒），返回 `1`（设置成功）或 `0`（key 不存在）  
  - `TTL key`：查询 key 剩余生存时间，返回秒数、`-2`（key 不存在）、`-1`（key 无过期）  
  - `PERSIST key`：移除 key 的过期时间，返回 `1`（移除成功）或 `0`（key 不存在或无过期）  
  - 实现机制：惰性删除（访问时检测并删除过期 key），持久化时 AOF 中记录 `EXPIRE` 操作，重放时恢复过期元数据  
- 持久化：AOF（Append-Only File）与 RDB（快照）  
- 零外部依赖（除 Tokio、serde、sled 等） 

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

```bash
# 建议另开终端，用 redis-cli 测试
redis-cli -p 6380

# --- String ---
> SET user:1 Alice
OK
> GET user:1
Alice
> DEL user:1
OK
> GET user:1
ERR key not found

# --- Hash ---
> HSET profile name Alice
1
> HSET profile age 30
1
> HGET profile name
Alice
> HKEYS profile
name,age
> HVALS profile
Alice,30
> HGETALL profile
name,Alice,age,30

# --- List ---
> LPUSH mylist a
1
> LPUSH mylist b
2       # 列表现在 [b, a]
> RPUSH mylist c
3       # 列表现在 [b, a, c]
> LRANGE mylist 0 -1
b,a,c
> LPOP mylist
b
> RPOP mylist
c

# --- Set ---
> SADD myset x
1
> SADD myset y
1
> SADD myset x
0       # x 已存在
> SMEMBERS myset
x,y
> SISMEMBER myset y
1
> SREM myset x
1

# --- 过期策略 ---
> SET temp hello
OK
> TTL temp
-1      # 默认无过期
> EXPIRE temp 5
1       # 设置 5 秒后过期
> TTL temp
5
> PERSIST temp
1       # 取消过期
> TTL temp
-1
```

---

## 命令支持一览

| 类型   | 命令                                      |
|------|-----------------------------------------   |
| String | GET, SET, DEL                            |
| Hash   | HSET, HGET, HDEL, HKEYS, HVALS, HGETALL  |
| List   | LPUSH, RPUSH, LPOP, RPOP, LRANGE         |
| Set    | SADD, SREM, SMEMBERS, SISMEMBER          |
| Expire | EXPIRE, TTL, PERSIST                     |

---

## 贡献

非常欢迎 Issue、PR 与讨论。  
本项目以学习为主，优先「简单、可读」的实现。

---

## 许可证

双重许可证：MIT 或 Apache-2.0，详见 [LICENSE](LICENSE) 文件。  