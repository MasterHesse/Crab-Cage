# rudis

[![Crates.io](https://img.shields.io/crates/v/rudis)](https://crates.io/crates/rudis) [![Rust](https://img.shields.io/badge/rust-1.70+-orange)](https://www.rust-lang.org/) [![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](LICENSE)

一个用 Rust 实现的**个人学习版 Redis-like 分布式缓存**。

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
  - [贡献](#贡献)
  - [许可证](#许可证)

---

## 什么是 rudis?

`rudis` 是一个用 Rust 从零实现的轻量级、个人学习性质的内存数据缓存，支持 Redis 协议（RESP）、AOF 持久化，以及简单的分片分布。

> **注意**：本项目仅用于学习和实验，不适合生产环境。  
> 探索内容包括：
> - Rust 异步生态（Tokio、async/await）  
> - RESP 协议解析与序列化  
> - 内存数据结构（String、List、Hash、Set）  
> - AOF 持久化与恢复  
> - 基础分片分布式缓存  

---

## 为什么开发 rudis?

- **学习 Rust & Tokio**  
  个人为了深入实践所有权、`async/await`、零拷贝等Rust核心特性。  
- **理解 Redis 内部**  
  手写 RESP 解析器、命令调度、数据类型实现。  
- **探索持久化**  
  实现 AOF 持久化、启动时重放、文件滚动等。  
- **尝试分布式**  
  构建简单的哈希槽分片层，掌握分布式缓存基本原理。  

---

## 特性

- RESP 协议 TCP 服务器（默认端口 6380，防止与Redis默认端口冲突）  
- 支持命令：  
  - 字符串：`GET`, `SET`, `DEL`, `INCR`  
- AOF 持久化与重放  
- RDB 持久化与重放 
- 简易分片：将键映射到不同节点  
- 零外部依赖（除 Tokio、env_logger、serde 等）  

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
cargo build --release
```

创建配置文件（参考 `examples/config.toml`），然后：

```bash
# 单节点启动
cargo run --release

# 多节点分片还在开发中
```

---

## 使用示例

使用 `redis-cli` 连接（默认端口 6380）：

```bash
redis-cli -p 6380
> SET user:1 "Alice"
OK
> GET user:1
"Alice"
> DEL user:1
OK
> GET user:1
(error) ERR key not found
```

---

## 贡献

本项目以学习为主，但欢迎：

- 提交 Issue 讨论问题或需求  
- Fork 并提交 PR 改进功能  
- 分享新的实验想法与实践经验  

请保持项目的学习性质：优先简单可理解的实现。

---

## 许可证

本项目采用 **MIT** 或 **Apache-2.0** 双重许可。详见 [LICENSE](LICENSE) 文件。  
