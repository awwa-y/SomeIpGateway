# SomeIpGateway — SOME/IP 风格车云通信 epoll 网关

[![build-and-bench](https://github.com/awwa-y/SomeIpGateway/actions/workflows/ci.yml/badge.svg)](https://github.com/awwa-y/SomeIpGateway/actions/workflows/ci.yml)

将传统"无差别TCP广播器"升级为具备**服务注册发现 + 点对点RPC + 心跳保活 + 持久化消息队列**的轻量级车云通信网关，模拟 SOME/IP (Service-Oriented MiddlewarE over IP) 核心机制。

> 本仓库与 `VehicleIpcBus` 配套使用：**车内走 UDS IPC（VehicleIpcBus），车云通过 TCP 走 SomeIpGateway**，
> 形成完整"车内+车外"智能汽车通信链路参考实现。

---

## ✨ 特性

- **SOME/IP 风格二进制协议头**：25字节 Header，`static_assert` 防协议漂移（与 VehicleIpcBus 保持字段级一致）
- **epoll ET + 非阻塞 IO**：读循环 drain 到 EAGAIN，accept 循环收干，SIGPIPE 免疫
- **每连接收发缓冲 + 半包状态机**：任意切割的字节流正确重组消息，逐字节慢速发送已验证
- **短写处理 + EPOLLOUT 按需开关 + 4MB 背压水位线**：写不完自动排队，慢客户端积压超限主动踢除
- **服务注册发现路由**：`ServiceID → ProviderFD` 映射，精准转发（告别无差别广播）
- **Request-Response RPC 配对**：`client_id` 匹配闭环；连接断开自动清理悬挂配对（防 fd 复用错发）
- **心跳保活 + 过期剔除**：Provider 超时无心跳从路由表移除并断连
- **离线消息持久化**（append-only 文件）：目标服务宕机时消息落盘，恢复后重放补发
- **writev 合并发送**：Header + payload 单次 syscall
- Makefile 构建（gcc / -Wall -Wextra 零警告目标）

---

## 📊 压测报告（CI 实测）

环境：GitHub Actions `ubuntu-latest`（2 核共享 runner）/ gcc / loopback TCP / 单线程事件循环

每次提交自动构建并跑压测，数据可在 [Actions 记录](https://github.com/awwa-y/SomeIpGateway/actions) 中复现。

| 指标 | 数值 |
|---|---|
| 压测命令 | `./build/bench_client 127.0.0.1 9091 100 1000` |
| 压测规模 | 100 并发连接 × 1000 请求 = 100,000 请求 |
| 成功数 | 99,886 / 100,000 = **99.89%** |
| 耗时 | 29.215 s |
| 吞吐 | **3,419 req/s** |
| 平均延迟 | **2.205 ms** |

压测期间网关侧统计：`kicked_slow=32`（背压水位线真实踢除慢客户端）、`[sweep] ... stale, close`（心跳超时清理），说明背压与心跳剔除机制在压力下实际生效，而非仅存在于代码中。

> 单线程实现，追求正确性与可读性；多线程主从 Reactor 见 Roadmap。

---

## 🗺 Roadmap（计划中，非当前能力）

- [ ] timerfd 替代 epoll_wait 超时轮询做心跳扫描
- [ ] /stats HTTP 接口（当前为 stdout 每秒打印）
- [ ] NOTIFY 订阅表广播
- [ ] tests/ 单元测试（ServiceRegistry 优先）
- [ ] 协议多字节字段 htonl/ntohl 大端化（当前依赖同构主机）
- [ ] 多线程主从 Reactor / SO_REUSEPORT
- [ ] TLS / 鉴权

---

## 🏗 拓扑

```
            ┌─────────────────────┐
            │  SomeIpGateway      │
 车端────────│   0.0.0.0:9090     │────────云端
 (多客户端)  │  ┌───────────────┐  │  (调度/OTA)
            │  │ServiceRegistry│  │
            │  │ svc_id → fd   │  │
            │  └───────────────┘  │
            │  ┌───────────────┐  │
            │  │PendingRPCMap  │  │
            │  │ client_id → fd│  │
            │  └───────────────┘  │
            │  ┌───────────────┐  │
            │  │ PersistQueue  │  │
            │  │ append-only   │  │
            │  └───────────────┘  │
            └─────────────────────┘
                    ▲
                    │ VehicleIpcBus State_Manager 可通过本地 TCP/localhost 接入
```

---

## 📁 目录

```
SomeIpGateway/
├── README.md
├── Makefile               # 默认构建
├── CMakeLists.txt         # 可选
├── include/
│   ├── protocol.h         # 25B 协议头 + static_assert（C/C++ 双端同步修改）
│   ├── ServiceRegistry.hpp  # 核心：svc_id→provider 路由 + 心跳
│   └── PersistQueue.h     # 离线持久化队列
├── src/
│   ├── PersistQueue.cpp
│   └── main.cpp           # epoll 主循环 + 收发缓冲 + 路由/配对/持久化（改动清单见文件头注释）
└── scripts/
    └── bench_client.c     # 压测小工具（N 连接 × M 请求，打印 QPS/延迟）
```

---

## 🚀 快速开始

```bash
make
# 终端1：启动网关
./build/someip_gateway --port 9090 --hb 5000   # 端口9090，心跳超时5秒
# 终端2：压测（100 连接 × 1000 请求）
./build/bench_client 127.0.0.1 9090 100 1000
```

---

## 🔗 相关仓库

| 仓库 | 说明 |
|---|---|
| VehicleIpcBus | 车内 UDS 微服务通信中间件 |
| CabPad-Refactor-Doc | 架构升级设计文档（车内4节点+车云拓扑） |
