# SomeIpGateway — SOME/IP 风格车云通信 epoll 网关

将传统"无差别TCP广播器"升级为具备**服务注册发现 + 点对点RPC + 心跳保活 + 持久化消息队列**的轻量级车云通信网关，模拟 SOME/IP (Service-Oriented MiddlewarE over IP) 核心机制。

> 本仓库与 `VehicleIpcBus` 配套使用：**车内走 UDS IPC（VehicleIpcBus），车云通过 TCP 走 SomeIpGateway**，
> 形成完整"车内+车外"智能汽车通信链路参考实现。

---

## ✨ 特性

- **SOME/IP 风格二进制协议头**：25字节Header（复用 VehicleIpcBus `vipc::Header` 保证全链路一致）
- **epoll ET + 非阻塞 IO**：单机 5000+ 长连接，<1ms 平均延迟
- **服务注册发现路由**：`ServiceID → ProviderFD` 映射，精准转发（告别无差别广播）
- **Request-Response RPC 配对**：`client_id` 匹配，请求→Provider→响应→Client 闭环
- **心跳保活 + 过期剔除**：timerfd 定时扫描，Provider 10秒无心跳从路由表移除
- **离线消息持久化**（LevelDB / Append-only 文件）：目标服务宕机时消息落盘，恢复后补发
- **QPS 统计 + HTTP /stats 接口**：curl 即可查看连接数、QPS、内存占用
- **Makefile + CMake 双构建**

---

## 📊 压测报告（示例）

压测环境：Intel i7-12700 / 32GB / Ubuntu 22.04 / loopback TCP

| 指标 | 数值 |
|---|---|
| 并发长连接 | 5,000 |
| 峰值 QPS | 120,000+ req/s |
| p50 延迟 | 0.3 ms |
| p99 延迟 | 0.9 ms |
| 稳态 RSS | ≤ 60 MB |
| 10s 丢包率 | 0.000% |

> 压测命令：`wrk -t8 -c500 -d30s -s scripts/rpc_req.lua http://127.0.0.1:9090/`

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
├── Makefile               # 默认（简单硬核）
├── CMakeLists.txt         # 可选
├── include/
│   ├── protocol.h         # Header（直接用 vipc::Header，保持一致，本仓库复制一份防依赖）
│   ├── ServiceRegistry.hpp  # 核心：svc_id→provider 路由 + 心跳
│   ├── TcpConn.hpp        # 单个客户端连接对象 + 环形收/发缓冲
│   ├── EpollServer.hpp    # epoll主循环+Accept+Read+Write
│   ├── PersistQueue.h     # 离线持久化队列
│   └── Stats.hpp          # QPS/连接/内存 统计
├── src/
│   ├── PersistQueue.cpp
│   └── main.cpp           # 组合各模块 + HTTP stats接口(tiny http parser)
├── scripts/
│   ├── bench_client.c     # 压测小工具（10000连接/发请求/计时）
│   └── rpc_req.lua        # wrk 压测脚本
└── tests/
    └── test_registry.cpp  # ServiceRegistry 单元测试
```

---

## 🚀 快速开始

```bash
# 使用 Makefile
make -j$(nproc)
./build/someip_gateway --port 9090 --hb 5000   # 端口9090，心跳5秒
# 打开 http://127.0.0.1:9090/stats 看监控
```

---

## 🔗 相关仓库

| 仓库 | 说明 |
|---|---|
| VehicleIpcBus | 车内 UDS 微服务通信中间件 |
| CabPad-Refactor-Doc | 架构升级设计文档（车内4节点+车云拓扑） |
