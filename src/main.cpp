// ============================================================================
// SomeIpGateway main — Phase 2：半包状态机 + ET + EPOLLOUT 背压
// 本轮已修复/实现：
//   [FIX] SIGPIPE 忽略（写已关闭socket默认杀死进程）
//   [FIX] 每连接接收缓冲 + 解析状态机（非阻塞 read 读到一半 EAGAIN 不再丢字节）
//   [FIX] 短写处理：写不完进发送缓冲，按需打开 EPOLLOUT，写完即关
//   [FIX] delFd 时清理 g_pending 悬挂项（fd 复用后响应可能错发）
//   [FIX] payload 长度上限校验（恶意/错误的 length 字段直接断开）
//   [FIX] packed Header 用 memcpy 解析（未对齐访问在 ARM 上可能崩溃）
//   [OPT] LT → ET（读循环 drain 到 EAGAIN；accept 循环收干）
//   [OPT] writev 合并 Header+payload 一次 syscall
//   [OPT] 背压水位线：发送缓冲超过 4MB 踢掉慢客户端（防慢消费者拖垮网关）
// TODO 留给你手搓（按优先级）：
//   1) timerfd 替代 epoll_wait 超时做心跳 sweep（把定时器变成 fd 融入事件循环）
//   2) /stats HTTP mini-parser（当前 statsThread 打印到 stdout）
//   3) NOTIFY 订阅表广播（当前 NOTIFY 是空实现）
//   4) tests/ 目录补 gtest 单元测试（ServiceRegistry 最适合先写）
// ============================================================================
#include "protocol.h"
#include "ServiceRegistry.hpp"
#include "PersistQueue.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <atomic>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <mutex>

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/uio.h>      // writev
#include <netinet/in.h>
#include <arpa/inet.h>

using namespace sip;

// ---- 协议与背压常量 --------------------------------------------------------
static constexpr uint32_t kMaxPayload = 1u << 20;   // 1MB，超过视为恶意/错误帧
static constexpr size_t   kMaxOutBuf  = 4u << 20;   // 4MB 发送缓冲水位线

// ---- epoll 事件组合 --------------------------------------------------------
static constexpr uint32_t kEvRead  = EPOLLIN | EPOLLRDHUP | EPOLLET;
static constexpr uint32_t kEvWrite = kEvRead | EPOLLOUT;

static std::atomic<bool> g_running{true};
static void on_sig(int) { g_running = false; }

static int setNonBlock(int fd) {
    int f = fcntl(fd, F_GETFL, 0);
    if (f < 0) return -1;
    return fcntl(fd, F_SETFL, f | O_NONBLOCK);
}

// ------------- 简单统计 -----------------------------------------------------
struct Stats {
    std::atomic<uint64_t> qps_total{0};
    std::atomic<uint64_t> qps_window{0};   // 每1秒清零的窗口
    std::atomic<uint32_t> connections{0};
    std::atomic<uint64_t> kicked_slow{0};  // 被背压踢掉的慢客户端数
} g_stats;

static ServiceRegistry* g_services = nullptr;
static PersistQueue*    g_queue    = nullptr;
static int              g_epfd     = -1;   // handleMsg 里注册 EPOLLOUT 要用

// client_id → original client fd（Response 原路返回）
static std::mutex g_pending_mtx;
static std::unordered_map<uint32_t, int> g_pending;

// ------------- 连接对象：收/发缓冲（半包与短写的核心）-------------------------
struct Conn {
    int                 fd = -1;
    std::vector<uint8_t> in;    // 接收缓冲：字节流攒消息（半包状态机的仓库）
    std::vector<uint8_t> out;   // 发送缓冲：短写暂存 + 排队保序
    size_t              outOff = 0;
};

// 主事件循环单线程访问 in/out，map 本身加锁保护 add/del 与 stats 并发
static std::unordered_map<int, Conn> g_conns;
static std::mutex g_conns_mtx;

static Conn* getConn(int fd) {
    // 仅在主循环线程调用；不返回跨线程持有
    auto it = g_conns.find(fd);
    return it == g_conns.end() ? nullptr : &it->second;
}

static void modEvents(int epfd, int fd, uint32_t ev) {
    epoll_event e{};
    e.events = ev;
    e.data.fd = fd;
    ::epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}

// -----------------------------------------------------------------------------
// 发送路径：先尝试 writev 直写，写不完的进 out 缓冲并打开 EPOLLOUT
// 关键认知：
//   1) 非阻塞 write 可能只写入一部分（内核发送缓冲满），返回值 != 请求长度不是错误
//   2) EPOLLOUT 平时绝不注册（几乎总是可写，注册了会 busy-loop 烧 CPU）
// -----------------------------------------------------------------------------
static void kickSlow(int fd, const char* why) {
    g_stats.kicked_slow.fetch_add(1);
    std::printf("[backpressure] kick fd=%d: %s\n", fd, why);
    // delFd 在下方定义；这里通过再次声明调用
    extern void gatewayDelFd(int fd);
    gatewayDelFd(fd);
}

static void sendRaw(int fd, const Header& h, const uint8_t* payload, uint32_t plen) {
    Conn* c = getConn(fd);
    if (!c) return;

    const uint8_t* hbuf = reinterpret_cast<const uint8_t*>(&h);
    const size_t   total = kHeaderSize + plen;

    if (c->out.empty() && c->outOff == 0) {
        // 无积压：writev 一次 syscall 发 Header+payload
        struct iovec iov[2] = {
            { const_cast<uint8_t*>(hbuf),     kHeaderSize },
            { const_cast<uint8_t*>(payload),  plen },
        };
        ssize_t n = ::writev(fd, iov, plen > 0 ? 2 : 1);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                n = 0;                      // 一字节都没写进去，全部排队
            } else {
                kickSlow(fd, "write error");
                return;
            }
        }
        size_t sent = (size_t)n;
        if (sent == total) return;          // 一次写完，最常见路径

        // 短写：把剩余字节搬进发送缓冲
        c->out.reserve(total - sent);
        if (sent < kHeaderSize) {
            c->out.insert(c->out.end(), hbuf + sent, hbuf + kHeaderSize);
            if (plen) c->out.insert(c->out.end(), payload, payload + plen);
        } else {
            size_t skip = sent - kHeaderSize;
            if (plen) c->out.insert(c->out.end(), payload + skip, payload + plen);
        }
    } else {
        // 已有积压：新消息直接排到尾部（保证 TCP 字节序不被打乱）
        c->out.insert(c->out.end(), hbuf, hbuf + kHeaderSize);
        if (plen) c->out.insert(c->out.end(), payload, payload + plen);
    }

    // 背压水位线：慢客户端积压超限直接踢掉，保护网关自身内存
    if (c->out.size() - c->outOff > kMaxOutBuf) {
        kickSlow(fd, "send buffer overflow");
        return;
    }
    // 打开写关注，等内核发送缓冲腾出空间
    modEvents(g_epfd, fd, kEvWrite);
}

// EPOLLOUT 到来：尽量把积压冲出去，冲完立刻关掉写关注
static void onWritable(int fd) {
    Conn* c = getConn(fd);
    if (!c) return;
    while (c->outOff < c->out.size()) {
        ssize_t n = ::write(fd, c->out.data() + c->outOff, c->out.size() - c->outOff);
        if (n > 0) {
            c->outOff += (size_t)n;
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return;                         // 还没冲完，继续等下一次 EPOLLOUT
        } else {
            kickSlow(fd, "flush write error");
            return;
        }
    }
    // 冲干净了：复位缓冲并关掉 EPOLLOUT（按需开关）
    c->out.clear();
    c->outOff = 0;
    modEvents(g_epfd, fd, kEvRead);
}

// -----------------------------------------------------------------------------
// 接收路径（ET 模式）：循环 read 到 EAGAIN 掏干，全部先进 in 缓冲，
// 再由状态机按 [Header|Payload] 切消息。字节流没有消息边界，这是唯一正确姿势。
// 注意：handleMsg 内部可能断开当前连接（如回 ACK 失败），所以每轮循环
// 都要重新 getConn()，绝不能长期持有 Conn* —— 否则就是 use-after-free。
// -----------------------------------------------------------------------------
static void handleMsg(int fd, const Header& h, const uint8_t* payload);

static void processInBuf(int fd) {
    size_t off = 0;

    while (true) {
        Conn* c = getConn(fd);                     // 每轮重取：上一条消息处理中连接可能已被踢
        if (!c) return;
        auto& buf = c->in;
        if (buf.size() - off < kHeaderSize) break; // 连 Header 都不齐

        Header h;
        std::memcpy(&h, buf.data() + off, kHeaderSize);   // packed 结构体用 memcpy，避免未对齐访问

        if (h.magic != kMagic) {
            std::printf("[proto] fd=%d bad magic, drop conn\n", fd);
            kickSlow(fd, "bad magic");             // 流已错位，只能断连重来
            return;
        }
        if (h.length > kMaxPayload) {
            std::printf("[proto] fd=%d length=%u too large, drop conn\n", fd, h.length);
            kickSlow(fd, "oversized length");
            return;
        }
        if (buf.size() - off < kHeaderSize + h.length) {
            break;                                 // 半包：payload 未到齐，留在缓冲等下次
        }
        handleMsg(fd, h, buf.data() + off + kHeaderSize);
        off += kHeaderSize + h.length;
    }

    // 消费掉已处理的前缀（O(n) 搬移；工业级会用环形缓冲/读下标，教学版从简）
    Conn* c = getConn(fd);
    if (c && off > 0) c->in.erase(c->in.begin(), c->in.begin() + (long)off);
}

static void onReadable(int fd) {
    Conn* c = getConn(fd);
    if (!c) return;
    uint8_t tmp[65536];

    // ET 只通知一次：必须 drain 到 EAGAIN，否则剩余数据永远没有下次通知
    while (true) {
        ssize_t n = ::read(fd, tmp, sizeof(tmp));
        if (n > 0) {
            c->in.insert(c->in.end(), tmp, tmp + n);
        } else if (n == 0) {
            // 对端关闭(EOF)：必须有这个分支，否则会把它当错误无限重试
            kickSlow(fd, "peer closed");
            return;
        } else {
            if (errno == EINTR)  continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;   // 掏干了，正常结束
            kickSlow(fd, "read error");
            return;
        }
    }
    processInBuf(fd);
}

// -----------------------------------------------------------------------------
// 消息处理（业务路由，与 Phase 1 逻辑一致）
// -----------------------------------------------------------------------------
static void handleMsg(int fd, const Header& h, const uint8_t* payload) {
    g_stats.qps_window.fetch_add(1);

    // ---- HEARTBEAT
    if (h.msg_type == MsgType::HEARTBEAT) {
        g_services->touchHeartbeat(fd);
        return;
    }
    // ---- REGISTER (payload = service_id 4B BE)
    if (h.msg_type == MsgType::REGISTER && h.length == 4) {
        uint32_t svc =
            ((uint32_t)payload[0]<<24)|((uint32_t)payload[1]<<16)|
            ((uint32_t)payload[2]<<8)|(uint32_t)payload[3];
        g_services->addProvider(svc, fd);
        // REGISTER 成功后重放持久化队列
        auto drain = g_queue->drainFor(svc);
        for (auto& m : drain) {
            Header req{};
            req.magic      = kMagic;
            req.service_id = m.service_id;
            req.method_id  = m.method_id;
            req.client_id  = m.client_id;
            req.length     = (uint32_t)m.payload.size();
            req.msg_type   = MsgType::REQUEST;
            { std::lock_guard<std::mutex> lk(g_pending_mtx);
                g_pending[req.client_id] = (int)m.original_client_fd; }
            sendRaw(fd, req, m.payload.data(), (uint32_t)m.payload.size());
        }
        Header ack = h;
        ack.msg_type   = MsgType::RESPONSE;
        ack.length     = 0;
        ack.error_code = ErrorCode::OK;
        sendRaw(fd, ack, nullptr, 0);
        return;
    }
    // ---- REQUEST
    if (h.msg_type == MsgType::REQUEST) {
        int provider = g_services->routeProvider(h.service_id);
        if (provider < 0) {
            // Provider 离线 → 入持久化队列
            QueuedMsg qm{};
            qm.original_client_fd = (uint32_t)fd;
            qm.service_id = h.service_id;
            qm.method_id  = h.method_id;
            qm.client_id  = h.client_id;
            qm.payload.assign(payload, payload + h.length);
            g_queue->enqueue(h.service_id, qm);
            Header rsp = h;
            rsp.msg_type   = MsgType::RESPONSE;
            rsp.length     = 0;
            rsp.error_code = ErrorCode::NOT_READY;
            sendRaw(fd, rsp, nullptr, 0);
            return;
        }
        { std::lock_guard<std::mutex> lk(g_pending_mtx);
            g_pending[h.client_id] = fd; }
        sendRaw(provider, h, payload, h.length);
        return;
    }
    // ---- RESPONSE
    if (h.msg_type == MsgType::RESPONSE) {
        int origin = -1;
        { std::lock_guard<std::mutex> lk(g_pending_mtx);
            auto it = g_pending.find(h.client_id);
            if (it != g_pending.end()) {
                origin = it->second;
                g_pending.erase(it);
            }
        }
        if (origin > 0) sendRaw(origin, h, payload, h.length);
        return;
    }
    // ---- NOTIFY：TODO 订阅表广播（见文件头 TODO#3）
    (void)payload;
}

// -----------------------------------------------------------------------------
// 连接表维护
// -----------------------------------------------------------------------------
static void addFd(int fd) {
    std::lock_guard<std::mutex> lk(g_conns_mtx);
    Conn& c = g_conns[fd];
    c.fd = fd;
    c.in.reserve(4096);
    c.out.reserve(4096);
    g_stats.connections.fetch_add(1);
}

void gatewayDelFd(int fd) {
    ::epoll_ctl(g_epfd, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    {
        std::lock_guard<std::mutex> lk(g_conns_mtx);
        g_conns.erase(fd);
    }
    // [FIX] 该 fd 上所有挂起的 RPC 配对一并清除：
    // 否则 client_id 悬空映射，将来 fd 号被复用时响应会错发到别人的连接
    {
        std::lock_guard<std::mutex> lk(g_pending_mtx);
        for (auto it = g_pending.begin(); it != g_pending.end(); ) {
            if (it->second == fd) it = g_pending.erase(it);
            else ++it;
        }
    }
    g_services->removeFd(fd);
    g_stats.connections.fetch_sub(1);
}

static void statsThreadFunc() {
    while (g_running) {
        sleep(1);
        uint64_t w = g_stats.qps_window.exchange(0);
        g_stats.qps_total += w;
        std::printf("[stats] conns=%u cur_qps=%lu total=%lu reg_svcs=%zu kicked_slow=%lu\n",
            g_stats.connections.load(), w, g_stats.qps_total.load(),
            g_services ? g_services->numServices() : 0,
            g_stats.kicked_slow.load());
    }
}

// ------------- main ---------------------------------------------------------
int main(int argc, char** argv) {
    uint16_t port         = 9090;
    uint64_t hb_timeout_ms = 10000;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--port" && i+1 < argc) port = (uint16_t)atoi(argv[++i]);
        else if (a == "--hb"   && i+1 < argc) hb_timeout_ms = (uint64_t)atoi(argv[++i]);
    }

    // [FIX] 忽略 SIGPIPE：写已关闭的 socket 内核默认发 SIGPIPE 杀死进程，
    // 网关类程序必须忽略它，靠 write 返回 -1/errno=EPIPE 走正常错误分支
    ::signal(SIGPIPE, SIG_IGN);

    ServiceRegistry registry;
    PersistQueue    queue("./queue_data");
    g_services = &registry;
    g_queue    = &queue;

    // 监听
    int lsock = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1; ::setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setNonBlock(lsock);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(lsock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::perror("bind"); return 1;
    }
    ::listen(lsock, 2048);

    g_epfd = ::epoll_create1(0);
    epoll_event ev{};
    ev.events = kEvRead;                   // ET
    ev.data.fd = lsock;
    ::epoll_ctl(g_epfd, EPOLL_CTL_ADD, lsock, &ev);

    struct sigaction sa{}; sa.sa_handler = on_sig;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,&sa,nullptr);
    sigaction(SIGTERM,&sa,nullptr);

    std::thread statThread(statsThreadFunc);

    std::printf("[SomeIpGateway] listening on 0.0.0.0:%u  hb=%lu ms (ET mode)\n",
                port, hb_timeout_ms);

    // sweep 计时（TODO#1: 改用 timerfd 融入事件循环后可去掉这里的轮询）
    uint64_t last_sweep_ms = 0;
    auto nowMs = []{
        using namespace std::chrono;
        return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    };

    const int MAXEV = 256;
    epoll_event events[MAXEV];

    while (g_running) {
        int n = ::epoll_wait(g_epfd, events, MAXEV, 300);
        // 心跳扫描
        uint64_t now = nowMs();
        if (now - last_sweep_ms >= 5000) {
            last_sweep_ms = now;
            auto dead = registry.sweepStale(hb_timeout_ms);
            for (int dfd : dead) {
                std::printf("[sweep] fd=%d stale, close\n", dfd);
                gatewayDelFd(dfd);
            }
        }
        if (n <= 0) continue;

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;

            if (fd == lsock) {
                // ET 下 accept 也必须循环收干，直到 EAGAIN
                while (true) {
                    sockaddr_in caddr{}; socklen_t clen = sizeof(caddr);
                    int c = ::accept(lsock, (sockaddr*)&caddr, &clen);
                    if (c < 0) {
                        // TODO: EMFILE（fd耗尽）时会在这里 busy-loop，
                        //       预留空闲fd法处理——压测到高连接数时你会亲眼见到
                        break;
                    }
                    setNonBlock(c);
                    epoll_event cev{};
                    cev.events = kEvRead;
                    cev.data.fd = c;
                    ::epoll_ctl(g_epfd, EPOLL_CTL_ADD, c, &cev);
                    addFd(c);
                }
                continue;
            }

            uint32_t ee = events[i].events;
            if (ee & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
                gatewayDelFd(fd);
                continue;
            }
            // 先读后写：同一轮事件里读产生的响应，可能紧接着就要冲发送缓冲
            if (ee & EPOLLIN)  onReadable(fd);
            if (ee & EPOLLOUT) onWritable(fd);
        }
    }

    ::close(g_epfd);
    ::close(lsock);
    statThread.join();
    return 0;
}
