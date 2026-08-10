// ============================================================================
// SomeIpGateway main — Phase 1 可编译骨架
// TODO 你手搓实现：
//   1) epoll ET 模式（当前是LT，简单但性能稍低）
//   2) 每个TcpConn带环形缓冲区处理半包/粘包
//   3) /stats HTTP mini-parser（当前占位）
//   4) timerfd heartbeat sweep
//   5) PersistQueue 接入（路由NOT_FOUND时 enqueue；REGISTER 后 drain 重发）
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
#include <unordered_map>
#include <mutex>

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

using namespace sip;

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
    uint64_t              qps_last_print = 0;
} g_stats;

static void statsThreadFunc() {
    while (g_running) {
        sleep(1);
        uint64_t w = g_stats.qps_window.exchange(0);
        g_stats.qps_total += w;
        std::printf("[stats] conns=%u cur_qps=%lu total=%lu reg_svcs=%zu\n",
            g_stats.connections.load(), w, g_stats.qps_total.load(),
            g_services ? g_services->numServices() : 0);
    }
}

// ------------- 全局对象（简化）-----------------------------------------------
static ServiceRegistry* g_services  = nullptr;
static PersistQueue*    g_queue     = nullptr;

// client_id → original client fd（Response 原路返回）
static std::mutex g_pending_mtx;
static std::unordered_map<uint32_t, int> g_pending;
static std::atomic<uint32_t> g_cid_seq{1};

static int recvFull(int fd, uint8_t* buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::read(fd, buf + got, len - got);
        if (n > 0) got += (size_t)n;
        else if (n == 0) return 0;
        else {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return -1;
            return -1;
        }
    }
    return (int)got;
}

static void forwardRaw(int fd, const Header& h, const uint8_t* payload) {
    // writev 可优化
    ::write(fd, &h, sizeof(h));
    if (h.length > 0) ::write(fd, payload, h.length);
}

static void handleMsg(int fd, const Header& h, const std::vector<uint8_t>& payload) {
    if (h.magic != kMagic) return;
    g_stats.qps_window.fetch_add(1);

    // ---- /stats HTTP（极简识别：GET /stats）
    // TODO: 真实实现需要解析首行，这里示意
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
            // 存 pending 以便响应回 original_client_fd
            { std::lock_guard<std::mutex> lk(g_pending_mtx);
                g_pending[req.client_id] = m.original_client_fd; }
            forwardRaw(fd, req, m.payload.data());
        }
        Header ack = h;
        ack.msg_type   = MsgType::RESPONSE;
        ack.length     = 0;
        ack.error_code = ErrorCode::OK;
        forwardRaw(fd, ack, nullptr);
        return;
    }
    // ---- REQUEST
    if (h.msg_type == MsgType::REQUEST) {
        int provider = g_services->routeProvider(h.service_id);
        if (provider < 0) {
            // Provider 离线 → 入持久化队列
            QueuedMsg qm{};
            qm.original_client_fd = fd;
            qm.service_id = h.service_id;
            qm.method_id  = h.method_id;
            qm.client_id  = h.client_id;
            qm.payload    = payload;
            g_queue->enqueue(h.service_id, qm);
            // 可选：立即回 ACCEPTED 告诉客户端"已排队稍后响应"
            Header rsp = h;
            rsp.msg_type   = MsgType::RESPONSE;
            rsp.length     = 0;
            rsp.error_code = ErrorCode::NOT_READY;
            forwardRaw(fd, rsp, nullptr);
            return;
        }
        { std::lock_guard<std::mutex> lk(g_pending_mtx);
            g_pending[h.client_id] = fd; }
        forwardRaw(provider, h, payload.empty() ? nullptr : payload.data());
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
        if (origin > 0) forwardRaw(origin, h, payload.empty()?nullptr:payload.data());
        return;
    }
    // ---- NOTIFY：广播给所有已注册订阅者（当前简化：发给除自己外所有连接；可扩展订阅表）
    // TODO: 此部分需维护 fd set 并遍历；这里留占位，避免 undefined 警告
    (void)fd;
}

// ------------- 连接表 -------------------------------------------------------
struct FdCtx {
    int fd = -1;
    // 预留：收/发环形缓冲区、半包处理
};
static std::unordered_map<int, FdCtx> g_fds;
static std::mutex g_fds_mtx;

static void addFd(int fd) {
    std::lock_guard<std::mutex> lk(g_fds_mtx);
    g_fds[fd] = FdCtx{fd};
    g_stats.connections.fetch_add(1);
}
static void delFd(int epfd, int fd) {
    ::epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    {
        std::lock_guard<std::mutex> lk(g_fds_mtx);
        auto it = g_fds.find(fd);
        if (it != g_fds.end()) g_fds.erase(it);
    }
    g_services->removeFd(fd);
    g_stats.connections.fetch_sub(1);
}

// ------------- main ---------------------------------------------------------
int main(int argc, char** argv) {
    uint16_t port      = 9090;
    uint64_t hb_timeout_ms = 10000;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--port" && i+1 < argc) port = (uint16_t)atoi(argv[++i]);
        else if (a == "--hb"   && i+1 < argc) hb_timeout_ms = (uint64_t)atoi(argv[++i]);
    }

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

    int epfd = ::epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = lsock;
    ::epoll_ctl(epfd, EPOLL_CTL_ADD, lsock, &ev);

    struct sigaction sa{}; sa.sa_handler = on_sig;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,&sa,nullptr);
    sigaction(SIGTERM,&sa,nullptr);

    // 统计线程
    std::thread statThread(statsThreadFunc);

    std::printf("[SomeIpGateway] listening on 0.0.0.0:%u  hb=%lu ms\n", port, hb_timeout_ms);

    // sweep 计时
    uint64_t last_sweep_ms = 0;
    auto nowMs = []{
        using namespace std::chrono;
        return (uint64_t)duration_cast<milliseconds>(
            steady_clock::now().time_since_epoch()).count();
    };

    const int MAXEV = 256;
    epoll_event events[MAXEV];
    std::vector<uint8_t> rbuf(kHeaderSize + 65536);

    while (g_running) {
        int n = ::epoll_wait(epfd, events, MAXEV, 300);
        // sweep
        uint64_t now = nowMs();
        if (now - last_sweep_ms >= 5000) {
            last_sweep_ms = now;
            auto dead = registry.sweepStale(hb_timeout_ms);
            for (int fd : dead) {
                std::printf("[sweep] fd=%d stale, close\n", fd);
                delFd(epfd, fd);
            }
        }
        if (n <= 0) continue;

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;

            if (fd == lsock) {
                // accept all
                while (true) {
                    sockaddr_in caddr{}; socklen_t clen = sizeof(caddr);
                    int c = ::accept(lsock, (sockaddr*)&caddr, &clen);
                    if (c < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) break;
                        break;
                    }
                    setNonBlock(c);
                    epoll_event cev{};
                    cev.events = EPOLLIN | EPOLLRDHUP;
                    cev.data.fd = c;
                    ::epoll_ctl(epfd, EPOLL_CTL_ADD, c, &cev);
                    addFd(c);
                }
                continue;
            }

            if (events[i].events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
                delFd(epfd, fd);
                continue;
            }
            if (!(events[i].events & EPOLLIN)) continue;

            // 读消息
            while (true) {
                int rh = recvFull(fd, rbuf.data(), kHeaderSize);
                if (rh == 0) { delFd(epfd, fd); break; }
                if (rh < 0) break;
                auto* h = reinterpret_cast<Header*>(rbuf.data());
                if (h->magic != kMagic) { delFd(epfd, fd); break; }
                std::vector<uint8_t> payload(h->length);
                if (h->length > 0) {
                    int rp = recvFull(fd, payload.data(), h->length);
                    if (rp <= 0) { delFd(epfd, fd); break; }
                }
                handleMsg(fd, *h, payload);
            }
        }
    }

    ::close(epfd);
    ::close(lsock);
    statThread.join();
    return 0;
}
