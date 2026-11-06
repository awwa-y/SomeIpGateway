#pragma once
// ============================================================================
// ServiceRegistry — 服务注册/路由表 + 心跳保活
// 线程安全（所有方法带 mtx）
// ============================================================================
#include "protocol.h"
#include <unordered_map>
#include <list>
#include <mutex>
#include <vector>
#include <cstdint>
#include <chrono>
#include <algorithm>   // sweepStale 里用了 std::sort / std::unique

namespace sip {

struct ProviderInfo {
    int       fd;
    uint64_t  last_hb_ms;
};

class ServiceRegistry {
public:
    // ---- Provider API ----
    void addProvider(uint32_t service_id, int fd) {
        std::lock_guard<std::mutex> lk(mtx_);
        routes_[service_id].push_back({fd, nowMs()});
        fd_to_services_[fd].push_back(service_id);
    }

    void touchHeartbeat(int fd) {
        uint64_t t = nowMs();
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = fd_to_services_.find(fd);
        if (it == fd_to_services_.end()) return;
        for (uint32_t svc : it->second) {
            auto& list = routes_[svc];
            for (auto& p : list) if (p.fd == fd) p.last_hb_ms = t;
        }
    }

    // 移除客户端（连接断开时清所有关联）
    void removeFd(int fd) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = fd_to_services_.find(fd);
        if (it == fd_to_services_.end()) return;
        for (uint32_t svc : it->second) {
            auto& l = routes_[svc];
            l.remove_if([fd](const ProviderInfo& p){ return p.fd == fd; });
            if (l.empty()) routes_.erase(svc);
        }
        fd_to_services_.erase(it);
    }

    // ---- Router API ----
    // 找 svc 的 provider。这里简单 round-robin：取第一个并挪到末尾（简易负载均衡）
    int routeProvider(uint32_t service_id) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = routes_.find(service_id);
        if (it == routes_.end() || it->second.empty()) return -1;
        int fd = it->second.front().fd;
        // round-robin: 头挪到尾
        it->second.splice(it->second.end(), it->second, it->second.begin());
        return fd;
    }

    // ---- Heartbeat Sweep ----
    // 返回被踢掉的 fd 列表（上层close它们）。
    // 语义：一个 fd 只要有任一注册服务心跳超时，就整条连接判定为失效，
    // 从所有服务与反向索引中彻底移除，避免"只删了部分服务、残留已 close 的 fd"。
    std::vector<int> sweepStale(uint64_t timeout_ms) {
        std::vector<int> dead;
        uint64_t now = nowMs();

        std::lock_guard<std::mutex> lk(mtx_);
        // 1. 找出所有过期 fd（任一服务过期即整连接过期）
        for (auto& [svc, lst] : routes_) {
            for (auto& p : lst) {
                if (now - p.last_hb_ms > timeout_ms) {
                    dead.push_back(p.fd);
                }
            }
        }
        // 2. 去重
        std::sort(dead.begin(), dead.end());
        dead.erase(std::unique(dead.begin(), dead.end()), dead.end());
        // 3. 从所有服务和反向索引中彻底移除这些 fd
        for (int fd : dead) {
            for (auto& [svc, lst] : routes_) {
                lst.remove_if([fd](const ProviderInfo& p){ return p.fd == fd; });
            }
            fd_to_services_.erase(fd);
        }
        // 4. 清理空服务条目
        for (auto it = routes_.begin(); it != routes_.end(); ) {
            if (it->second.empty()) it = routes_.erase(it);
            else ++it;
        }
        return dead;
    }

    // ---- Stats ----
    size_t numServices() const { std::lock_guard<std::mutex> lk(mtx_); return routes_.size(); }

private:
    static uint64_t nowMs() {
        using namespace std::chrono;
        return (uint64_t)duration_cast<milliseconds>(
            steady_clock::now().time_since_epoch()).count();
    }

    mutable std::mutex mtx_;
    // svc → provider list
    std::unordered_map<uint32_t, std::list<ProviderInfo>> routes_;
    // fd → 注册过的svc列表（用于fd断开快速清理）
    std::unordered_map<int, std::vector<uint32_t>> fd_to_services_;
};

} // namespace sip
