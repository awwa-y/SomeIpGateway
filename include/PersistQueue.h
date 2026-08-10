#pragma once
// ============================================================================
// PersistQueue — 离线消息持久化（轻量 append-only 文件）
// 用途：Provider 离线期间，发往它的REQUEST不再返回NOT_FOUND，而是落盘排队，
//      当Provider重新 REGISTER → 立即重放队列。
// ============================================================================
#include "protocol.h"
#include <cstdio>
#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>
#include <mutex>
#include <list>

namespace sip {

struct QueuedMsg {
    uint32_t    original_client_fd;   // 恢复后回传的响应要回到哪个fd
    uint32_t    service_id;
    uint32_t    method_id;
    uint32_t    client_id;            // 流水号
    std::vector<uint8_t> payload;
};

class PersistQueue {
public:
    explicit PersistQueue(const std::string& dir = "./queue_data");
    ~PersistQueue();

    // 入队：svc 暂不可用时存盘（内存 + 文件快照）
    void enqueue(uint32_t service_id, const QueuedMsg& m);

    // Provider 重新上线：取出 svc 对应的所有排队消息（同时从磁盘清除）
    std::list<QueuedMsg> drainFor(uint32_t service_id);

    // 统计
    size_t pendingBytes() const;
    size_t pendingCount() const;

private:
    void fsyncFile();
    std::string filePath(uint32_t svc) const;

    std::string dir_;
    mutable std::mutex mtx_;
    // svc → list（内存索引，磁盘是持久化镜像）
    std::unordered_map<uint32_t, std::list<QueuedMsg>> memory_;
    std::unordered_map<uint32_t, FILE*> fds_;
};

} // namespace sip
