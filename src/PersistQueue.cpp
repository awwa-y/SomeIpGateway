// ============================================================================
// PersistQueue.cpp — 简易实现（轻量版够用，生产可替换 LevelDB / RocksDB）
// ============================================================================
#include "PersistQueue.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <cstring>
#include <cstdio>
#include <algorithm>

namespace sip {

static void mkdir_p(const std::string& dir) {
    ::mkdir(dir.c_str(), 0755);
}

PersistQueue::PersistQueue(const std::string& dir) : dir_(dir) {
    mkdir_p(dir_);
}

PersistQueue::~PersistQueue() {
    for (auto& [k, fp] : fds_) if (fp) ::fclose(fp);
    fds_.clear();
}

std::string PersistQueue::filePath(uint32_t svc) const {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "/svc_%08x.qlog", svc);
    return dir_ + buf;
}

void PersistQueue::enqueue(uint32_t service_id, const QueuedMsg& m) {
    std::lock_guard<std::mutex> lk(mtx_);
    memory_[service_id].push_back(m);

    // 简化磁盘：每次入队一条写文件（可以批量刷）
    if (!fds_.count(service_id)) {
        fds_[service_id] = ::fopen(filePath(service_id).c_str(), "ab");
    }
    FILE* fp = fds_[service_id];
    if (!fp) return;
    // 记录格式: [len:4][orig_fd:4][svc:4][m:4][cid:4][plen:4][payload bytes]
    uint32_t plen = (uint32_t)m.payload.size();
    uint32_t reclen = 4*6 + plen;
    ::fwrite(&reclen, 4, 1, fp);
    ::fwrite(&m.original_client_fd, 4, 1, fp);
    ::fwrite(&m.service_id, 4, 1, fp);
    ::fwrite(&m.method_id,  4, 1, fp);
    ::fwrite(&m.client_id,  4, 1, fp);
    ::fwrite(&plen, 4, 1, fp);
    if (plen) ::fwrite(m.payload.data(), 1, plen, fp);
    ::fflush(fp);
}

std::list<QueuedMsg> PersistQueue::drainFor(uint32_t service_id) {
    std::list<QueuedMsg> out;
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = memory_.find(service_id);
    if (it != memory_.end()) {
        out.swap(it->second);
        memory_.erase(it);
    }
    auto fit = fds_.find(service_id);
    if (fit != fds_.end() && fit->second) {
        ::fclose(fit->second);
        fds_.erase(fit);
        std::remove(filePath(service_id).c_str());
    }
    return out;
}

size_t PersistQueue::pendingCount() const {
    size_t n = 0;
    std::lock_guard<std::mutex> lk(mtx_);
    for (auto& [k,lst] : memory_) n += lst.size();
    return n;
}

size_t PersistQueue::pendingBytes() const {
    size_t n = 0;
    std::lock_guard<std::mutex> lk(mtx_);
    for (auto& [k,lst] : memory_)
        for (auto& m : lst) n += 24 + m.payload.size();
    return n;
}

} // namespace sip
