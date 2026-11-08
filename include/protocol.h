#pragma once
// ============================================================================
// protocol.h — 与 VehicleIpcBus::vipc::Header 保持一致（为了零依赖本仓库复制一份）
// 上位机通信 TCP 帧：Header(25B) + Payload(length B)
// ============================================================================
#include <cstdint>

namespace sip {

static constexpr uint32_t kMagic      = 0xABCDEF01;
static constexpr uint32_t kHeaderSize = 25;

enum class MsgType : uint8_t {
    REQUEST    = 0x00,
    RESPONSE   = 0x02,
    NOTIFY     = 0x04,
    REGISTER   = 0x08,
    HEARTBEAT  = 0x10,
};

enum class ErrorCode : uint8_t {
    OK                  = 0x00,
    NOT_READY           = 0x01,
    NOT_FOUND           = 0x02,
    TIMEOUT             = 0x03,
    INVALID_PAYLOAD     = 0x04,
    SERVICE_UNAVAILABLE = 0x05,
};

struct __attribute__((packed)) Header {
    uint32_t  magic;
    uint32_t  service_id;
    uint32_t  method_id;
    uint32_t  length;
    uint32_t  client_id;
    MsgType   msg_type;
    ErrorCode error_code;
    uint8_t   version;      // 协议版本，当前=0（预留：版本协商/兼容判断）
    uint8_t   reserved[2];  // 对齐到 25B，预留未来扩展
};
static_assert(sizeof(Header) == kHeaderSize, "Header size != 25 bytes");

// ---- 网络字节序（大端）转换 ------------------------------------------------
// 不依赖 <arpa/inet.h>，纯 C++ 实现，便于跨平台与单元测试。
// 协议规定：Header 的多字节字段（magic/service_id/method_id/length/client_id）
// 在网络上统一为大端序，收发两端都须经 headerToNet / headerToHost 转换。
inline bool isLittleEndianHost() {
    const uint16_t one = 1;
    return *reinterpret_cast<const uint8_t*>(&one) == 1;
}

inline uint32_t hostToNet32(uint32_t v) {
    if (!isLittleEndianHost()) return v;   // 大端主机无需转换
    return ((v & 0x000000FFu) << 24) |
           ((v & 0x0000FF00u) << 8)  |
           ((v & 0x00FF0000u) >> 8)  |
           ((v & 0xFF000000u) >> 24);
}
inline uint32_t netToHost32(uint32_t v) { return hostToNet32(v); }  // 字节序转换自反

inline Header headerToNet(const Header& h) {
    Header n = h;
    n.magic      = hostToNet32(h.magic);
    n.service_id = hostToNet32(h.service_id);
    n.method_id  = hostToNet32(h.method_id);
    n.length     = hostToNet32(h.length);
    n.client_id  = hostToNet32(h.client_id);
    return n;
}
inline Header headerToHost(const Header& n) { return headerToNet(n); }

// ---- 常用 ServiceID（可扩展）----
enum ServiceID : uint32_t {
    SVC_GATEWAY     = 0x00000000,   // 网关自身的/stats等
    SVC_VEHICLE_API = 0x00001001,   // 车端对外API（远程控制/状态查询）
    SVC_CLOUD_OTA   = 0x00002001,   // 云端下发OTA
};

} // namespace sip
