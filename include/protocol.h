#pragma once
// ============================================================================
// protocol.h — 与 VehicleIpcBus::vipc::Header 保持一致（为了零依赖本仓库复制一份）
// 车云 TCP 帧：Header(25B) + Payload(length B)
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

// ---- 常用 ServiceID（可扩展）----
enum ServiceID : uint32_t {
    SVC_GATEWAY     = 0x00000000,   // 网关自身的/stats等
    SVC_VEHICLE_API = 0x00001001,   // 车端对外API（远程控制/状态查询）
    SVC_CLOUD_OTA   = 0x00002001,   // 云端下发OTA
};

} // namespace sip
