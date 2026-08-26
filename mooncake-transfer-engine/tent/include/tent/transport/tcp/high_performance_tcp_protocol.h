// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_PROTOCOL_H_
#define TENT_HIGH_PERFORMANCE_TCP_PROTOCOL_H_

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake::tent {

constexpr uint32_t kHighPerformanceTcpMagic = 0x4d435450;  // MCTP
constexpr uint16_t kHighPerformanceTcpVersion = 1;
constexpr size_t kHighPerformanceTcpRequestSize = 48;
constexpr size_t kHighPerformanceTcpResponseSize = 32;

enum class HighPerformanceTcpOpcode : uint8_t { kRead = 1, kWrite = 2 };
enum class HighPerformanceTcpStatus : uint16_t {
    kOk = 0,
    kBadVersion = 1,
    kBadOpcode = 2,
    kBadLength = 3,
    kRangeRejected = 4,
    kPermissionDenied = 5,
    kStaleRegistration = 6,
    kShuttingDown = 7,
    kInternalError = 8,
};

struct HighPerformanceTcpRequestFrame {
    HighPerformanceTcpOpcode opcode{HighPerformanceTcpOpcode::kRead};
    uint64_t request_id{0};
    uint64_t registration_id{0};
    uint64_t remote_addr{0};
    uint64_t length{0};
};
struct HighPerformanceTcpResponseFrame {
    HighPerformanceTcpStatus status{HighPerformanceTcpStatus::kOk};
    uint64_t request_id{0};
    uint64_t committed_bytes{0};
};

std::array<uint8_t, kHighPerformanceTcpRequestSize>
EncodeHighPerformanceTcpRequest(const HighPerformanceTcpRequestFrame& frame);
Status DecodeHighPerformanceTcpRequest(const uint8_t* bytes, size_t size,
                                       HighPerformanceTcpRequestFrame* frame);
std::array<uint8_t, kHighPerformanceTcpResponseSize>
EncodeHighPerformanceTcpResponse(const HighPerformanceTcpResponseFrame& frame);
Status DecodeHighPerformanceTcpResponse(const uint8_t* bytes, size_t size,
                                        HighPerformanceTcpResponseFrame* frame);

struct HighPerformanceTcpEndpoint {
    std::string host;
    uint16_t port{0};
};
struct HighPerformanceTcpEndpointAttr {
    std::string incarnation;
    std::vector<HighPerformanceTcpEndpoint> endpoints;
    uint64_t max_transfer_bytes{0};
};
struct HighPerformanceTcpBufferAttr {
    uint64_t registration_id{0};
    std::string permission;
};

Status EncodeHighPerformanceTcpEndpointAttr(
    const HighPerformanceTcpEndpointAttr& attr, std::string* encoded);
Status DecodeHighPerformanceTcpEndpointAttr(
    const std::string& encoded, HighPerformanceTcpEndpointAttr* attr);
Status EncodeHighPerformanceTcpBufferAttr(
    const HighPerformanceTcpBufferAttr& attr, std::string* encoded);
Status DecodeHighPerformanceTcpBufferAttr(const std::string& encoded,
                                          HighPerformanceTcpBufferAttr* attr);
const char* HighPerformanceTcpPermissionName(Permission permission);
}  // namespace mooncake::tent
#endif
