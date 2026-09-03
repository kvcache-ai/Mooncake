// Copyright 2026 KVCache.AI
#ifndef TENT_RUNTIME_HP_TCP_TRANSPORT_CONFIG_H_
#define TENT_RUNTIME_HP_TCP_TRANSPORT_CONFIG_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "tent/common/config.h"

namespace mooncake::tent {

struct HighPerformanceTcpParams {
    std::string bind_address;
    std::string advertise_address;
    uint16_t port{0};
    size_t worker_count{16};
    size_t connections_per_peer{4};
    uint64_t max_outstanding_tasks{4096};
    uint64_t max_outstanding_bytes{1ULL << 32};
    uint64_t max_transfer_bytes{1ULL << 30};
    uint64_t connect_timeout_ms{2000};
    uint64_t progress_timeout_ms{30000};
};

struct HpTcpTransportConfig {
    bool enabled{false};
    HighPerformanceTcpParams params;
};

// This is intentionally the sole parser for transports/hp_tcp. Config::get()
// silently substitutes defaults for type mismatches and is not suitable here.
Status ParseHpTcpTransportConfig(const Config& config,
                                 HpTcpTransportConfig* out);

}  // namespace mooncake::tent

#endif  // TENT_RUNTIME_HP_TCP_TRANSPORT_CONFIG_H_
