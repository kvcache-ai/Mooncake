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
    size_t queue_capacity_per_worker{256};
    size_t connections_per_peer{4};
    uint64_t max_outstanding_tasks{4096};
    uint64_t max_outstanding_bytes{1ULL << 32};
    uint64_t max_transfer_bytes{1ULL << 30};
    size_t chunk_size{1ULL << 20};
    uint64_t connect_timeout_ms{2000};
    uint64_t progress_timeout_ms{30000};
};

struct HpTcpTransportConfig {
    bool enabled{false};
    HighPerformanceTcpParams params;
};

// This is intentionally the sole parser for transports/hp_tcp. Config::get()
// silently substitutes defaults for type mismatches and is not suitable here.
// HP TCP and standard TCP are mutually exclusive in v1 because both data
// planes currently own the single ControlService notification callback.
Status ParseHpTcpTransportConfig(const Config& config,
                                 HpTcpTransportConfig* out);

}  // namespace mooncake::tent

#endif  // TENT_RUNTIME_HP_TCP_TRANSPORT_CONFIG_H_
