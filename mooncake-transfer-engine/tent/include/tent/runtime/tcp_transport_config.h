// Copyright 2026 KVCache.AI
#ifndef TENT_RUNTIME_TCP_TRANSPORT_CONFIG_H_
#define TENT_RUNTIME_TCP_TRANSPORT_CONFIG_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "tent/common/config.h"

namespace mooncake::tent {

enum class TcpImplementation { kStandard, kHighPerformance };

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

struct TcpTransportConfig {
    bool enabled{true};
    TcpImplementation implementation{TcpImplementation::kStandard};
    HighPerformanceTcpParams high_performance;

    bool hpRequired() const {
        return enabled && implementation == TcpImplementation::kHighPerformance;
    }
};

// This is intentionally the sole parser for transports/tcp.  Config::get()
// silently substitutes defaults for type mismatches and is not suitable here.
Status ParseTcpTransportConfig(const Config& config, TcpTransportConfig* out);

}  // namespace mooncake::tent

#endif  // TENT_RUNTIME_TCP_TRANSPORT_CONFIG_H_
