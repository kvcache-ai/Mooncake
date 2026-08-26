// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_CLIENT_H_
#define TENT_HIGH_PERFORMANCE_TCP_CLIENT_H_
#include <cstdint>
#include <string>
#include "tent/common/status.h"
#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"
namespace mooncake::tent {
class HighPerformanceTcpClient {
   public:
    struct Config {
        uint64_t max_transfer_bytes{1ULL << 30};
        size_t chunk_size{1ULL << 20};
        uint64_t connect_timeout_ms{2000};
    };
    explicit HighPerformanceTcpClient(Config config) : config_(config) {}
    Status transfer(const std::string& host, uint16_t port,
                    uint64_t registration_id, uint64_t remote_addr,
                    void* local_addr, uint64_t length,
                    HighPerformanceTcpOpcode opcode, uint64_t request_id);

   private:
    Config config_;
};
}  // namespace mooncake::tent
#endif
