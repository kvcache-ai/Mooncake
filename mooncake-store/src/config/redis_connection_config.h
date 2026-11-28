#pragma once

#include <string>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

enum class RedisDurabilityProfile { kMemory, kPersistent };

struct RedisConnectionConfig {
    int db_index = 0;
    bool cluster_enabled = false;
    int connect_timeout_ms = 3000;
    int command_timeout_ms = 3000;
    int topology_refresh_ms = 1000;
    int max_redirects = 5;
    // Explicit compatibility default for existing standalone deployments.
    RedisDurabilityProfile durability_profile = RedisDurabilityProfile::kMemory;
    int replication_ack_timeout_ms = 1000;
    int aof_ack_timeout_ms = 1000;
    std::string username;
    std::string password;

    static tl::expected<RedisConnectionConfig, ErrorCode> FromEnvironment();
};

}  // namespace mooncake
