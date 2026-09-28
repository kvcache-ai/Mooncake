#pragma once

#include <chrono>
#include <cstdint>

#include "types.h"

namespace mooncake {

struct NofHeartbeatBootstrapConfig {
    std::chrono::seconds interval{DEFAULT_NOF_HEARTBEAT_INTERVAL_SEC};
    std::chrono::milliseconds probe_timeout{
        DEFAULT_NOF_HEARTBEAT_PROBE_TIMEOUT_MS};
    uint32_t failures_threshold = DEFAULT_NOF_HEARTBEAT_FAILURES_THRESHOLD;
};

}  // namespace mooncake
