#pragma once

#include <chrono>
#include <cstdint>

#include "types.h"

namespace mooncake {

// The serving layer keeps the probe timeout in an unsigned 32-bit millisecond
// field, so the unsigned domain is part of the type instead of being restored
// by a narrowing cast at the forwarding boundary.
using NofHeartbeatProbeTimeout = std::chrono::duration<uint32_t, std::milli>;

struct NofHeartbeatBootstrapConfig {
    std::chrono::seconds interval{DEFAULT_NOF_HEARTBEAT_INTERVAL_SEC};
    NofHeartbeatProbeTimeout probe_timeout{
        DEFAULT_NOF_HEARTBEAT_PROBE_TIMEOUT_MS};
    uint32_t failures_threshold{DEFAULT_NOF_HEARTBEAT_FAILURES_THRESHOLD};
};

}  // namespace mooncake
