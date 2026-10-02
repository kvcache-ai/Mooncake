#pragma once

#include <cstdint>
#include <optional>

#include "config/nof_heartbeat_bootstrap_config.h"

namespace mooncake {

class DefaultConfig;

struct NofHeartbeatCommandLineOverrides {
    std::optional<int64_t> interval_seconds;
    std::optional<uint32_t> probe_timeout_ms;
    std::optional<uint32_t> failures_threshold;
};

NofHeartbeatBootstrapConfig ResolveNofHeartbeatBootstrapConfig(
    const DefaultConfig* file_config,
    const NofHeartbeatCommandLineOverrides& command_line);

}  // namespace mooncake
