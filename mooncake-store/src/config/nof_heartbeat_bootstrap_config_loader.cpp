#include "nof_heartbeat_bootstrap_config_loader.h"

#include "default_config.h"

namespace mooncake {

NofHeartbeatBootstrapConfig ResolveNofHeartbeatBootstrapConfig(
    const DefaultConfig* file_config,
    const NofHeartbeatCommandLineOverrides& command_line) {
    NofHeartbeatBootstrapConfig result;
    int64_t interval_seconds = result.interval.count();
    uint32_t probe_timeout_ms =
        static_cast<uint32_t>(result.probe_timeout.count());

    if (file_config != nullptr) {
        if (file_config->Contains("nof_heartbeat_interval_sec")) {
            file_config->GetInt64("nof_heartbeat_interval_sec",
                                  &interval_seconds);
        }
        if (file_config->Contains("nof_heartbeat_probe_timeout_ms")) {
            file_config->GetUInt32("nof_heartbeat_probe_timeout_ms",
                                   &probe_timeout_ms);
        }
        if (file_config->Contains("nof_heartbeat_failures_threshold")) {
            file_config->GetUInt32("nof_heartbeat_failures_threshold",
                                   &result.failures_threshold);
        }
    }

    if (command_line.interval_seconds.has_value()) {
        interval_seconds = *command_line.interval_seconds;
    }
    if (command_line.probe_timeout_ms.has_value()) {
        probe_timeout_ms = *command_line.probe_timeout_ms;
    }
    if (command_line.failures_threshold.has_value()) {
        result.failures_threshold = *command_line.failures_threshold;
    }

    result.interval = std::chrono::seconds(interval_seconds);
    result.probe_timeout = std::chrono::milliseconds(probe_timeout_ms);
    return result;
}

}  // namespace mooncake
