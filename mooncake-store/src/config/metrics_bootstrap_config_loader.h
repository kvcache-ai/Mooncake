#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "config/metrics_bootstrap_config.h"

namespace mooncake {

class DefaultConfig;

struct MetricsBootstrapCommandLineOverrides {
    std::optional<bool> enabled;
    std::optional<uint32_t> port;
    std::optional<std::string> host;
};

MetricsBootstrapConfig ResolveMetricsBootstrapConfig(
    const DefaultConfig* file_config,
    const MetricsBootstrapCommandLineOverrides& command_line);

}  // namespace mooncake
