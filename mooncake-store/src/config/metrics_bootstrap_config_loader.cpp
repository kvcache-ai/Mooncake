#include "metrics_bootstrap_config_loader.h"

#include <limits>
#include <stdexcept>

#include "default_config.h"

namespace mooncake {

MetricsBootstrapConfig ResolveMetricsBootstrapConfig(
    const DefaultConfig* file_config,
    const MetricsBootstrapCommandLineOverrides& command_line) {
    MetricsBootstrapConfig result;

    if (file_config != nullptr) {
        if (file_config->Contains("enable_metric_reporting")) {
            file_config->GetBool("enable_metric_reporting", &result.enabled);
        }
        if (file_config->Contains("metrics_port")) {
            file_config->GetUInt32("metrics_port", &result.port);
        }
        if (file_config->Contains("metrics_host")) {
            file_config->GetString("metrics_host", &result.host);
        }
    }

    if (command_line.enabled.has_value()) {
        result.enabled = *command_line.enabled;
    }
    if (command_line.port.has_value()) {
        result.port = *command_line.port;
    }
    if (command_line.host.has_value()) {
        result.host = *command_line.host;
    }
    if (result.port > std::numeric_limits<uint16_t>::max()) {
        throw std::invalid_argument(
            "metrics_port must be in the range 0..65535");
    }
    return result;
}

}  // namespace mooncake
