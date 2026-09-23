#include "admin_http_bootstrap_config_loader.h"

#include <limits>
#include <stdexcept>

#include "default_config.h"

namespace mooncake {

AdminHttpBootstrapConfig ResolveAdminHttpBootstrapConfig(
    const DefaultConfig* file_config,
    const AdminHttpBootstrapCommandLineOverrides& command_line) {
    AdminHttpBootstrapConfig result;
    uint32_t port = result.port;

    if (file_config != nullptr) {
        if (file_config->Contains("enable_http_metadata_server")) {
            file_config->GetBool("enable_http_metadata_server",
                                 &result.enabled);
        }
        if (file_config->Contains("http_metadata_server_port")) {
            file_config->GetUInt32("http_metadata_server_port", &port);
        }
        if (file_config->Contains("http_metadata_server_host")) {
            file_config->GetString("http_metadata_server_host", &result.host);
        }
    }

    if (command_line.enabled.has_value()) {
        result.enabled = *command_line.enabled;
    }
    if (command_line.port.has_value()) {
        port = *command_line.port;
    }
    if (command_line.host.has_value()) {
        result.host = *command_line.host;
    }
    if (port > std::numeric_limits<uint16_t>::max()) {
        throw std::invalid_argument(
            "http_metadata_server_port must be in the range 0..65535");
    }
    result.port = static_cast<uint16_t>(port);
    return result;
}

}  // namespace mooncake
