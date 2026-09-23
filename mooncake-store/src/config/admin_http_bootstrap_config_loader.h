#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "config/admin_http_bootstrap_config.h"

namespace mooncake {

class DefaultConfig;

struct AdminHttpBootstrapCommandLineOverrides {
    std::optional<bool> enabled;
    std::optional<uint32_t> port;
    std::optional<std::string> host;
};

AdminHttpBootstrapConfig ResolveAdminHttpBootstrapConfig(
    const DefaultConfig* file_config,
    const AdminHttpBootstrapCommandLineOverrides& command_line);

}  // namespace mooncake
