#pragma once

#include <optional>
#include <string>

#include "config/runtime_identity_config.h"

namespace mooncake {

class DefaultConfig;

struct RuntimeIdentityCommandLineOverrides {
    std::optional<std::string> pod_name;
    std::optional<std::string> pod_namespace;
};

RuntimeIdentityConfig ResolveRuntimeIdentityConfig(
    const DefaultConfig* file_config,
    const RuntimeIdentityCommandLineOverrides& command_line);

}  // namespace mooncake
