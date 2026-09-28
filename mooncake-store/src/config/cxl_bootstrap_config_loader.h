#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "config/cxl_bootstrap_config.h"

namespace mooncake {

class DefaultConfig;

struct CxlBootstrapCommandLineOverrides {
    std::optional<bool> enabled;
    std::optional<std::string> path;
    std::optional<uint64_t> size;
};

CxlBootstrapConfig ResolveCxlBootstrapConfig(
    const DefaultConfig* file_config,
    const CxlBootstrapCommandLineOverrides& command_line);

}  // namespace mooncake
