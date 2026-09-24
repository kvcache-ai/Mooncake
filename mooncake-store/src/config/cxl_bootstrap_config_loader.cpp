#include "cxl_bootstrap_config_loader.h"

#include <limits>
#include <stdexcept>

#include "default_config.h"

namespace mooncake {

CxlBootstrapConfig ResolveCxlBootstrapConfig(
    const DefaultConfig* file_config,
    const CxlBootstrapCommandLineOverrides& command_line) {
    CxlBootstrapConfig result;
    uint64_t size = result.size;

    if (file_config != nullptr) {
        if (file_config->Contains("enable_cxl")) {
            file_config->GetBool("enable_cxl", &result.enabled);
        }
        if (file_config->Contains("cxl_path")) {
            file_config->GetString("cxl_path", &result.path);
        }
        if (file_config->Contains("cxl_size")) {
            file_config->GetUInt64("cxl_size", &size);
        }
    }

    if (command_line.enabled.has_value()) {
        result.enabled = *command_line.enabled;
    }
    if (command_line.path.has_value()) {
        result.path = *command_line.path;
    }
    if (command_line.size.has_value()) {
        size = *command_line.size;
    }
    if (size > std::numeric_limits<size_t>::max()) {
        throw std::invalid_argument("cxl_size cannot fit in size_t");
    }
    result.size = static_cast<size_t>(size);
    return result;
}

}  // namespace mooncake
