#include "transfer_submitter_config.h"

#include <glog/logging.h>

#include <algorithm>
#include <cctype>

#include "bool_parser.h"
#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

TransferSubmitterConfig TransferSubmitterConfig::FromEnvironment() {
    TransferSubmitterConfig config;
    auto value =
        Environ::Read(TransferSubmitterEnvironmentVariables::MC_STORE_MEMCPY);
    if (!value.has_value()) {
        return config;
    }

    std::transform(value->begin(), value->end(), value->begin(),
                   [](unsigned char c) { return std::tolower(c); });
    const auto parsed = TryParseBool(*value, {.trim_ascii_whitespace = false});
    // Reject enable/disable, which only the shared parser accepts.
    if (parsed.has_value() && *value != "enable" && *value != "disable") {
        config.memcpy_enabled_override = *parsed;
    } else {
        LOG(WARNING) << "Invalid value for MC_STORE_MEMCPY: " << *value
                     << ", defaulting to enabled";
        config.memcpy_enabled_override = true;
    }
    return config;
}

}  // namespace mooncake
