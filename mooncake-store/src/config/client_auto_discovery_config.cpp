#include "client_auto_discovery_config.h"

#include <optional>

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "ascii_string.h"
#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

ClientAutoDiscoveryConfig ClientAutoDiscoveryConfig::FromEnvironment(
    std::string_view protocol, bool device_names_configured) {
    using Variables = ClientAutoDiscoveryEnvironmentVariables;
    ClientAutoDiscoveryConfig config;

    const auto raw_auto_discover = Environ::Read(Variables::MC_MS_AUTO_DISC);
    std::optional<bool> configured_auto_discover;
    if (raw_auto_discover) {
        try {
            const int value = std::stoi(*raw_auto_discover);
            if (value == 1) {
                LOG(INFO) << "auto discovery set by env "
                          << Variables::MC_MS_AUTO_DISC.name;
                configured_auto_discover = true;
            } else if (value == 0) {
                LOG(INFO) << "auto discovery not set by env "
                          << Variables::MC_MS_AUTO_DISC.name;
                configured_auto_discover = false;
            }
        } catch (const std::exception&) {
            // Preserve the legacy fallback for malformed and out-of-range
            // values instead of propagating std::stoi failures.
        }
        if (!configured_auto_discover.has_value()) {
            LOG(WARNING) << "invalid " << Variables::MC_MS_AUTO_DISC.name
                         << " value: " << *raw_auto_discover
                         << ", should be 0 or 1, using default: auto discovery "
                            "not set";
        }
    }

    if (configured_auto_discover.has_value()) {
        config.enabled = *configured_auto_discover;
    } else {
        config.enabled = (protocol == "rdma" || protocol == "efa") &&
                         !device_names_configured;
        if (config.enabled) {
            LOG(INFO) << "Set auto discovery ON by default for " << protocol
                      << " protocol, since no device names provided";
        }
    }

    return config;
}

void ClientAutoDiscoveryConfig::LoadFiltersFromEnvironment() {
    using Variables = ClientAutoDiscoveryEnvironmentVariables;

    const auto raw_filters = Environ::Read(Variables::MC_MS_FILTERS);
    if (enabled) {
        if (raw_filters) {
            LOG(INFO) << "whitelist filters: " << *raw_filters;
            boost::split(filters, *raw_filters, boost::is_any_of(","),
                         boost::token_compress_off);
            for (auto& filter : filters) {
                filter = std::string(TrimAsciiWhitespace(filter));
            }
        }
    } else if (raw_filters && !raw_filters->empty()) {
        LOG(WARNING) << Variables::MC_MS_FILTERS.name
                     << " is set but auto discovery is disabled; ignoring "
                        "whitelist: "
                     << *raw_filters;
    }
}

}  // namespace mooncake
