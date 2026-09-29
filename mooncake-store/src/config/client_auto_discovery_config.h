#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace mooncake {

class Environ;

struct ClientAutoDiscoveryConfig {
    bool enabled = false;
    std::vector<std::string> filters;

    static ClientAutoDiscoveryConfig FromEnvironment(
        const Environ& env, std::string_view protocol,
        bool device_names_configured);

    // Filters are loaded after the enabled state is applied so the existing
    // environment-read and diagnostic order remains unchanged.
    void LoadFiltersFromEnvironment(const Environ& env);
};

}  // namespace mooncake
