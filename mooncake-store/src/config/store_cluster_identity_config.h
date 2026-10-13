#pragma once

#include <optional>
#include <string>

namespace mooncake {

class Environ;

struct StoreClusterIdentityConfig {
    std::optional<std::string> cluster_id;

    static StoreClusterIdentityConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
