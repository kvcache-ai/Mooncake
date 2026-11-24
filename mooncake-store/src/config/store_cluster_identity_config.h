#pragma once

#include <optional>
#include <string>

namespace mooncake {

struct StoreClusterIdentityConfig {
    std::optional<std::string> cluster_id;

    static StoreClusterIdentityConfig FromEnvironment();
};

}  // namespace mooncake
