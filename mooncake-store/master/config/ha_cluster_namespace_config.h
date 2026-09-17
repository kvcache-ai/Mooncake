#pragma once

#include <string>

#include "common/types.h"

namespace mooncake {

struct HaClusterNamespaceConfig {
    std::string cluster_namespace = DEFAULT_CLUSTER_ID;

    static HaClusterNamespaceConfig FromEnvironment();
};

}  // namespace mooncake
