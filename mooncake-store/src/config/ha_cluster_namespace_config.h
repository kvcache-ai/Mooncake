#pragma once

#include <string>

#include "types.h"

namespace mooncake {

class Environ;

struct HaClusterNamespaceConfig {
    std::string cluster_namespace = DEFAULT_CLUSTER_ID;

    static HaClusterNamespaceConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
