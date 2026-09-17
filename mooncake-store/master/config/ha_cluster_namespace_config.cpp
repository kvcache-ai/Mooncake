#include "ha_cluster_namespace_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

HaClusterNamespaceConfig HaClusterNamespaceConfig::FromEnvironment() {
    HaClusterNamespaceConfig config;
    const auto value = Environ::Read(
        HaClusterNamespaceEnvironmentVariables::MC_STORE_CLUSTER_ID);
    if (value && !value->empty()) {
        config.cluster_namespace = *value;
    }
    return config;
}

}  // namespace mooncake
