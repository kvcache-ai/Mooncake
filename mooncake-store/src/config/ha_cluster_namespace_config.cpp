#include "ha_cluster_namespace_config.h"

#include "config/store_cluster_identity_config.h"
#include "environ.h"

namespace mooncake {

HaClusterNamespaceConfig HaClusterNamespaceConfig::FromEnvironment(
    const Environ& env) {
    HaClusterNamespaceConfig config;
    const auto identity = StoreClusterIdentityConfig::FromEnvironment(env);
    if (identity.cluster_id && !identity.cluster_id->empty()) {
        config.cluster_namespace = *identity.cluster_id;
    }
    return config;
}

}  // namespace mooncake
