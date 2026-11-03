#include "ha_cluster_namespace_config.h"

#include "config/store_cluster_identity_config.h"

namespace mooncake {

HaClusterNamespaceConfig HaClusterNamespaceConfig::FromEnvironment() {
    HaClusterNamespaceConfig config;
    const auto identity = StoreClusterIdentityConfig::FromEnvironment();
    if (identity.cluster_id && !identity.cluster_id->empty()) {
        config.cluster_namespace = *identity.cluster_id;
    }
    return config;
}

}  // namespace mooncake
