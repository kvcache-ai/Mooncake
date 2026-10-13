#include "store_cluster_identity_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

StoreClusterIdentityConfig StoreClusterIdentityConfig::FromEnvironment(
    const Environ& env) {
    StoreClusterIdentityConfig config;
    config.cluster_id = env.GetTyped(
        StoreClusterIdentityEnvironmentVariables::MC_STORE_CLUSTER_ID);
    return config;
}

}  // namespace mooncake
