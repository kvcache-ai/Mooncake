#include "store_cluster_identity_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

StoreClusterIdentityConfig StoreClusterIdentityConfig::FromEnvironment() {
    StoreClusterIdentityConfig config;
    config.cluster_id = Environ::Read(
        StoreClusterIdentityEnvironmentVariables::MC_STORE_CLUSTER_ID);
    return config;
}

}  // namespace mooncake
