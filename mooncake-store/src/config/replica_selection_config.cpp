#include "config/replica_selection_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

ReplicaSelectionConfig ReplicaSelectionConfig::FromEnvironment(
    const Environ& env) {
    ReplicaSelectionConfig config;
    const auto value = env.GetTyped(
        ReplicaSelectionEnvironmentVariables::MC_STORE_REPLICA_SCORING);
    config.remote_scoring_enabled = value.has_value() && *value == "1";
    return config;
}

}  // namespace mooncake
