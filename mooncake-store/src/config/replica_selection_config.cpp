#include "config/replica_selection_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

ReplicaSelectionConfig ReplicaSelectionConfig::FromEnvironment() {
    ReplicaSelectionConfig config;
    const auto value = Environ::Read(
        ReplicaSelectionEnvironmentVariables::MC_STORE_REPLICA_SCORING);
    config.remote_scoring_enabled = value.has_value() && *value == "1";
    return config;
}

}  // namespace mooncake
