#include "config/replica_selection_config.h"

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

ReplicaSelectionConfig ReplicaSelectionConfig::FromEnvironment() {
    ReplicaSelectionConfig config;
    const auto value = Environ::Read(
        ReplicaSelectionEnvironmentVariables::MC_STORE_REPLICA_SCORING);
    config.remote_scoring_enabled = value.has_value() && *value == "1";
    config.hca_affinity_enabled =
        Environ::Read(
            ReplicaSelectionEnvironmentVariables::MC_STORE_HCA_AFFINITY)
            .value_or(false);
    config.hca_numa_segments_enabled =
        config.hca_affinity_enabled &&
        Environ::Read(
            ReplicaSelectionEnvironmentVariables::MC_STORE_HCA_NUMA_SEGMENTS)
            .value_or(false);
    return config;
}

}  // namespace mooncake
