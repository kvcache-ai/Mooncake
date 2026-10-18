#pragma once

namespace mooncake {

struct ReplicaSelectionConfig {
    bool remote_scoring_enabled = false;
    bool hca_affinity_enabled = false;
    bool hca_numa_segments_enabled = false;

    static ReplicaSelectionConfig FromEnvironment();
};

inline bool StoreHcaAffinityEnabled() {
    static const bool enabled =
        ReplicaSelectionConfig::FromEnvironment().hca_affinity_enabled;
    return enabled;
}

}  // namespace mooncake
