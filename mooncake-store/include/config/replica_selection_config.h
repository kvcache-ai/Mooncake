#pragma once

namespace mooncake {

struct ReplicaSelectionConfig {
    bool remote_scoring_enabled = false;

    static ReplicaSelectionConfig FromEnvironment();
};

}  // namespace mooncake
