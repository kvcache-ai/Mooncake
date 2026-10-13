#pragma once

namespace mooncake {

class Environ;

struct ReplicaSelectionConfig {
    bool remote_scoring_enabled = false;

    static ReplicaSelectionConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
