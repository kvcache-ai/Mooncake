#pragma once

#include <map>
#include <string>

#include "types.h"

namespace mooncake {

struct LocalSsdPersistedClient {
    bool enable_offloading{false};
    int64_t total_capacity_bytes{0};

    // The codec iterates this map directly, so ordering by the legacy encoded
    // key makes the snapshot bytes deterministic without rewriting that key.
    std::map<std::string, OffloadTaskItem> pending_offloads;
};

// UUID ordering likewise makes client encoding deterministic.
using LocalSsdPersistedState = std::map<UUID, LocalSsdPersistedClient>;

}  // namespace mooncake
