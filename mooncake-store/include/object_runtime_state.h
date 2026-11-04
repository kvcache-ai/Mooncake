#pragma once

#include <chrono>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake {

struct ReplicationTask {
    UUID client_id;
    std::chrono::steady_clock::time_point start_time;
    enum class Type {
        COPY,
        MOVE,
    } type;
    ReplicaID source_id;
    std::vector<ReplicaID> replica_ids;
};

}  // namespace mooncake
