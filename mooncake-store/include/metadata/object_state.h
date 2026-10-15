#pragma once

#include <chrono>

#include "common/heap_optional.h"
#include "object_runtime_state.h"

namespace mooncake {
namespace route {

// The per-object runtime state kept next to the object's metadata: the task
// in flight for the key, at most one of each kind. It lives and dies with the
// object, under the same lock as its metadata.
//
// Every object carries one, and almost every object carries no task, so each
// task lives on the heap and costs a pointer while absent.
struct ObjectState {
    // A primary write or a background task is in flight for this key.
    bool is_processing{false};
    HeapOptional<ReplicationTask> replication_task;
    HeapOptional<OffloadingTask> offloading_task;
    HeapOptional<PromotionTask> promotion_task;
    // The dynamic replica proposal admitted for this key and not yet started
    // by its CopyStart, and how long the key waits before the next one. Owned
    // by DynamicReplicationController; neither is work in flight.
    HeapOptional<DynamicReplicaPending> dynamic_replication_pending;
    std::chrono::steady_clock::time_point dynamic_replication_cooldown{};

    bool HasWorkInFlight() const {
        return is_processing || replication_task || offloading_task ||
               promotion_task;
    }
};

}  // namespace route
}  // namespace mooncake
